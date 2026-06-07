import "dotenv/config";
import express from "express";
import { execFile } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs/promises";
import { createServer } from "node:http";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import { WebSocket, WebSocketServer } from "ws";
import { createServer as createViteServer } from "vite";
import {
  ClientMessage,
  CommandResult,
  DEFAULT_ROOM,
  DeviceInfo,
  FleetCommand,
  FleetEvent,
  FleetSnapshotMessage,
  newId
} from "../shared/protocol.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const rootDir = path.resolve(__dirname, "..");
const isProduction = process.argv.includes("--production") || process.env.NODE_ENV === "production";
const port = Number(process.env.PORT ?? 8787);
const runFile = promisify(execFile);
const agentArchivePath = path.join(rootDir, "dist", "computah-agent.tgz");

type Peer =
  | { kind: "conductor"; room: string; socket: WebSocket }
  | { kind: "agent"; room: string; deviceId: string; socket: WebSocket };
type CommandWaitReport = {
  completed: boolean;
  pendingDeviceIds: string[];
  results: CommandResult[];
};
type CommandWaiter = {
  expectedDeviceIds: Set<string>;
  resolve: (report: CommandWaitReport) => void;
  resultsByDeviceId: Map<string, CommandResult>;
  timeout: ReturnType<typeof setTimeout>;
};

const peers = new Map<WebSocket, Peer>();
const devices = new Map<string, DeviceInfo>();
const eventsByRoom = new Map<string, FleetEvent[]>();
const commandWaiters = new Map<string, CommandWaiter>();
// Per-room OpenAI conversation chaining so each room is one continuous thread.
const lastResponseByRoom = new Map<string, string>();

const app = express();
app.set("trust proxy", true);
app.use(express.json({ limit: "10mb" }));

app.get("/api/health", (_req, res) => {
  res.json({ ok: true, service: "computah", rooms: roomNames() });
});

app.get("/api/invite", (req, res) => {
  const room = typeof req.query.room === "string" ? req.query.room : DEFAULT_ROOM;
  const baseUrl = publicBaseUrl(req);
  res.set("Cache-Control", "no-store");
  res.json({
    room,
    baseUrl,
    conductorUrl: `${baseUrl}?room=${encodeURIComponent(room)}`,
    appDownloadUrl: agentAppDownloadUrl(baseUrl, room),
    installCommand: hostedInstallCommand(baseUrl, room)
  });
});

app.get("/api/agent/version", async (_req, res) => {
  try {
    const version = await agentSourceVersion();
    res.set("Cache-Control", "no-store");
    res.json({ ok: true, version });
  } catch (error) {
    res.status(500).json({ ok: false, error: error instanceof Error ? error.message : String(error) });
  }
});

app.get("/api/agent/version.txt", async (_req, res) => {
  try {
    const version = await agentSourceVersion();
    res.set("Cache-Control", "no-store");
    res.type("text/plain").send(`${version}\n`);
  } catch (error) {
    res.status(500).type("text/plain").send(error instanceof Error ? error.message : String(error));
  }
});

app.get("/join", (req, res) => {
  const room = typeof req.query.room === "string" ? req.query.room : DEFAULT_ROOM;
  const baseUrl = publicBaseUrl(req);
  const installCommand = hostedInstallCommand(baseUrl, room);
  const appDownloadUrl = agentAppDownloadUrl(baseUrl, room);
  res.set("Cache-Control", "no-store");
  res.type("html").send(renderJoinPage({ appDownloadUrl, baseUrl, installCommand, room }));
});

app.get("/api/rooms/:room", (req, res) => {
  const room = req.params.room || DEFAULT_ROOM;
  res.json(snapshot(room));
});

app.get("/install.sh", (req, res) => {
  const baseUrl = publicBaseUrl(req);
  res.set("Cache-Control", "no-store");
  res.type("text/plain").send(renderInstallScript(baseUrl));
});

app.get("/downloads/computah-agent.tgz", async (_req, res) => {
  try {
    await ensureAgentArchive();
    res.set("Cache-Control", "no-store");
    res.download(agentArchivePath, "computah-agent.tgz");
  } catch (error) {
    res.status(500).send(error instanceof Error ? error.message : String(error));
  }
});

app.get("/downloads/computah-agent-app.zip", async (req, res) => {
  const room = typeof req.query.room === "string" ? req.query.room : DEFAULT_ROOM;
  const baseUrl = publicBaseUrl(req);

  try {
    const { archivePath, cleanup } = await createAgentAppArchive(baseUrl, room);
    res.on("finish", cleanup);
    res.set("Cache-Control", "no-store");
    res.download(archivePath, "Computah-Agent.app.zip");
  } catch (error) {
    res.status(500).send(error instanceof Error ? error.message : String(error));
  }
});

app.post("/api/rooms/:room/commands", (req, res) => {
  const room = req.params.room || DEFAULT_ROOM;
  const command = normalizeCommand({ ...req.body, room, origin: "api" });
  if (!command) {
    res.status(400).json({ ok: false, error: "Invalid command payload." });
    return;
  }

  dispatchCommand(command);
  res.json({ ok: true, command });
});

app.get("/api/ai/tools", (_req, res) => {
  res.json({
    enabled: Boolean(process.env.OPENAI_API_KEY),
    model: process.env.OPENAI_MODEL ?? "gpt-5",
    realtimeModel: process.env.OPENAI_REALTIME_MODEL ?? "gpt-realtime-2",
    tools: aiTools
  });
});

app.post(
  "/api/rooms/:room/realtime/session",
  express.text({ type: ["application/sdp", "text/plain"], limit: "1mb" }),
  async (req, res) => {
    const room = req.params.room || DEFAULT_ROOM;
    const offerSdp = typeof req.body === "string" ? req.body : "";

    if (!process.env.OPENAI_API_KEY) {
      res.status(503).type("text/plain").send("OPENAI_API_KEY is not configured.");
      return;
    }

    if (!offerSdp.trim()) {
      res.status(400).type("text/plain").send("Missing WebRTC offer SDP.");
      return;
    }

    try {
      const fd = new FormData();
      fd.set("sdp", offerSdp);
      fd.set("session", JSON.stringify(realtimeSessionConfig(room)));

      const response = await fetch("https://api.openai.com/v1/realtime/calls", {
        method: "POST",
        headers: {
          Authorization: `Bearer ${process.env.OPENAI_API_KEY}`,
          "OpenAI-Safety-Identifier": safetyIdentifier(room)
        },
        body: fd
      });

      const answerSdp = await response.text();
      res.status(response.status).type(response.ok ? "application/sdp" : "text/plain").send(answerSdp);
    } catch (error) {
      res.status(500).type("text/plain").send(error instanceof Error ? error.message : String(error));
    }
  }
);

app.post("/api/rooms/:room/realtime/tool", async (req, res) => {
  const room = req.params.room || DEFAULT_ROOM;
  const name = typeof req.body?.name === "string" ? req.body.name : "";
  const args = objectFrom(req.body?.arguments ?? req.body?.args, {});

  if (!name) {
    res.status(400).json({ ok: false, error: "Missing tool name." });
    return;
  }

  try {
    const result = await executeAIToolWithResults(room, name, args);
    res.json({ ok: true, result });
  } catch (error) {
    res.status(500).json({
      ok: false,
      error: error instanceof Error ? error.message : String(error)
    });
  }
});

app.post("/api/rooms/:room/ai", async (req, res) => {
  const room = req.params.room || DEFAULT_ROOM;
  const prompt = typeof req.body?.prompt === "string" ? req.body.prompt.trim() : "";
  if (!prompt) {
    res.status(400).json({ ok: false, error: "Missing prompt." });
    return;
  }

  try {
    const result = process.env.OPENAI_API_KEY
      ? await runOpenAICommand(room, prompt)
      : runLocalAICommand(room, prompt);
    res.json({ ok: true, ...result });
  } catch (error) {
    res.status(500).json({
      ok: false,
      error: error instanceof Error ? error.message : String(error)
    });
  }
});

if (isProduction) {
  app.use(express.static(path.join(rootDir, "dist/client")));
  app.get(/.*/, (_req, res) => {
    res.sendFile(path.join(rootDir, "dist/client/index.html"));
  });
} else {
  const vite = await createViteServer({
    server: { middlewareMode: true },
    appType: "spa"
  });
  app.use(vite.middlewares);
}

const httpServer = createServer(app);
const wss = new WebSocketServer({ server: httpServer, path: "/ws" });

wss.on("connection", (socket) => {
  socket.on("message", (data) => {
    let message: ClientMessage;
    try {
      message = JSON.parse(data.toString()) as ClientMessage;
    } catch {
      send(socket, { type: "server.error", message: "Invalid JSON message." });
      return;
    }

    handleMessage(socket, message);
  });

  socket.on("close", () => {
    const peer = peers.get(socket);
    peers.delete(socket);

    if (peer?.kind === "agent") {
      const replacementPeer = agentPeerForDevice(peer.deviceId);
      if (replacementPeer) {
        broadcastSnapshot(peer.room);
        return;
      }

      const device = devices.get(peer.deviceId);
      if (device) {
        device.status = "offline";
        device.lastSeen = Date.now();
        addEvent(device.room, "leave", `${device.name} disconnected`, device.id, { deviceId: device.id });
        broadcastSnapshot(device.room);
      }
    }
  });
});

httpServer.listen(port, "0.0.0.0", () => {
  console.log(`Computah listening on http://localhost:${port}`);
  console.log(`Agents join with: npm run agent -- join --server ws://localhost:${port}/ws --room demo`);
});

function handleMessage(socket: WebSocket, message: ClientMessage) {
  switch (message.type) {
    case "conductor.join": {
      const room = message.room || DEFAULT_ROOM;
      peers.set(socket, { kind: "conductor", room, socket });
      send(socket, { type: "server.ack", message: `Joined room ${room}` });
      send(socket, snapshot(room));
      break;
    }

    case "agent.join": {
      const room = message.room || DEFAULT_ROOM;
      const now = Date.now();
      const device: DeviceInfo = {
        ...message.device,
        room,
        status: "online",
        connectedAt: devices.get(message.device.id)?.connectedAt ?? now,
        lastSeen: now
      };
      devices.set(device.id, device);
      peers.set(socket, { kind: "agent", room, deviceId: device.id, socket });
      addEvent(room, "join", `${device.name} joined`, device.id, {
        capabilities: device.capabilities,
        hostname: device.hostname,
        platform: device.platform
      });
      send(socket, { type: "server.ack", message: `Registered ${device.name}` });
      broadcastSnapshot(room);
      break;
    }

    case "agent.status": {
      const device = devices.get(message.deviceId);
      if (device) {
        const changed =
          device.status !== message.status ||
          (message.lastResult !== undefined && message.lastResult !== device.lastResult);
        device.status = message.status;
        device.lastResult = message.lastResult ?? device.lastResult;
        device.lastSeen = Date.now();
        if (changed) {
          addEvent(device.room, "status", `${device.name} is ${message.status}`, device.id, {
            lastResult: device.lastResult,
            status: message.status
          });
          broadcastSnapshot(device.room);
        }
      }
      break;
    }

    case "command.dispatch": {
      const peer = peers.get(socket);
      const room = message.command.room || peer?.room || DEFAULT_ROOM;
      const command = normalizeCommand({
        ...message.command,
        room,
        origin: "conductor"
      });
      if (!command) {
        send(socket, { type: "server.error", message: "Invalid command payload." });
        return;
      }

      dispatchCommand(command);
      break;
    }

    case "command.result": {
      handleResult(message.result);
      break;
    }
  }
}

function normalizeCommand(input: Partial<FleetCommand>): FleetCommand | null {
  if (!input.room || !input.target || !input.action) return null;
  if (input.target.type !== "all" && input.target.type !== "device") return null;
  if (input.target.type === "device" && !input.target.deviceId) return null;

  return {
    id: input.id ?? newId("cmd"),
    room: input.room,
    target: input.target,
    action: input.action,
    args: input.args ?? {},
    createdAt: input.createdAt ?? Date.now(),
    origin: input.origin ?? "api"
  };
}

function dispatchCommand(command: FleetCommand) {
  const targetDevices = targetDevicesForCommand(command);

  addEvent(
    command.room,
    "command",
    `${command.action} -> ${command.target.type === "all" ? "all devices" : targetDevices[0]?.name ?? command.target.deviceId}`,
    undefined,
    {
      action: command.action,
      args: command.args,
      commandId: command.id,
      origin: command.origin,
      target: command.target,
      targetDevices: targetDevices.map((device) => device.name)
    }
  );

  for (const device of targetDevices) {
    if (!device.capabilities.includes(command.action)) {
      handleResult({
        action: command.action,
        commandId: command.id,
        completedAt: Date.now(),
        deviceId: device.id,
        error: `${device.name} does not support ${command.action}. Restart the agent to get the latest tools.`,
        ok: false
      });
      continue;
    }

    const peer = agentPeerForDevice(device.id);
    if (!peer) {
      handleResult({
        action: command.action,
        commandId: command.id,
        completedAt: Date.now(),
        deviceId: device.id,
        error: `${device.name} is listed online, but no live agent socket is connected.`,
        ok: false
      });
      continue;
    }

    device.status = "busy";
    device.lastSeen = Date.now();
    send(peer.socket, { type: "command.execute", command });
  }

  broadcastSnapshot(command.room);
}

function targetDevicesForCommand(command: FleetCommand) {
  return [...devices.values()].filter((device) => {
    if (device.room !== command.room || device.status === "offline") return false;
    return command.target.type === "all" || command.target.deviceId === device.id;
  });
}

function agentPeerForDevice(deviceId: string) {
  let selected: Extract<Peer, { kind: "agent" }> | undefined;
  for (const peer of peers.values()) {
    if (peer.kind === "agent" && peer.deviceId === deviceId && peer.socket.readyState === WebSocket.OPEN) {
      selected = peer;
    }
  }
  return selected;
}

function waitForCommandResults(commandId: string, expectedDeviceIds: string[], timeoutMs = 12000): Promise<CommandWaitReport> {
  const expectedDeviceIdsSet = new Set(expectedDeviceIds);
  if (expectedDeviceIdsSet.size === 0) {
    return Promise.resolve({ completed: true, pendingDeviceIds: [], results: [] });
  }

  return new Promise((resolve) => {
    const timeout = setTimeout(() => finishCommandWaiter(commandId, false), timeoutMs);
    commandWaiters.set(commandId, {
      expectedDeviceIds: expectedDeviceIdsSet,
      resolve,
      resultsByDeviceId: new Map(),
      timeout
    });
  });
}

function recordCommandWaitResult(result: CommandResult) {
  const waiter = commandWaiters.get(result.commandId);
  if (!waiter || !waiter.expectedDeviceIds.has(result.deviceId)) return;

  if (!waiter.resultsByDeviceId.has(result.deviceId)) {
    waiter.resultsByDeviceId.set(result.deviceId, result);
  }

  if (waiter.resultsByDeviceId.size >= waiter.expectedDeviceIds.size) {
    finishCommandWaiter(result.commandId, true);
  }
}

function finishCommandWaiter(commandId: string, completed: boolean) {
  const waiter = commandWaiters.get(commandId);
  if (!waiter) return;

  clearTimeout(waiter.timeout);
  commandWaiters.delete(commandId);
  const pendingDeviceIds = [...waiter.expectedDeviceIds].filter((deviceId) => !waiter.resultsByDeviceId.has(deviceId));
  waiter.resolve({
    completed,
    pendingDeviceIds,
    results: [...waiter.resultsByDeviceId.values()]
  });
}

const aiTools = [
  {
    type: "function",
    name: "list_devices",
    description: "List the Macs currently connected to the Computah room.",
    parameters: {
      type: "object",
      properties: {},
      additionalProperties: false
    }
  },
  {
    type: "function",
    name: "visual_click",
    description:
      "Take a screenshot of a Mac, use vision to locate a described visible target, then click those screen coordinates. Use this when browser page content or another visible UI element is not present in the Accessibility tree.",
    parameters: {
      type: "object",
      properties: {
        target: {
          type: "string",
          description: "Device id, device name, or all."
        },
        description: {
          type: "string",
          description: "Visible thing to click, for example: first YouTube video result, blue Save button, first search result."
        },
        app: {
          type: "string",
          description: "Optional app context for the click, such as Google Chrome, Safari, Arc, or Finder."
        }
      },
      required: ["target", "description"],
      additionalProperties: false
    }
  },
  {
    type: "function",
    name: "visual_type",
    description:
      "Take a screenshot of a Mac, use vision to locate a described text field or typing target, click it, then type text there. Use this when the user asks to type into a visible field, search box, chat box, form input, or webpage input.",
    parameters: {
      type: "object",
      properties: {
        target: {
          type: "string",
          description: "Device id, device name, or all."
        },
        description: {
          type: "string",
          description: "The visible input or typing target, for example: YouTube search box, message field, first text field."
        },
        text: {
          type: "string",
          description: "Text to type after the target is focused."
        },
        app: {
          type: "string",
          description: "Optional app context such as Google Chrome, Safari, Arc, Finder, or Messages."
        },
        clear: {
          type: "boolean",
          description: "Whether to clear existing text before typing."
        },
        press_enter: {
          type: "boolean",
          description: "Whether to press Return after typing."
        }
      },
      required: ["target", "description", "text"],
      additionalProperties: false
    }
  },
  {
    type: "function",
    name: "dispatch_command",
    description: "Send one action to one Mac or to every Mac in the room.",
    parameters: {
      type: "object",
      properties: {
        target: {
          type: "string",
          description: "Device id, device name, or all."
        },
        action: {
          type: "string",
          enum: [
            "open_url",
            "open_app",
            "quit_app",
            "say",
            "notify",
            "screenshot",
            "click",
            "type_text",
            "press_key",
            "agent_log",
            "list_apps",
            "get_app_state",
            "drag",
            "set_value",
            "scroll",
            "perform_secondary_action",
            "mcp_tool"
          ]
        },
        url: { type: "string" },
        app: { type: "string" },
        tool: { type: "string" },
        arguments: {
          type: "object",
          description: "Raw arguments for mcp_tool. Example: {\"app\":\"Safari\"}.",
          additionalProperties: true
        },
        element_index: { type: "string" },
        action_name: { type: "string" },
        direction: { type: "string", enum: ["up", "down", "left", "right"] },
        pages: { type: "number" },
        text: { type: "string" },
        key: { type: "string" },
        x: { type: "number" },
        y: { type: "number" },
        from_x: { type: "number" },
        from_y: { type: "number" },
        to_x: { type: "number" },
        to_y: { type: "number" },
        value: { type: "string" }
      },
      required: ["target", "action"],
      additionalProperties: false
    }
  }
] as const;

type AICommandResult = {
  mode: "openai" | "local";
  message: string;
  toolCalls: Array<{
    name: string;
    args: Record<string, unknown>;
    result: unknown;
  }>;
};

async function runOpenAICommand(room: string, prompt: string): Promise<AICommandResult> {
  const input = [
    {
      role: "user",
      content: [
        {
          type: "input_text",
          text: [
            `Room: ${room}`,
            `Current devices: ${JSON.stringify(roomDevices(room).map(publicDevice))}`,
            `User request: ${prompt}`
          ].join("\n")
        }
      ]
    }
  ];
  const instructions = computahInstructions(room);

  // Continue the room's existing thread so the model remembers prior turns.
  let first: Record<string, unknown>;
  try {
    first = await createOpenAIResponse({
      model: process.env.OPENAI_MODEL ?? "gpt-5",
      instructions,
      tools: aiTools,
      input,
      reasoning: { effort: "medium" },
      previous_response_id: lastResponseByRoom.get(room)
    });
  } catch (error) {
    // A stale/expired previous_response_id breaks the chain — reset and retry fresh once.
    if (lastResponseByRoom.has(room)) {
      lastResponseByRoom.delete(room);
      first = await createOpenAIResponse({
        model: process.env.OPENAI_MODEL ?? "gpt-5",
        instructions,
        tools: aiTools,
        input,
        reasoning: { effort: "medium" }
      });
    } else {
      throw error;
    }
  }
  rememberResponse(room, first);

  const toolCalls = extractToolCalls(first);
  if (toolCalls.length === 0) {
    const status = typeof first.status === "string" ? first.status : "completed";
    const truncated = status === "incomplete";
    return {
      mode: "openai",
      message:
        outputText(first) ||
        (truncated
          ? "The model ran out of output budget before acting. Try again or rephrase."
          : "No tool call returned. Try rephrasing the command."),
      toolCalls: []
    };
  }

  const toolResults = await Promise.all(toolCalls.map(async (call) => {
    const result = await executeAIToolWithResults(room, call.name, call.args);
    return { ...call, result };
  }));

  // The function_call items already live in `first` (server-side), so only send the outputs.
  const followupInput = toolResults.map((call) => ({
    type: "function_call_output",
    call_id: call.callId,
    output: JSON.stringify(call.result)
  }));

  const second = await createOpenAIResponse({
    model: process.env.OPENAI_MODEL ?? "gpt-5",
    instructions: "Report the tool results truthfully in one short sentence. Say completed only when the tool output has completed=true and the relevant device result is ok=true. If a device failed or timed out, say that plainly.",
    tools: aiTools,
    input: followupInput,
    previous_response_id: first.id as string,
    max_output_tokens: 200
  });
  rememberResponse(room, second);

  return {
    mode: "openai",
    message: outputText(second) || summarizeToolResults(toolResults),
    toolCalls: toolResults.map(({ name, args, result }) => ({ name, args, result }))
  };
}

function realtimeSessionConfig(room: string) {
  return {
    type: "realtime",
    model: process.env.OPENAI_REALTIME_MODEL ?? "gpt-realtime-2",
    instructions: computahInstructions(room),
    audio: {
      input: {
        noise_reduction: { type: "near_field" },
        transcription: {
          model: process.env.OPENAI_REALTIME_TRANSCRIPTION_MODEL ?? "gpt-4o-mini-transcribe"
        },
        turn_detection: {
          type: "semantic_vad",
          eagerness: "auto",
          create_response: true,
          interrupt_response: true
        }
      },
      output: {
        voice: process.env.OPENAI_REALTIME_VOICE ?? "marin"
      }
    },
    tools: aiTools,
    tool_choice: "auto",
    max_output_tokens: 2048
  };
}

function computahInstructions(room: string) {
  return [
    "You are Computer, the realtime voice interface for Computah, a conductor for a fleet of opted-in Macs.",
    "Speak naturally, briefly, and with confidence. The user is driving a live demo, so narrate what you are doing without overexplaining.",
    `Room: ${room}.`,
    `Current devices at session start: ${JSON.stringify(roomDevices(room).map(publicDevice))}.`,
    "Use function tools to control devices. Prefer dispatch_command when the user asks for an action.",
    "Resolve targets by device name. If the user says everyone, all, room, or fleet, target all.",
    "Never invent devices. If a target is unclear, use list_devices first or ask a short clarification.",
    "For URLs use open_url. For native apps use open_app. For spoken output use say. For visible pings use notify.",
    "If the user asks to close, quit, or exit an app, use quit_app with the app name.",
    "If the user asks what happened, asks for logs, or asks to debug an agent, use agent_log on the relevant Mac.",
    "If the user asks what is on a screen or wants UI elements, use get_app_state with an app name.",
    "If the user asks to list running apps, use list_apps.",
    "If the user asks to search for or look something up, use open_url with https://www.google.com/search?q= followed by the URL-encoded query.",
    "The macOS MCP tools are list_apps, get_app_state, click, drag, type_text, press_key, set_value, scroll, and perform_secondary_action.",
    "For any macOS MCP call that is not better represented by a named Computah action, use mcp_tool with tool and arguments.",
    "For UI element manipulation, use set_value or perform_secondary_action with element_index from get_app_state.",
    "If get_app_state cannot see the relevant page or visible UI content, use visual_click with a plain-language description instead of asking the user for coordinates or an element index.",
    "Use visual_click for requests like click the first video, click the first search result, click the blue button, or click the visible item. visual_click takes a screenshot, uses vision to choose coordinates, and then clicks.",
    "When the user asks to type into a visible field, search box, webpage input, chat box, or form, use visual_type with the field description and text. Do not use raw type_text unless the insertion point is already focused.",
    "For closing the current browser tab, prefer dispatch_command with action press_key, app Google Chrome or Safari, and key cmd+w.",
    "For clicking around or manipulating UI, first call get_app_state for the target app unless the user gives explicit coordinates or an element index.",
    "Do not click, type, or press keys unless the user explicitly asks for those actions.",
    "Always call a function tool when the user requests an action. Only answer with plain text when the user is asking a question that needs no device action.",
    "After dispatch_command, inspect the returned results. Say an action is done only if completed is true and the relevant result has ok true.",
    "If dispatch_command returns completed false, pending devices, or an error, tell the user the command was sent but did not confirm, or state the exact failure. Do not pretend success.",
    "This is one continuous conversation for the room. Remember earlier turns: previously mentioned apps, targets, and URLs can be referred to with words like it, that, them, again, or the same one."
  ].join(" ");
}

function safetyIdentifier(room: string) {
  return createHash("sha256").update(`computah:${room}`).digest("hex");
}

function rememberResponse(room: string, response: Record<string, unknown>) {
  if (typeof response.id === "string") lastResponseByRoom.set(room, response.id);
}

function runLocalAICommand(room: string, prompt: string): AICommandResult {
  const lower = prompt.toLowerCase();
  const target = inferTarget(room, prompt);
  const action = inferAction(prompt);
  const args = inferArgs(action, prompt);
  const result = executeAITool(room, "dispatch_command", { target, action, ...args });

  return {
    mode: "local",
    message: process.env.OPENAI_API_KEY
      ? summarizeToolResults([{ name: "dispatch_command", args: { target, action, ...args }, result }])
      : `Local fallback sent ${action} to ${lower.includes("all") || lower.includes("everyone") ? "all Macs" : target}. Set OPENAI_API_KEY for smarter planning.`,
    toolCalls: [{ name: "dispatch_command", args: { target, action, ...args }, result }]
  };
}

async function createOpenAIResponse(body: Record<string, unknown>) {
  const response = await fetch("https://api.openai.com/v1/responses", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${process.env.OPENAI_API_KEY}`
    },
    body: JSON.stringify(body)
  });

  if (!response.ok) {
    throw new Error(`OpenAI ${response.status}: ${await response.text()}`);
  }
  return (await response.json()) as Record<string, unknown>;
}

function extractToolCalls(response: Record<string, unknown>) {
  const output = Array.isArray(response.output) ? response.output : [];
  return output
    .filter((item): item is { type: string; name: string; arguments: string; call_id: string } => {
      return (
        typeof item === "object" &&
        item !== null &&
        (item as { type?: unknown }).type === "function_call" &&
        typeof (item as { name?: unknown }).name === "string" &&
        typeof (item as { arguments?: unknown }).arguments === "string" &&
        typeof (item as { call_id?: unknown }).call_id === "string"
      );
    })
    .map((item) => ({
      callId: item.call_id,
      name: item.name,
      args: safeJson(item.arguments)
    }));
}

function executeAITool(room: string, name: string, args: Record<string, unknown>) {
  if (name === "list_devices") {
    return { devices: roomDevices(room).map(publicDevice) };
  }

  if (name !== "dispatch_command") {
    return { ok: false, error: `Unknown tool: ${name}` };
  }

  const prepared = prepareAICommand(room, args);
  if (!prepared.ok) return prepared;

  dispatchCommand(prepared.command);
  return {
    ok: true,
    commandId: prepared.command.id,
    target: prepared.target.type === "all" ? "all" : prepared.target.deviceId,
    action: prepared.action
  };
}

async function executeAIToolWithResults(room: string, name: string, args: Record<string, unknown>) {
  if (name === "list_devices") {
    return { ok: true, devices: roomDevices(room).map(publicDevice) };
  }

  if (name === "visual_click") {
    return executeVisualClick(room, args);
  }

  if (name === "visual_type") {
    return executeVisualType(room, args);
  }

  if (name !== "dispatch_command") {
    return { ok: false, error: `Unknown tool: ${name}` };
  }

  const prepared = prepareAICommand(room, args);
  if (!prepared.ok) return prepared;

  const targetDevices = targetDevicesForCommand(prepared.command);
  if (targetDevices.length === 0) {
    return {
      ok: false,
      action: prepared.action,
      commandId: prepared.command.id,
      error: "No online Macs matched that target.",
      target: prepared.target.type === "all" ? "all" : prepared.target.deviceId
    };
  }

  const wait = waitForCommandResults(prepared.command.id, targetDevices.map((device) => device.id));
  dispatchCommand(prepared.command);
  const report = await wait;
  const results = report.results.map(publicCommandResult);
  const screenshot = report.results.find((result) => result.screenshot)?.screenshot;
  const ok = report.completed && report.pendingDeviceIds.length === 0 && report.results.every((result) => result.ok);

  return {
    ok,
    action: prepared.action,
    commandId: prepared.command.id,
    completed: report.completed,
    output: summarizeCommandReport(report),
    pendingDevices: report.pendingDeviceIds.map((deviceId) => devices.get(deviceId)?.name ?? deviceId),
    results,
    screenshot,
    target: prepared.target.type === "all" ? "all" : prepared.target.deviceId
  };
}

async function executeVisualClick(room: string, args: Record<string, unknown>) {
  if (!process.env.OPENAI_API_KEY) {
    return { ok: false, error: "OPENAI_API_KEY is required for visual_click." };
  }

  const description = stringFrom(args.description ?? args.text, "");
  if (!description) return { ok: false, error: "Missing visual click description." };

  const target = resolveAITarget(room, typeof args.target === "string" ? args.target : "all");
  if (!target) return { ok: false, error: `Could not find target: ${String(args.target)}` };

  const screenshotCommand = normalizeCommand({
    room,
    target,
    action: "screenshot",
    args: {},
    origin: "api"
  });
  if (!screenshotCommand) return { ok: false, error: "Could not prepare screenshot command." };

  const targetDevices = targetDevicesForCommand(screenshotCommand);
  if (targetDevices.length === 0) {
    return { ok: false, error: "No online Macs matched that target." };
  }

  const screenshotWait = waitForCommandResults(screenshotCommand.id, targetDevices.map((device) => device.id), 15000);
  dispatchCommand(screenshotCommand);
  const screenshotReport = await screenshotWait;
  const screenshotResults = screenshotReport.results.filter((result) => result.ok && result.screenshot);
  if (screenshotResults.length === 0) {
    return {
      ok: false,
      completed: screenshotReport.completed,
      error: summarizeCommandReport(screenshotReport) || "No screenshot was returned.",
      pendingDevices: screenshotReport.pendingDeviceIds.map((deviceId) => devices.get(deviceId)?.name ?? deviceId),
      results: screenshotReport.results.map(publicCommandResult)
    };
  }

  const app = stringFrom(args.app, "Finder");
  const clicks = await Promise.all(
    screenshotResults.map(async (screenshotResult) => {
      const device = devices.get(screenshotResult.deviceId);
      const screenshotSize = screenshotDimensionsFromDataUrl(screenshotResult.screenshot!);
      const point = await locateVisualTarget({
        app,
        description,
        deviceName: device?.name ?? screenshotResult.deviceId,
        screenshot: screenshotResult.screenshot!,
        screenshotSize
      });

      if (!point.found) {
        return {
          click: undefined,
          deviceId: screenshotResult.deviceId,
          point,
          screenshot: screenshotResult.screenshot
        };
      }

      const clickCommand = normalizeCommand({
        room,
        target: { type: "device", deviceId: screenshotResult.deviceId },
        action: "click",
        args: {
          app,
          x: Math.round(point.x),
          y: Math.round(point.y),
          screenshot_height: screenshotSize?.height,
          screenshot_width: screenshotSize?.width
        },
        origin: "api"
      });
      if (!clickCommand) {
        return {
          click: undefined,
          deviceId: screenshotResult.deviceId,
          point: { ...point, found: false, reason: "Could not prepare click command." },
          screenshot: screenshotResult.screenshot
        };
      }

      const clickWait = waitForCommandResults(clickCommand.id, [screenshotResult.deviceId], 12000);
      dispatchCommand(clickCommand);
      const clickReport = await clickWait;
      return {
        click: clickReport.results[0] ? publicCommandResult(clickReport.results[0]) : undefined,
        clickCompleted: clickReport.completed,
        deviceId: screenshotResult.deviceId,
        point,
        screenshot: screenshotResult.screenshot
      };
    })
  );

  const ok = clicks.every((item) => item.point.found && item.click?.ok);
  const firstScreenshot = clicks.find((item) => item.screenshot)?.screenshot;
  return {
    ok,
    action: "visual_click",
    completed: ok,
    output: clicks
      .map((item) => {
        const name = devices.get(item.deviceId)?.name ?? item.deviceId;
        if (!item.point.found) return `${name}: could not locate target (${item.point.reason || "uncertain"}).`;
        if (!item.click) return `${name}: located (${item.point.x}, ${item.point.y}) but did not click.`;
        return `${name}: ${item.click.ok ? "clicked" : "click failed"} (${item.point.x}, ${item.point.y})${item.click.error ? ` - ${item.click.error}` : ""}.`;
      })
      .join(" "),
    results: clicks.map((item) => ({
      click: item.click,
      deviceId: item.deviceId,
      deviceName: devices.get(item.deviceId)?.name ?? item.deviceId,
      point: item.point
    })),
    screenshot: firstScreenshot,
    target: target.type === "all" ? "all" : target.deviceId
  };
}

async function executeVisualType(room: string, args: Record<string, unknown>) {
  if (!process.env.OPENAI_API_KEY) {
    return { ok: false, error: "OPENAI_API_KEY is required for visual_type." };
  }

  const description = stringFrom(args.description ?? args.field ?? args.app, "");
  const text = stringFrom(args.text, "");
  if (!description) return { ok: false, error: "Missing visual type target description." };
  if (!text) return { ok: false, error: "Missing text to type." };

  const target = resolveAITarget(room, typeof args.target === "string" ? args.target : "all");
  if (!target) return { ok: false, error: `Could not find target: ${String(args.target)}` };

  const screenshotCommand = normalizeCommand({
    room,
    target,
    action: "screenshot",
    args: {},
    origin: "api"
  });
  if (!screenshotCommand) return { ok: false, error: "Could not prepare screenshot command." };

  const targetDevices = targetDevicesForCommand(screenshotCommand);
  if (targetDevices.length === 0) {
    return { ok: false, error: "No online Macs matched that target." };
  }

  const screenshotWait = waitForCommandResults(screenshotCommand.id, targetDevices.map((device) => device.id), 15000);
  dispatchCommand(screenshotCommand);
  const screenshotReport = await screenshotWait;
  const screenshotResults = screenshotReport.results.filter((result) => result.ok && result.screenshot);
  if (screenshotResults.length === 0) {
    return {
      ok: false,
      completed: screenshotReport.completed,
      error: summarizeCommandReport(screenshotReport) || "No screenshot was returned.",
      pendingDevices: screenshotReport.pendingDeviceIds.map((deviceId) => devices.get(deviceId)?.name ?? deviceId),
      results: screenshotReport.results.map(publicCommandResult)
    };
  }

  const app = stringFrom(args.app, "Finder");
  const clear = Boolean(args.clear);
  const pressEnter = Boolean(args.press_enter ?? args.pressEnter);

  const typed = await Promise.all(
    screenshotResults.map(async (screenshotResult) => {
      const device = devices.get(screenshotResult.deviceId);
      const screenshotSize = screenshotDimensionsFromDataUrl(screenshotResult.screenshot!);
      const point = await locateVisualTarget({
        app,
        description,
        deviceName: device?.name ?? screenshotResult.deviceId,
        screenshot: screenshotResult.screenshot!,
        screenshotSize
      });

      if (!point.found) {
        return {
          deviceId: screenshotResult.deviceId,
          point,
          screenshot: screenshotResult.screenshot,
          steps: []
        };
      }

      const steps: Array<{ step: string; result?: ReturnType<typeof publicCommandResult> }> = [];
      const clickReport = await runCommandWithResults(
        room,
        { type: "device", deviceId: screenshotResult.deviceId },
        "click",
        {
          app,
          x: Math.round(point.x),
          y: Math.round(point.y),
          screenshot_height: screenshotSize?.height,
          screenshot_width: screenshotSize?.width
        }
      );
      steps.push({ step: "click", result: clickReport.results[0] ? publicCommandResult(clickReport.results[0]) : undefined });
      if (!clickReport.completed || !clickReport.results[0]?.ok) {
        return { deviceId: screenshotResult.deviceId, point, screenshot: screenshotResult.screenshot, steps };
      }

      await sleep(250);

      if (clear) {
        const clearReport = await runCommandWithResults(room, { type: "device", deviceId: screenshotResult.deviceId }, "press_key", {
          app,
          key: "cmd+a"
        });
        steps.push({ step: "clear", result: clearReport.results[0] ? publicCommandResult(clearReport.results[0]) : undefined });
      }

      const typeReport = await runCommandWithResults(room, { type: "device", deviceId: screenshotResult.deviceId }, "type_text", {
        app,
        text
      });
      steps.push({ step: "type", result: typeReport.results[0] ? publicCommandResult(typeReport.results[0]) : undefined });

      if (pressEnter) {
        const enterReport = await runCommandWithResults(room, { type: "device", deviceId: screenshotResult.deviceId }, "press_key", {
          app,
          key: "return"
        });
        steps.push({ step: "enter", result: enterReport.results[0] ? publicCommandResult(enterReport.results[0]) : undefined });
      }

      return { deviceId: screenshotResult.deviceId, point, screenshot: screenshotResult.screenshot, steps };
    })
  );

  const ok = typed.every((item) => item.point.found && item.steps.some((step) => step.step === "type" && step.result?.ok));
  return {
    ok,
    action: "visual_type",
    completed: ok,
    output: typed
      .map((item) => {
        const name = devices.get(item.deviceId)?.name ?? item.deviceId;
        if (!item.point.found) return `${name}: could not locate typing target (${item.point.reason || "uncertain"}).`;
        const failed = item.steps.find((step) => step.result && !step.result.ok);
        if (failed) return `${name}: ${failed.step} failed (${failed.result?.error || "unknown error"}).`;
        return `${name}: typed into ${description}.`;
      })
      .join(" "),
    results: typed.map((item) => ({
      deviceId: item.deviceId,
      deviceName: devices.get(item.deviceId)?.name ?? item.deviceId,
      point: item.point,
      steps: item.steps
    })),
    screenshot: typed.find((item) => item.screenshot)?.screenshot,
    target: target.type === "all" ? "all" : target.deviceId
  };
}

async function runCommandWithResults(
  room: string,
  target: FleetCommand["target"],
  action: FleetCommand["action"],
  args: Record<string, unknown>,
  timeoutMs = 12000
) {
  const command = normalizeCommand({ room, target, action, args, origin: "api" });
  if (!command) return { completed: false, pendingDeviceIds: [], results: [] };
  const targetDevices = targetDevicesForCommand(command);
  const wait = waitForCommandResults(command.id, targetDevices.map((device) => device.id), timeoutMs);
  dispatchCommand(command);
  return wait;
}

type VisualClickPoint = {
  confidence: number;
  found: boolean;
  reason?: string;
  x: number;
  y: number;
};

async function locateVisualTarget({
  app,
  description,
  deviceName,
  screenshot,
  screenshotSize
}: {
  app: string;
  description: string;
  deviceName: string;
  screenshot: string;
  screenshotSize?: { width: number; height: number };
}): Promise<VisualClickPoint> {
  const response = await createOpenAIResponse({
    model: process.env.OPENAI_VISION_MODEL ?? process.env.OPENAI_MODEL ?? "gpt-5",
    instructions:
      "You locate clickable UI targets in screenshots. Return only compact JSON with keys: found boolean, x number, y number, confidence number from 0 to 1, reason string. Coordinates must be screenshot pixel coordinates from the top-left corner. Choose the center of the described target. If the target is not visible or is too ambiguous, set found=false and x=0 y=0.",
    input: [
      {
        role: "user",
        content: [
          {
            type: "input_text",
            text: [
              `Device: ${deviceName}`,
              `App context: ${app}`,
              `Target to click: ${description}`,
              screenshotSize ? `Original screenshot size: ${screenshotSize.width}x${screenshotSize.height} pixels.` : "",
              "Return JSON only."
            ].filter(Boolean).join("\n")
          },
          {
            type: "input_image",
            image_url: screenshot
          }
        ]
      }
    ],
    max_output_tokens: 250
  });

  return parseVisualClickPoint(outputText(response));
}

function parseVisualClickPoint(text: string): VisualClickPoint {
  const jsonText = text
    .trim()
    .replace(/^```(?:json)?/i, "")
    .replace(/```$/i, "")
    .trim();
  try {
    const parsed = JSON.parse(jsonText) as Partial<VisualClickPoint>;
    return {
      confidence: clamp01(typeof parsed.confidence === "number" ? parsed.confidence : 0),
      found: Boolean(parsed.found),
      reason: typeof parsed.reason === "string" ? parsed.reason : undefined,
      x: numberFrom(parsed.x, 0),
      y: numberFrom(parsed.y, 0)
    };
  } catch {
    return {
      confidence: 0,
      found: false,
      reason: `Vision model returned non-JSON: ${text.slice(0, 160)}`,
      x: 0,
      y: 0
    };
  }
}

function screenshotDimensionsFromDataUrl(dataUrl: string) {
  const [, base64] = dataUrl.split(",", 2);
  if (!base64) return undefined;
  const bytes = Buffer.from(base64, "base64");
  if (bytes.length < 24 || bytes.subarray(0, 8).toString("hex") !== "89504e470d0a1a0a") return undefined;
  return {
    height: bytes.readUInt32BE(20),
    width: bytes.readUInt32BE(16)
  };
}

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

type PreparedAICommand =
  | {
      ok: true;
      action: FleetCommand["action"];
      command: FleetCommand;
      target: FleetCommand["target"];
    }
  | {
      ok: false;
      error: string;
    };

function prepareAICommand(room: string, args: Record<string, unknown>): PreparedAICommand {
  const action = typeof args.action === "string" ? (args.action as FleetCommand["action"]) : undefined;
  if (!action) return { ok: false, error: "Missing action." };

  const target = resolveAITarget(room, typeof args.target === "string" ? args.target : "all");
  if (!target) return { ok: false, error: `Could not find target: ${String(args.target)}` };

  const command = normalizeCommand({
    room,
    target,
    action,
    args: argsForAIAction(action, args),
    origin: "api"
  });
  if (!command) return { ok: false, error: "Invalid command." };

  return { ok: true, action, command, target };
}

function resolveAITarget(room: string, target: string): FleetCommand["target"] | null {
  const normalized = target.trim().toLowerCase();
  if (!normalized || ["all", "everyone", "fleet", "room", "all macs"].includes(normalized)) {
    return { type: "all" };
  }

  const device = roomDevices(room).find((candidate) => {
    return (
      candidate.id.toLowerCase() === normalized ||
      candidate.name.toLowerCase() === normalized ||
      candidate.name.toLowerCase().includes(normalized)
    );
  });

  return device ? { type: "device", deviceId: device.id } : null;
}

function roomDevices(room: string) {
  return [...devices.values()].filter((device) => device.room === room && device.status !== "offline");
}

function publicDevice(device: DeviceInfo) {
  return {
    id: device.id,
    name: device.name,
    hostname: device.hostname,
    capabilities: device.capabilities,
    status: device.status
  };
}

function argsForAIAction(action: FleetCommand["action"], args: Record<string, unknown>) {
  if (action === "open_url") return { url: withProtocol(stringFrom(args.url ?? args.text ?? args.app, "https://github.com")) };
  if (action === "open_app") return { app: stringFrom(args.app ?? args.text, "Safari") };
  if (action === "quit_app") return { app: stringFrom(args.app ?? args.text, "Safari") };
  if (action === "say" || action === "notify" || action === "type_text") return { text: stringFrom(args.text ?? args.url ?? args.app, "Computah online") };
  if (action === "press_key") return { key: stringFrom(args.key ?? args.text, "return") };
  if (action === "click") return { app: stringFrom(args.app, "Finder"), x: numberFrom(args.x, 400), y: numberFrom(args.y, 300) };
  if (action === "list_apps") return {};
  if (action === "get_app_state") return { app: stringFrom(args.app ?? args.text, "Finder") };
  if (action === "drag") {
    return {
      app: stringFrom(args.app, "Finder"),
      from_x: numberFrom(args.from_x, 400),
      from_y: numberFrom(args.from_y, 300),
      to_x: numberFrom(args.to_x, 450),
      to_y: numberFrom(args.to_y, 300)
    };
  }
  if (action === "set_value") {
    return {
      app: stringFrom(args.app, "Finder"),
      element_index: stringFrom(args.element_index, "0"),
      value: stringFrom(args.value ?? args.text, "")
    };
  }
  if (action === "scroll") {
    return {
      app: stringFrom(args.app, "Finder"),
      element_index: stringFrom(args.element_index, "0"),
      direction: stringFrom(args.direction, "down"),
      pages: numberFrom(args.pages, 1)
    };
  }
  if (action === "perform_secondary_action") {
    return {
      app: stringFrom(args.app, "Finder"),
      element_index: stringFrom(args.element_index, "0"),
      action: stringFrom(args.action_name ?? args.value ?? args.text, "Press")
    };
  }
  if (action === "mcp_tool") {
    return {
      tool: stringFrom(args.tool ?? args.text, "list_apps"),
      arguments: objectFrom(args.arguments ?? args.args, {})
    };
  }
  return {};
}

function inferTarget(room: string, prompt: string) {
  const lower = prompt.toLowerCase();
  if (lower.includes("all") || lower.includes("everyone") || lower.includes("fleet")) return "all";
  const device = roomDevices(room).find((candidate) => lower.includes(candidate.name.toLowerCase()));
  return device?.name ?? "all";
}

function inferAction(prompt: string): FleetCommand["action"] {
  const lower = prompt.toLowerCase();
  if (lower.includes("screenshot") || lower.includes("screen shot") || lower.includes("watch")) return "screenshot";
  if (lower.includes("log") || lower.includes("what happened") || lower.includes("debug")) return "agent_log";
  if (lower.includes("list apps") || lower.includes("running apps")) return "list_apps";
  if (lower.includes("app state") || lower.includes("accessibility") || lower.includes("ui elements") || lower.includes("what is on")) return "get_app_state";
  if (lower.includes("drag")) return "drag";
  if (lower.includes("set value")) return "set_value";
  if (lower.includes("scroll")) return "scroll";
  if (lower.includes("secondary action") || lower.includes("perform action")) return "perform_secondary_action";
  if (lower.includes("mcp tool") || lower.includes("raw mcp")) return "mcp_tool";
  if (lower.includes("click")) return "click";
  if (lower.includes("type")) return "type_text";
  if (lower.includes("press") || lower.includes("key")) return "press_key";
  if (lower.includes("close") || lower.includes("quit") || lower.includes("exit app")) return "quit_app";
  if (lower.includes("say") || lower.includes("speak")) return "say";
  if (lower.includes("notify") || lower.includes("ping") || lower.includes("tell")) return "notify";
  if (lower.includes("open app") || lower.includes("launch")) return "open_app";
  return "open_url";
}

function inferArgs(action: FleetCommand["action"], prompt: string): Record<string, unknown> {
  const url = prompt.match(/https?:\/\/\S+|[a-z0-9-]+\.[a-z]{2,}\S*/i)?.[0];
  if (action === "open_url") return { url: url ?? "https://github.com" };
  if (action === "open_app") return { app: prompt.replace(/open app|launch/gi, "").trim() || "Safari" };
  if (action === "quit_app") return { app: inferClosableApp(prompt) };
  if (action === "get_app_state") return { app: inferApp(prompt) };
  if (action === "list_apps") return {};
  if (action === "click") {
    const coords = prompt.match(/(\d+)\s*,\s*(\d+)/);
    return { app: inferApp(prompt), x: coords ? Number(coords[1]) : 400, y: coords ? Number(coords[2]) : 300 };
  }
  if (action === "drag") {
    const nums = [...prompt.matchAll(/\d+/g)].map((match) => Number(match[0]));
    return {
      app: inferApp(prompt),
      from_x: nums[0] ?? 400,
      from_y: nums[1] ?? 300,
      to_x: nums[2] ?? 450,
      to_y: nums[3] ?? 300
    };
  }
  if (action === "scroll") {
    return {
      app: inferApp(prompt),
      element_index: prompt.match(/element\s+(\S+)/i)?.[1] ?? "0",
      direction: prompt.match(/\b(up|down|left|right)\b/i)?.[1]?.toLowerCase() ?? "down",
      pages: Number(prompt.match(/(\d+)\s+pages?/i)?.[1] ?? 1)
    };
  }
  if (action === "set_value") {
    return {
      app: inferApp(prompt),
      element_index: prompt.match(/element\s+(\S+)/i)?.[1] ?? "0",
      value: prompt.match(/to\s+(.+)$/i)?.[1] ?? ""
    };
  }
  if (action === "perform_secondary_action") {
    return {
      app: inferApp(prompt),
      element_index: prompt.match(/element\s+(\S+)/i)?.[1] ?? "0",
      action_name: prompt.match(/action\s+(\S+)/i)?.[1] ?? "Press"
    };
  }
  if (action === "press_key") return { key: prompt.match(/(?:press|key)\s+(.+)$/i)?.[1]?.trim() ?? "return" };
  if (action === "mcp_tool") return { tool: "list_apps", arguments: {} };
  if (action === "screenshot") return {};
  return { text: prompt.replace(/^(say|speak|notify|ping|tell)\s+/i, "").trim() || "Computah online" };
}

function inferApp(prompt: string) {
  const match = prompt.match(/\b(?:in|on|for|app)\s+([A-Za-z][A-Za-z0-9 ._-]{1,40})/i);
  return match?.[1]?.trim() ?? "Finder";
}

function inferClosableApp(prompt: string) {
  const match = prompt.match(/\b(?:close|quit|exit)\s+([A-Za-z][A-Za-z0-9 ._-]{1,40}?)(?:\s+on\b|\s+for\b|\s+in\b|$)/i);
  const app = match?.[1]?.trim() ?? inferApp(prompt);
  if (/^chrome$/i.test(app)) return "Google Chrome";
  return app;
}

function summarizeToolResults(toolResults: Array<{ name: string; result: unknown; args?: Record<string, unknown> }>) {
  const okCount = toolResults.filter((item) => Boolean((item.result as { ok?: unknown })?.ok)).length;
  const failedCount = toolResults.length - okCount;
  if (failedCount > 0) return `${failedCount} command${failedCount === 1 ? "" : "s"} failed or did not confirm.`;
  return okCount === 1 ? "Completed one Computah command." : `Completed ${okCount} Computah commands.`;
}

function summarizeCommandReport(report: CommandWaitReport) {
  const failures = report.results.filter((result) => !result.ok);
  if (failures.length > 0) {
    return failures
      .map((result) => `${devices.get(result.deviceId)?.name ?? result.deviceId}: ${result.error ?? "failed"}`)
      .join("; ");
  }

  if (!report.completed) {
    const pendingNames = report.pendingDeviceIds.map((deviceId) => devices.get(deviceId)?.name ?? deviceId);
    return `Waiting for ${pendingNames.join(", ")}.`;
  }

  return report.results
    .map((result) => `${devices.get(result.deviceId)?.name ?? result.deviceId}: ${result.output ?? `${result.action} completed`}`)
    .join("; ");
}

function publicCommandResult(result: CommandResult) {
  return {
    action: result.action,
    completedAt: result.completedAt,
    deviceId: result.deviceId,
    deviceName: devices.get(result.deviceId)?.name ?? result.deviceId,
    error: result.error,
    ok: result.ok,
    output: result.output,
    screenshot: result.screenshot
  };
}

function outputText(response: Record<string, unknown>) {
  if (typeof response.output_text === "string") return response.output_text;
  const output = Array.isArray(response.output) ? response.output : [];
  return output
    .flatMap((item) => {
      if (typeof item !== "object" || item === null) return [];
      const content = (item as { content?: unknown }).content;
      if (!Array.isArray(content)) return [];
      return content
        .filter((part) => typeof part === "object" && part !== null)
        .map((part) => (part as { text?: unknown }).text)
        .filter((text): text is string => typeof text === "string");
    })
    .join(" ")
    .trim();
}

function safeJson(value: string): Record<string, unknown> {
  try {
    const parsed = JSON.parse(value);
    return typeof parsed === "object" && parsed !== null ? (parsed as Record<string, unknown>) : {};
  } catch {
    return {};
  }
}

function stringFrom(value: unknown, fallback: string) {
  return typeof value === "string" && value.trim() ? value.trim() : fallback;
}

function numberFrom(value: unknown, fallback: number) {
  return typeof value === "number" && Number.isFinite(value) ? value : fallback;
}

function clamp01(value: number) {
  return Math.max(0, Math.min(1, value));
}

function objectFrom(value: unknown, fallback: Record<string, unknown>) {
  return typeof value === "object" && value !== null && !Array.isArray(value) ? (value as Record<string, unknown>) : fallback;
}

function withProtocol(value: string) {
  if (/^https?:\/\//i.test(value)) return value;
  return `https://${value}`;
}

function handleResult(result: CommandResult) {
  const device = devices.get(result.deviceId);
  if (!device) return;

  device.status = "online";
  device.lastSeen = Date.now();
  device.lastResult = result.ok ? result.output ?? `${result.action} completed` : result.error ?? `${result.action} failed`;
  if (result.screenshot) {
    device.lastScreenshot = result.screenshot;
  }

  addEvent(
    device.room,
    "result",
    `${device.name}: ${result.ok ? "ok" : "failed"} ${result.action}`,
    device.id,
    {
      action: result.action,
      commandId: result.commandId,
      completedAt: result.completedAt,
      error: result.error,
      ok: result.ok,
      output: result.output,
      screenshot: Boolean(result.screenshot)
    }
  );
  recordCommandWaitResult(result);
  broadcastSnapshot(device.room);
}

function snapshot(room: string): FleetSnapshotMessage {
  return {
    type: "fleet.snapshot",
    room,
    devices: [...devices.values()].filter((device) => device.room === room),
    events: eventsByRoom.get(room) ?? []
  };
}

function broadcastSnapshot(room: string) {
  const payload = snapshot(room);
  for (const peer of peers.values()) {
    if (peer.room === room && peer.socket.readyState === WebSocket.OPEN) {
      send(peer.socket, payload);
    }
  }
}

function send(socket: WebSocket, payload: unknown) {
  socket.send(JSON.stringify(payload));
}

function addEvent(
  room: string,
  kind: FleetEvent["kind"],
  message: string,
  deviceId?: string,
  details?: Record<string, unknown>
) {
  const list = eventsByRoom.get(room) ?? [];
  list.unshift({
    id: newId("evt"),
    room,
    kind,
    deviceId,
    message,
    details,
    at: Date.now()
  });
  eventsByRoom.set(room, list.slice(0, 80));
}

function roomNames() {
  return [...new Set([...devices.values()].map((device) => device.room))];
}

function publicBaseUrl(req: express.Request) {
  const hostHeader = req.get("host") ?? `localhost:${port}`;
  const [hostOnly] = hostHeader.split(":");
  const host = isLoopbackHost(hostOnly) ? `${localIPv4() ?? hostOnly}:${port}` : hostHeader;
  return `${req.protocol}://${host}`;
}

function hostedInstallCommand(baseUrl: string, room: string) {
  return [
    `curl -fsSL ${baseUrl}/install.sh | bash -s --`,
    `--room ${shellQuote(room)}`,
    `--name "$(scutil --get ComputerName 2>/dev/null || hostname)"`,
    "--allow-input"
  ].join(" ");
}

function agentAppDownloadUrl(baseUrl: string, room: string) {
  return `${baseUrl}/downloads/computah-agent-app.zip?room=${encodeURIComponent(room)}&v=${Date.now()}`;
}

function isLoopbackHost(host: string) {
  return host === "localhost" || host === "127.0.0.1" || host === "::1" || host === "[::1]";
}

function localIPv4() {
  for (const net of Object.values(os.networkInterfaces())) {
    for (const address of net ?? []) {
      if (address.family === "IPv4" && !address.internal) {
        return address.address;
      }
    }
  }
  return undefined;
}

async function ensureAgentArchive() {
  await fs.mkdir(path.dirname(agentArchivePath), { recursive: true });
  await runFile("tar", [
    "-czf",
    agentArchivePath,
    "-C",
    rootDir,
    "package.json",
    "package-lock.json",
    "tsconfig.json",
    "agent",
    "shared"
  ]);
}

async function agentSourceVersion() {
  const hash = createHash("sha256");
  const files = (
    await Promise.all(["package.json", "package-lock.json", "tsconfig.json", "agent", "shared"].map(agentSourceFiles))
  )
    .flat()
    .sort();

  for (const relativePath of files) {
    hash.update(relativePath);
    hash.update("\0");
    hash.update(await fs.readFile(path.join(rootDir, relativePath)));
    hash.update("\0");
  }

  return hash.digest("hex");
}

async function agentSourceFiles(relativePath: string): Promise<string[]> {
  const absolutePath = path.join(rootDir, relativePath);
  const stat = await fs.stat(absolutePath);
  if (stat.isFile()) return [relativePath];
  if (!stat.isDirectory()) return [];

  const entries = await fs.readdir(absolutePath, { withFileTypes: true });
  const nested = await Promise.all(
    entries
      .filter((entry) => !entry.name.startsWith("."))
      .map((entry) => agentSourceFiles(path.join(relativePath, entry.name)))
  );
  return nested.flat();
}

async function createAgentAppArchive(baseUrl: string, room: string) {
  await ensureAgentArchive();

  const wsUrl = baseUrl.replace(/^http:\/\//, "ws://").replace(/^https:\/\//, "wss://");
  const signing = agentSigningConfig();
  const tmpRoot = await fs.mkdtemp(path.join(os.tmpdir(), "computah-app-"));
  const appRoot = path.join(tmpRoot, "Computah Agent.app");
  const contentsRoot = path.join(appRoot, "Contents");
  const macosRoot = path.join(contentsRoot, "MacOS");
  const resourcesRoot = path.join(contentsRoot, "Resources");
  const archivePath = path.join(tmpRoot, "Computah-Agent.app.zip");

  await fs.mkdir(macosRoot, { recursive: true });
  await fs.mkdir(resourcesRoot, { recursive: true });
  await fs.writeFile(path.join(contentsRoot, "Info.plist"), renderAppInfoPlist(), "utf8");
  const scriptPath = path.join(resourcesRoot, "launcher.sh");
  await fs.writeFile(scriptPath, renderAppLauncher({ baseUrl, room, wsUrl }), "utf8");
  await fs.chmod(scriptPath, 0o755);
  await buildAgentAppLauncher(tmpRoot, path.join(macosRoot, "Computah Agent"));

  if (signing.identity) {
    await signAgentApp(appRoot, signing.identity);
  }

  await zipAgentApp(tmpRoot, archivePath);

  if (signing.notaryProfile) {
    if (!signing.identity) {
      throw new Error("COMPUTAH_NOTARY_PROFILE requires COMPUTAH_CODESIGN_IDENTITY.");
    }

    await notarizeAgentApp(archivePath, signing.notaryProfile);
    await stapleAgentApp(appRoot);
    await zipAgentApp(tmpRoot, archivePath);
  }

  return {
    archivePath,
    cleanup: () => {
      void fs.rm(tmpRoot, { recursive: true, force: true });
    }
  };
}

async function buildAgentAppLauncher(tmpRoot: string, outputPath: string) {
  const sourcePath = path.join(tmpRoot, "computah-launcher.c");
  await fs.writeFile(sourcePath, renderAppLauncherSource(), "utf8");
  await runFile(
    "xcrun",
    [
      "clang",
      "-arch",
      "arm64",
      "-arch",
      "x86_64",
      sourcePath,
      "-o",
      outputPath
    ],
    { maxBuffer: 10 * 1024 * 1024 }
  );
  await fs.chmod(outputPath, 0o755);
}

function agentSigningConfig() {
  return {
    identity: cleanEnv("COMPUTAH_CODESIGN_IDENTITY"),
    notaryProfile: cleanEnv("COMPUTAH_NOTARY_PROFILE")
  };
}

function cleanEnv(name: string) {
  const value = process.env[name]?.trim();
  return value || undefined;
}

async function signAgentApp(appRoot: string, identity: string) {
  console.log(`Signing Computah Agent.app with ${identity}`);
  await runFile(
    "codesign",
    [
      "--force",
      "--timestamp",
      "--options",
      "runtime",
      "--sign",
      identity,
      appRoot
    ],
    { maxBuffer: 10 * 1024 * 1024 }
  );
  await runFile("codesign", ["--verify", "--strict", "--verbose=2", appRoot], {
    maxBuffer: 10 * 1024 * 1024
  });
}

async function zipAgentApp(tmpRoot: string, archivePath: string) {
  await fs.rm(archivePath, { force: true });
  await runFile("ditto", ["-c", "-k", "--keepParent", "Computah Agent.app", archivePath], {
    cwd: tmpRoot,
    maxBuffer: 10 * 1024 * 1024
  });
}

async function notarizeAgentApp(archivePath: string, keychainProfile: string) {
  console.log(`Submitting Computah Agent.app for notarization with profile ${keychainProfile}`);
  await runFile(
    "xcrun",
    ["notarytool", "submit", archivePath, "--keychain-profile", keychainProfile, "--wait"],
    { maxBuffer: 10 * 1024 * 1024 }
  );
}

async function stapleAgentApp(appRoot: string) {
  console.log("Stapling Computah Agent.app notarization ticket");
  await runFile("xcrun", ["stapler", "staple", appRoot], {
    maxBuffer: 10 * 1024 * 1024
  });
  await runFile("spctl", ["--assess", "--type", "execute", "--verbose=4", appRoot], {
    maxBuffer: 10 * 1024 * 1024
  });
}

function renderAppLauncherSource() {
  return `#include <mach-o/dyld.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#ifndef PATH_MAX
#define PATH_MAX 4096
#endif

int main(void) {
  char executable[PATH_MAX];
  uint32_t executableSize = sizeof(executable);
  if (_NSGetExecutablePath(executable, &executableSize) != 0) {
    fprintf(stderr, "Computah launcher path is too long.\\n");
    return 1;
  }

  char resolved[PATH_MAX];
  const char *executablePath = realpath(executable, resolved) ? resolved : executable;
  char scriptPath[PATH_MAX];
  if (snprintf(scriptPath, sizeof(scriptPath), "%s", executablePath) >= (int)sizeof(scriptPath)) {
    fprintf(stderr, "Computah launcher path is too long.\\n");
    return 1;
  }

  char *contents = strstr(scriptPath, ".app/Contents/MacOS/");
  if (!contents) {
    fprintf(stderr, "Computah launcher could not find its app bundle.\\n");
    return 1;
  }

  contents += strlen(".app/Contents/");
  *contents = '\\0';
  if (strlcat(scriptPath, "Resources/launcher.sh", sizeof(scriptPath)) >= sizeof(scriptPath)) {
    fprintf(stderr, "Computah script path is too long.\\n");
    return 1;
  }

  execl("/bin/bash", "/bin/bash", scriptPath, (char *)NULL);
  perror("Computah launcher failed");
  return 1;
}
`;
}

function renderAppInfoPlist() {
  return `<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>CFBundleExecutable</key>
  <string>Computah Agent</string>
  <key>CFBundleIdentifier</key>
  <string>app.computah.agent</string>
  <key>CFBundleName</key>
  <string>Computah Agent</string>
  <key>CFBundleDisplayName</key>
  <string>Computah Agent</string>
  <key>CFBundlePackageType</key>
  <string>APPL</string>
  <key>CFBundleShortVersionString</key>
  <string>0.1.0</string>
  <key>NSHighResolutionCapable</key>
  <true/>
</dict>
</plist>
`;
}

function renderAppLauncher({ baseUrl, room, wsUrl }: { baseUrl: string; room: string; wsUrl: string }) {
  return `#!/usr/bin/env bash
set -euo pipefail

export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin:$PATH"

BASE_URL=${bashString(baseUrl)}
WS_URL=${bashString(wsUrl)}
ROOM=${bashString(room)}
NAME="$(scutil --get ComputerName 2>/dev/null || hostname)"
INSTALL_DIR="$HOME/.computah-agent"
NPM_CACHE_DIR="$INSTALL_DIR/.npm-cache"
VERSION_FILE="$INSTALL_DIR/.computah-agent-version"
LOG_DIR="$HOME/Library/Logs"
LOG_FILE="$LOG_DIR/Computah Agent.log"

mkdir -p "$LOG_DIR"
exec > >(tee -a "$LOG_FILE") 2>&1

on_error() {
  status="$?"
  echo "Computah Agent failed with exit code $status"
  dialog "Computah Agent failed. Open $LOG_FILE for details."
  exit "$status"
}

trap on_error ERR

notify() {
  osascript -e "display notification \\"$1\\" with title \\"Computah\\"" >/dev/null 2>&1 || true
}

dialog() {
  osascript -e "display dialog \\"$1\\" buttons {\\"OK\\"} default button \\"OK\\"" >/dev/null 2>&1 || true
}

find_node_tools() {
  if command -v npm >/dev/null 2>&1; then
    return 0
  fi

  if [[ -s "$HOME/.nvm/nvm.sh" ]]; then
    export NVM_DIR="$HOME/.nvm"
    # shellcheck disable=SC1090
    source "$HOME/.nvm/nvm.sh" >/dev/null 2>&1 || true
    nvm use --silent default >/dev/null 2>&1 || nvm use --silent node >/dev/null 2>&1 || true
    if command -v npm >/dev/null 2>&1; then
      return 0
    fi
  fi

  for npm_path in \\
    "$HOME"/.nvm/versions/node/*/bin/npm \\
    "$HOME"/.volta/bin/npm \\
    "$HOME"/.asdf/shims/npm \\
    /opt/homebrew/bin/npm \\
    /usr/local/bin/npm; do
    if [[ -x "$npm_path" ]]; then
      export PATH="$(dirname "$npm_path"):$PATH"
      return 0
    fi
  done

  return 1
}

if ! find_node_tools; then
  dialog "Computah needs Node.js/npm first. Install Node.js from nodejs.org, then open Computah Agent again."
  open "https://nodejs.org/"
  exit 1
fi

TMP_DIR="$(mktemp -d)"
cleanup() {
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

mkdir -p "$INSTALL_DIR"

REMOTE_VERSION="$(curl -fsSL "$BASE_URL/api/agent/version.txt" 2>/dev/null | tr -d '[:space:]' || true)"
LOCAL_VERSION="$(cat "$VERSION_FILE" 2>/dev/null | tr -d '[:space:]' || true)"
NEEDS_UPDATE=1

if [[ -n "$REMOTE_VERSION" && "$REMOTE_VERSION" == "$LOCAL_VERSION" && -f "$INSTALL_DIR/package.json" && -x "$INSTALL_DIR/node_modules/.bin/tsx" ]]; then
  NEEDS_UPDATE=0
fi

if [[ "$NEEDS_UPDATE" == "1" ]]; then
  if [[ -n "$LOCAL_VERSION" ]]; then
    notify "Updating Computah Agent..."
    echo "Updating Computah Agent from \${LOCAL_VERSION:-unknown} to \${REMOTE_VERSION:-unknown}"
  else
    notify "Installing Computah Agent..."
    echo "Installing Computah Agent version \${REMOTE_VERSION:-unknown}"
  fi

  curl -fsSL "$BASE_URL/downloads/computah-agent.tgz" -o "$TMP_DIR/computah-agent.tgz"
  rm -rf "$INSTALL_DIR/agent" "$INSTALL_DIR/shared" "$INSTALL_DIR/node_modules"
  rm -f "$INSTALL_DIR/package.json" "$INSTALL_DIR/package-lock.json" "$INSTALL_DIR/tsconfig.json"
  tar -xzf "$TMP_DIR/computah-agent.tgz" -C "$INSTALL_DIR"

  cd "$INSTALL_DIR"
  npm install --silent --include=dev --production=false --cache "$NPM_CACHE_DIR"

  if [[ -n "$REMOTE_VERSION" ]]; then
    printf "%s\\n" "$REMOTE_VERSION" > "$VERSION_FILE"
  fi
else
  echo "Computah Agent is up to date at $LOCAL_VERSION"
  notify "Starting Computah Agent..."
  cd "$INSTALL_DIR"
fi

if [[ ! -x "$INSTALL_DIR/node_modules/.bin/tsx" ]]; then
  echo "Missing $INSTALL_DIR/node_modules/.bin/tsx after npm install"
  dialog "Computah installed files, but dependencies did not finish. Open $LOG_FILE for details."
  exit 1
fi

notify "Computah connecting to room $ROOM..."

cat <<'EOF'
Computah Agent is running.

To enable screenshots:
  System Settings > Privacy & Security > Screen Recording > Computah Agent

To enable click/type:
  System Settings > Privacy & Security > Accessibility > Computah Agent

Leave this app running while the room is active.
EOF

exec npm run agent -- join --server "$WS_URL/ws" --room "$ROOM" --name "$NAME" --allow-input
`;
}

function renderInstallScript(baseUrl: string) {
  const wsUrl = baseUrl.replace(/^http:\/\//, "ws://").replace(/^https:\/\//, "wss://");

  return `#!/usr/bin/env bash
set -euo pipefail

ROOM="demo"
NAME="$(hostname)"
ALLOW_INPUT=""
INSTALL_DIR="$HOME/.computah-agent"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --room)
      ROOM="$2"
      shift 2
      ;;
    --name)
      NAME="$2"
      shift 2
      ;;
    --allow-input)
      ALLOW_INPUT="--allow-input"
      shift
      ;;
    --dir)
      INSTALL_DIR="$2"
      shift 2
      ;;
    *)
      echo "Unknown option: $1" >&2
      exit 1
      ;;
  esac
done

VERSION_FILE="$INSTALL_DIR/.computah-agent-version"

if ! command -v npm >/dev/null 2>&1; then
  echo "Computah needs Node/npm installed first." >&2
  exit 1
fi

TMP_DIR="$(mktemp -d)"
cleanup() {
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

mkdir -p "$INSTALL_DIR"

REMOTE_VERSION="$(curl -fsSL "${baseUrl}/api/agent/version.txt" 2>/dev/null | tr -d '[:space:]' || true)"
LOCAL_VERSION="$(cat "$VERSION_FILE" 2>/dev/null | tr -d '[:space:]' || true)"
NEEDS_UPDATE=1

if [[ -n "$REMOTE_VERSION" && "$REMOTE_VERSION" == "$LOCAL_VERSION" && -f "$INSTALL_DIR/package.json" && -x "$INSTALL_DIR/node_modules/.bin/tsx" ]]; then
  NEEDS_UPDATE=0
fi

if [[ "$NEEDS_UPDATE" == "1" ]]; then
  echo "Downloading Computah agent..."
  curl -fsSL "${baseUrl}/downloads/computah-agent.tgz" -o "$TMP_DIR/computah-agent.tgz"

  rm -rf "$INSTALL_DIR/agent" "$INSTALL_DIR/shared" "$INSTALL_DIR/node_modules"
  rm -f "$INSTALL_DIR/package.json" "$INSTALL_DIR/package-lock.json" "$INSTALL_DIR/tsconfig.json"
  tar -xzf "$TMP_DIR/computah-agent.tgz" -C "$INSTALL_DIR"

  cd "$INSTALL_DIR"
  echo "Installing agent dependencies..."
  npm install --silent --include=dev --production=false --cache "$INSTALL_DIR/.npm-cache"

  if [[ -n "$REMOTE_VERSION" ]]; then
    printf "%s\\n" "$REMOTE_VERSION" > "$VERSION_FILE"
  fi
else
  echo "Computah agent is up to date at $LOCAL_VERSION"
  cd "$INSTALL_DIR"
fi

if [[ ! -x "$INSTALL_DIR/node_modules/.bin/tsx" ]]; then
  echo "Missing $INSTALL_DIR/node_modules/.bin/tsx after npm install" >&2
  exit 1
fi

echo "Joining Computah room '$ROOM' as '$NAME'..."
echo "Server: ${wsUrl}/ws"
npm run agent -- join --server "${wsUrl}/ws" --room "$ROOM" --name "$NAME" $ALLOW_INPUT
`;
}

function renderJoinPage({
  appDownloadUrl,
  baseUrl,
  installCommand,
  room
}: {
  appDownloadUrl: string;
  baseUrl: string;
  installCommand: string;
  room: string;
}) {
  return `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Join Computah</title>
    <style>
      :root { color-scheme: dark; font-family: ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; background: #101114; color: #f6f3eb; }
      body { margin: 0; min-height: 100vh; display: grid; place-items: center; padding: 24px; background: linear-gradient(135deg, #2f3b35, #101114); }
      main { width: min(760px, 100%); border: 1px solid rgba(255,255,255,.14); border-radius: 8px; background: rgba(16,17,20,.78); padding: 24px; box-shadow: 0 24px 80px rgba(0,0,0,.35); }
      h1 { margin: 0 0 8px; font-size: 36px; letter-spacing: 0; }
      p { color: #c6c7bf; line-height: 1.5; }
      ol { color: #dfe1d8; line-height: 1.65; padding-left: 22px; }
      code { display: block; overflow-x: auto; white-space: nowrap; padding: 14px; border: 1px solid rgba(216,255,106,.24); border-radius: 8px; color: #d8ff6a; background: rgba(216,255,106,.08); }
      a, button { color: #101114; background: #d8ff6a; border: 0; border-radius: 8px; padding: 10px 12px; font-weight: 750; text-decoration: none; }
      .primary { display: inline-block; margin: 8px 0 12px; }
      .actions { display: flex; gap: 10px; flex-wrap: wrap; margin-top: 16px; }
      small { display: block; margin-top: 18px; color: #9da29a; }
    </style>
  </head>
  <body>
    <main>
      <h1>Join Computah</h1>
      <p>Room <strong>${escapeHtml(room)}</strong> is live. Download the Mac app, unzip it, then open it.</p>
      <a class="primary" href="${escapeHtml(appDownloadUrl)}">Download Computah Agent.app</a>
      <ol>
        <li>Open the downloaded zip.</li>
        <li>Open <strong>Computah Agent.app</strong>.</li>
        <li>If macOS blocks it, right-click the app and choose <strong>Open</strong>.</li>
        <li>Leave it running while this room is active.</li>
      </ol>
      <p>Terminal fallback:</p>
      <code>${escapeHtml(installCommand)}</code>
      <div class="actions">
        <button onclick="navigator.clipboard.writeText(${JSON.stringify(installCommand)})">Copy command</button>
        <a href="${escapeHtml(baseUrl)}?room=${encodeURIComponent(room)}">Open dashboard</a>
        <a href="${escapeHtml(baseUrl)}/downloads/computah-agent.tgz">Download raw agent</a>
      </div>
      <small>If the app says npm is missing, install Node.js first.</small>
    </main>
  </body>
</html>`;
}

function shellQuote(value: string) {
  return `'${value.replaceAll("'", "'\\''")}'`;
}

function bashString(value: string) {
  return `'${value.replaceAll("'", "'\\''")}'`;
}

function escapeHtml(value: string) {
  return value
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}
