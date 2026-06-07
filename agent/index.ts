#!/usr/bin/env node
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js";
import { execFile } from "node:child_process";
import crypto from "node:crypto";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { promisify } from "node:util";
import { WebSocket } from "ws";
import {
  AgentCapability,
  CommandExecuteMessage,
  CommandResult,
  DEFAULT_ROOM,
  FleetAction,
  ServerMessage
} from "../shared/protocol.js";

const runFile = promisify(execFile);
const agentLogPath = path.join(os.homedir(), "Library", "Logs", "Computah Agent.log");
let macUseClientPromise: Promise<Client> | undefined;

type AgentOptions = {
  server: string;
  room: string;
  name: string;
  id?: string;
  allowInput: boolean;
};

const args = process.argv.slice(2);
const options = parseArgs(args);

if (!options) {
  printHelp();
  process.exit(args.includes("--help") || args.includes("-h") ? 0 : 1);
}

const deviceId = options.id ?? stableDeviceId();
const capabilities: AgentCapability[] = [
  "open_url",
  "open_app",
  "quit_app",
  "say",
  "notify",
  "screenshot",
  "agent_log",
  "list_apps",
  "get_app_state",
  "drag",
  "set_value",
  "scroll",
  "perform_secondary_action",
  "mcp_tool"
];
if (options.allowInput) {
  capabilities.push("click", "type_text", "press_key");
}

connect(options);

function connect(opts: AgentOptions) {
  const wsUrl = normalizeWsUrl(opts.server);
  const socket = new WebSocket(wsUrl);

  socket.on("open", () => {
    socket.send(
      JSON.stringify({
        type: "agent.join",
        room: opts.room,
        device: {
          id: deviceId,
          name: opts.name,
          platform: `${os.type()} ${os.release()}`,
          hostname: os.hostname(),
          capabilities
        }
      })
    );
    console.log(`Computah agent joined ${opts.room} at ${wsUrl} as ${opts.name}`);
    console.log(opts.allowInput ? "Input control enabled." : "Input control locked. Pass --allow-input for click/type/key.");
    void appendAgentLog(`joined room=${opts.room} server=${wsUrl} name=${opts.name} input=${opts.allowInput}`);
  });

  socket.on("message", async (raw) => {
    let message: ServerMessage;
    try {
      message = JSON.parse(raw.toString()) as ServerMessage;
    } catch {
      return;
    }

    if (message.type === "server.ack") {
      console.log(message.message);
      return;
    }

    if (message.type === "command.execute") {
      const result = await executeCommand(message, opts);
      socket.send(JSON.stringify({ type: "command.result", result }));
    }
  });

  socket.on("close", () => {
    console.log("Disconnected. Reconnecting in 2s...");
    setTimeout(() => connect(opts), 2000);
  });

  socket.on("error", (error) => {
    console.error(`Agent connection error: ${error.message}`);
  });

  setInterval(() => {
    if (socket.readyState === WebSocket.OPEN) {
      socket.send(JSON.stringify({ type: "agent.status", deviceId, status: "online" }));
    }
  }, 10000).unref();
}

async function executeCommand(message: CommandExecuteMessage, opts: AgentOptions): Promise<CommandResult> {
  const { command } = message;
  void appendAgentLog(`command ${command.id} ${command.action} ${JSON.stringify(command.args)}`);
  const base = {
    commandId: command.id,
    deviceId,
    action: command.action,
    completedAt: Date.now()
  };

  try {
    const output = await runAction(command.action, command.args, opts);
    void appendAgentLog(`result ${command.id} ok ${output.output ?? "completed"}`);
    return { ...base, ok: true, ...output };
  } catch (error) {
    void appendAgentLog(`result ${command.id} failed ${error instanceof Error ? error.message : String(error)}`);
    return {
      ...base,
      ok: false,
      error: error instanceof Error ? error.message : String(error)
    };
  }
}

async function runAction(
  action: FleetAction,
  args: Record<string, unknown>,
  opts: AgentOptions
): Promise<{ output?: string; screenshot?: string }> {
  switch (action) {
    case "open_url": {
      const url = stringArg(args, "url");
      assertSafeUrl(url);
      await runFile("open", [url]);
      return { output: `Opened ${url}` };
    }

    case "open_app": {
      const app = stringArg(args, "app");
      await runFile("open", ["-a", app]);
      return { output: `Opened ${app}` };
    }

    case "quit_app": {
      const app = normalizeAppName(stringArg(args, "app"));
      await runFile("osascript", ["-e", `tell ${appleApplicationRef(app)} to quit`]);
      return { output: `Quit ${app}` };
    }

    case "say": {
      const text = stringArg(args, "text");
      await runFile("say", [text]);
      return { output: "Spoke text" };
    }

    case "notify": {
      const text = stringArg(args, "text");
      await runFile("osascript", [
        "-e",
        `display notification ${appleString(text)} with title ${appleString("Computah")}`
      ]);
      return { output: "Notification shown" };
    }

    case "screenshot": {
      const shotPath = path.join(os.tmpdir(), `computah-${Date.now()}.png`);
      try {
        await runFile("screencapture", ["-x", "-t", "png", shotPath]);
      } catch {
        throw new Error(
          "Screen Recording permission is required for Terminal. Enable it in System Settings > Privacy & Security > Screen Recording, then restart the agent."
        );
      }
      const bytes = await fs.readFile(shotPath);
      await fs.rm(shotPath, { force: true });
      return {
        output: "Screenshot captured",
        screenshot: `data:image/png;base64,${bytes.toString("base64")}`
      };
    }

    case "click": {
      assertInputAllowed(opts);
      const x = numberArg(args, "x");
      const y = numberArg(args, "y");
      const screenshotWidth = optionalNumberArg(args, "screenshot_width");
      const screenshotHeight = optionalNumberArg(args, "screenshot_height");
      if (screenshotWidth && screenshotHeight) {
        const point = await screenshotPixelToScreenPoint(x, y, screenshotWidth, screenshotHeight);
        try {
          await systemClick(point.x, point.y);
          return { output: `Clicked ${point.x}, ${point.y} from screenshot pixel ${x}, ${y}` };
        } catch (error) {
          void appendAgentLog(`scaled click fallback: ${error instanceof Error ? error.message : String(error)}`);
        }
      }
      return callMacUseWithFallback("click", { app: stringArg(args, "app", "Finder"), x, y }, async () => {
        await systemClick(x, y);
        return { output: `Clicked ${x}, ${y}` };
      });
    }

    case "type_text": {
      assertInputAllowed(opts);
      const text = stringArg(args, "text");
      return callMacUseWithFallback("type_text", { app: stringArg(args, "app", "Finder"), text }, async () => {
        await runFile("osascript", ["-e", `tell application "System Events" to keystroke ${appleString(text)}`]);
        return { output: "Typed text" };
      });
    }

    case "press_key": {
      assertInputAllowed(opts);
      const key = stringArg(args, "key");
      return callMacUseWithFallback("press_key", { app: stringArg(args, "app", "Finder"), key }, async () => {
        await runFile("osascript", ["-e", keyScript(key)]);
        return { output: `Pressed ${key}` };
      });
    }

    case "agent_log": {
      const output = await readAgentLog();
      return { output };
    }

    case "list_apps":
    case "get_app_state":
    case "drag":
    case "set_value":
    case "scroll":
    case "perform_secondary_action": {
      return callMacUse(action, args);
    }

    case "mcp_tool": {
      const tool = stringArg(args, "tool");
      const toolArgs = objectArg(args, "arguments", objectArg(args, "args", {}));
      assertMcpToolAllowed(tool, opts);
      return callMacUse(tool, toolArgs);
    }
  }
}

async function callMacUse(toolName: string, args: Record<string, unknown>): Promise<{ output?: string; screenshot?: string }> {
  const client = await getMacUseClient();
  const result = await client.callTool({
    name: toolName,
    arguments: args
  });
  const formatted = formatMcpResult(result as unknown);
  if (isMcpError(result)) {
    throw new Error(formatted.output ?? `${toolName} failed`);
  }
  return formatted;
}

async function callMacUseWithFallback(
  toolName: "click" | "type_text" | "press_key",
  args: Record<string, unknown>,
  fallback: () => Promise<{ output?: string; screenshot?: string }>
) {
  try {
    return await callMacUse(toolName, args);
  } catch (error) {
    void appendAgentLog(`mac-use fallback ${toolName}: ${error instanceof Error ? error.message : String(error)}`);
    return fallback();
  }
}

function assertMcpToolAllowed(toolName: string, opts: AgentOptions) {
  if (!/^[a-zA-Z0-9_.-]+$/.test(toolName)) {
    throw new Error(`Invalid MCP tool name: ${toolName}`);
  }

  if (!opts.allowInput && !["list_apps", "get_app_state"].includes(toolName)) {
    throw new Error(`Input control is locked. Restart the agent with --allow-input to use ${toolName}.`);
  }
}

async function getMacUseClient() {
  macUseClientPromise ??= startMacUseClient();
  return macUseClientPromise;
}

async function startMacUseClient() {
  const client = new Client({ name: "computah-agent", version: "0.1.0" });
  const transport = new StdioClientTransport({
    command: process.execPath,
    args: [path.join(process.cwd(), "node_modules", "mac-use", "bin", "mac-use.js")],
    stderr: "pipe"
  });

  transport.stderr?.on("data", (chunk) => {
    void appendAgentLog(`mac-use stderr ${String(chunk).trim()}`);
  });

  await client.connect(transport);
  const tools = await client.listTools();
  void appendAgentLog(`mac-use ready tools=${tools.tools.map((tool) => tool.name).join(",")}`);
  return client;
}

type MpcContentPart = { type: string; text?: string; data?: string; mimeType?: string };

function formatMcpResult(rawResult: unknown): { output?: string; screenshot?: string } {
  const result =
    typeof rawResult === "object" && rawResult !== null ? (rawResult as { content?: unknown; structuredContent?: unknown; toolResult?: unknown }) : {};
  const content = Array.isArray(result.content) ? (result.content as MpcContentPart[]) : [];
  const textParts = content
    .filter((part) => part.type === "text" && typeof part.text === "string")
    .map((part) => part.text)
    .filter((text): text is string => typeof text === "string");
  const image = content.find((part) => part.type === "image" && typeof part.data === "string");
  const structured = extractStructuredResult(result, textParts);
  const output = structured ? summarizeStructuredResult(structured) : textParts.join("\n").trim();
  const screenshot =
    extractScreenshot(structured) ??
    (image?.data ? `data:${image.mimeType ?? "image/png"};base64,${image.data}` : undefined);

  return {
    output: output || "mac-use completed",
    screenshot
  };
}

function extractStructuredResult(
  result: { structuredContent?: unknown },
  textParts: string[]
): Record<string, unknown> | undefined {
  if (typeof result.structuredContent === "object" && result.structuredContent !== null) {
    return result.structuredContent as Record<string, unknown>;
  }

  for (const text of textParts) {
    try {
      const parsed = JSON.parse(text);
      if (typeof parsed === "object" && parsed !== null) return parsed as Record<string, unknown>;
    } catch {
      continue;
    }
  }
  return undefined;
}

function summarizeStructuredResult(result: Record<string, unknown>) {
  if (result.ok === false) {
    const error = result.error as { message?: unknown } | undefined;
    return typeof error?.message === "string" ? error.message : JSON.stringify(result, null, 2);
  }

  const snapshot = result.snapshot as { windowTitle?: unknown; treeText?: unknown; elements?: unknown[] } | undefined;
  const data = result.data as { apps?: unknown[] } | undefined;
  if (Array.isArray(data?.apps)) {
    return `Apps: ${data.apps
      .slice(0, 12)
      .map((app) => (typeof app === "object" && app !== null ? (app as { name?: unknown }).name : undefined))
      .filter((name): name is string => typeof name === "string")
      .join(", ")}`;
  }
  if (snapshot) {
    const title = typeof snapshot.windowTitle === "string" ? snapshot.windowTitle : "window";
    const elementCount = Array.isArray(snapshot.elements) ? snapshot.elements.length : 0;
    const treeText = typeof snapshot.treeText === "string" ? snapshot.treeText.slice(0, 1600) : "";
    return `${title} (${elementCount} elements)\n${treeText}`.trim();
  }

  return JSON.stringify(result, null, 2).slice(0, 5000);
}

function extractScreenshot(result?: Record<string, unknown>) {
  const artifacts = result?.artifacts as { screenshotBase64?: unknown; screenshotMimeType?: unknown } | undefined;
  if (typeof artifacts?.screenshotBase64 !== "string") return undefined;
  const mimeType = typeof artifacts.screenshotMimeType === "string" ? artifacts.screenshotMimeType : "image/png";
  return `data:${mimeType};base64,${artifacts.screenshotBase64}`;
}

function parseArgs(argv: string[]): AgentOptions | null {
  if (argv[0] !== "join") return null;

  const get = (name: string, fallback?: string) => {
    const index = argv.indexOf(name);
    return index >= 0 ? argv[index + 1] : fallback;
  };

  return {
    server: get("--server", "ws://localhost:8787/ws")!,
    room: get("--room", DEFAULT_ROOM)!,
    name: get("--name", os.hostname())!,
    id: get("--id"),
    allowInput: argv.includes("--allow-input")
  };
}

function printHelp() {
  console.log(`
Computah Agent

Usage:
  npm run agent -- join --server ws://localhost:8787/ws --room demo --name "Alex Mac"
  npm run agent -- join --server ws://localhost:8787/ws --room demo --allow-input

Flags:
  --server       WebSocket relay URL. Defaults to ws://localhost:8787/ws
  --room         Room code. Defaults to demo
  --name         Device name. Defaults to hostname
  --id           Device id override for local rehearsal
  --allow-input  Enables click/type/keyboard actions after macOS Accessibility permission
`);
}

function normalizeWsUrl(input: string): string {
  if (input.startsWith("ws://") || input.startsWith("wss://")) return input;
  if (input.startsWith("http://")) return input.replace("http://", "ws://").replace(/\/$/, "") + "/ws";
  if (input.startsWith("https://")) return input.replace("https://", "wss://").replace(/\/$/, "") + "/ws";
  return input;
}

function stableDeviceId(): string {
  const seed = `${os.hostname()}-${os.userInfo().username}`;
  return `mac_${crypto.createHash("sha1").update(seed).digest("hex").slice(0, 10)}`;
}

function stringArg(args: Record<string, unknown>, key: string, fallback?: string): string {
  const value = args[key];
  if (typeof value !== "string" || value.trim().length === 0) {
    if (fallback !== undefined) return fallback;
    throw new Error(`Missing string argument: ${key}`);
  }
  return value;
}

function objectArg(args: Record<string, unknown>, key: string, fallback: Record<string, unknown>): Record<string, unknown> {
  const value = args[key];
  return typeof value === "object" && value !== null && !Array.isArray(value) ? (value as Record<string, unknown>) : fallback;
}

function numberArg(args: Record<string, unknown>, key: string): number {
  const value = args[key];
  if (typeof value !== "number" || !Number.isFinite(value)) {
    throw new Error(`Missing number argument: ${key}`);
  }
  return Math.round(value);
}

function optionalNumberArg(args: Record<string, unknown>, key: string): number | undefined {
  const value = args[key];
  return typeof value === "number" && Number.isFinite(value) ? Math.round(value) : undefined;
}

async function screenshotPixelToScreenPoint(x: number, y: number, screenshotWidth: number, screenshotHeight: number) {
  const screen = await mainScreenPointSize();
  return {
    x: Math.round((x / screenshotWidth) * screen.width),
    y: Math.round((y / screenshotHeight) * screen.height)
  };
}

async function mainScreenPointSize() {
  const { stdout } = await runFile("osascript", [
    "-l",
    "JavaScript",
    "-e",
    'ObjC.import("AppKit"); const f=$.NSScreen.mainScreen.frame; console.log(`${Math.round(f.size.width)} ${Math.round(f.size.height)}`);'
  ]);
  const match = stdout.match(/(\d+)\s+(\d+)/);
  if (!match) throw new Error(`Could not read screen size: ${stdout}`);
  return { width: Number(match[1]), height: Number(match[2]) };
}

async function systemClick(x: number, y: number) {
  await runFile("osascript", ["-e", `tell application "System Events" to click at {${x}, ${y}}`]);
}

function assertSafeUrl(url: string) {
  const parsed = new URL(url);
  if (!["http:", "https:"].includes(parsed.protocol)) {
    throw new Error("Only http and https URLs are allowed.");
  }
}

function assertInputAllowed(opts: AgentOptions) {
  if (!opts.allowInput) {
    throw new Error("Input control is locked. Restart the agent with --allow-input.");
  }
}

function appleString(value: string): string {
  return `"${value.replaceAll("\\", "\\\\").replaceAll('"', '\\"')}"`;
}

function normalizeAppName(app: string) {
  const aliases: Record<string, string> = {
    chrome: "Google Chrome",
    googlechrome: "Google Chrome",
    arc: "Arc",
    safari: "Safari",
    finder: "Finder"
  };
  const normalized = app.toLowerCase().replace(/\s+/g, "");
  return aliases[normalized] ?? app;
}

function appleApplicationRef(app: string) {
  return app.includes(".") && /^[a-zA-Z0-9.-]+$/.test(app) ? `application id ${appleString(app)}` : `application ${appleString(app)}`;
}

function isMcpError(result: unknown) {
  if (typeof result !== "object" || result === null) return false;
  const maybe = result as { isError?: unknown; structuredContent?: unknown };
  if (maybe.isError === true) return true;
  const structured = maybe.structuredContent;
  return typeof structured === "object" && structured !== null && (structured as { ok?: unknown }).ok === false;
}

function keyScript(combo: string): string {
  const parts = combo.toLowerCase().split("+").map((part) => part.trim()).filter(Boolean);
  const key = parts.pop();
  if (!key) throw new Error("Missing key.");

  const modifiers = parts.map((part) => {
    if (part === "cmd" || part === "command") return "command down";
    if (part === "ctrl" || part === "control") return "control down";
    if (part === "alt" || part === "option") return "option down";
    if (part === "shift") return "shift down";
    throw new Error(`Unsupported modifier: ${part}`);
  });

  const keyCodes: Record<string, number> = {
    return: 36,
    enter: 36,
    tab: 48,
    escape: 53,
    esc: 53,
    space: 49,
    delete: 117,
    backspace: 51,
    up: 126,
    down: 125,
    left: 123,
    right: 124
  };

  const using = modifiers.length ? ` using {${modifiers.join(", ")}}` : "";
  if (keyCodes[key] !== undefined) {
    return `tell application "System Events" to key code ${keyCodes[key]}${using}`;
  }

  if (key.length === 1) {
    return `tell application "System Events" to keystroke ${appleString(key)}${using}`;
  }

  throw new Error(`Unsupported key: ${key}`);
}

async function appendAgentLog(message: string) {
  await fs.mkdir(path.dirname(agentLogPath), { recursive: true });
  await fs.appendFile(agentLogPath, `[${new Date().toISOString()}] ${message}\n`, "utf8");
}

async function readAgentLog() {
  try {
    const text = await fs.readFile(agentLogPath, "utf8");
    return text.split("\n").slice(-80).join("\n").trim() || "Log is empty.";
  } catch {
    return "No Computah agent log found yet.";
  }
}
