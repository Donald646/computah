import React, { useEffect, useRef, useState } from "react";
import { createRoot } from "react-dom/client";
import { Activity, Copy, Download, History, Mic, MicOff, Monitor, PhoneOff, Terminal, X } from "lucide-react";
import {
  DEFAULT_ROOM,
  DeviceInfo,
  FleetEvent,
  FleetSnapshotMessage,
  ServerMessage
} from "../shared/protocol.js";
import "./styles.css";

type ToolCall = { name: string; args?: Record<string, unknown>; result?: unknown };
type ChatMessage = {
  id: string;
  role: "user" | "assistant";
  text: string;
  tools?: ToolCall[];
  screenshot?: string;
  at: number;
};
type RealtimeStatus = "idle" | "connecting" | "listening" | "thinking" | "speaking" | "error";
type RealtimeEvent = { type?: unknown; [key: string]: unknown };
type RealtimeFunctionCall = {
  name: string;
  arguments: string;
  call_id: string;
};
type ActiveTab = "voice" | "connections";
type InviteInfo = {
  room: string;
  conductorUrl: string;
  appDownloadUrl: string;
  installCommand: string;
};

let messageSeq = 0;
const nextId = () => `m${Date.now().toString(36)}_${messageSeq++}`;

function App() {
  const room = new URLSearchParams(location.search).get("room") || DEFAULT_ROOM;
  const [devices, setDevices] = useState<DeviceInfo[]>([]);
  const [events, setEvents] = useState<FleetEvent[]>([]);
  const [invite, setInvite] = useState<InviteInfo | null>(null);
  const [connected, setConnected] = useState(false);
  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [showHistory, setShowHistory] = useState(false);
  const [activeTab, setActiveTab] = useState<ActiveTab>("voice");
  const [copied, setCopied] = useState<string>("");
  const [aiResponse, setAiResponse] = useState("");
  const [aiBusy, setAiBusy] = useState(false);
  const [micMuted, setMicMuted] = useState(false);
  const [voiceText, setVoiceText] = useState("");
  const [realtimeStatus, setRealtimeStatus] = useState<RealtimeStatus>("idle");
  const [realtimeError, setRealtimeError] = useState("");
  const wsRef = useRef<WebSocket | null>(null);
  const peerRef = useRef<RTCPeerConnection | null>(null);
  const dataChannelRef = useRef<RTCDataChannel | null>(null);
  const mediaStreamRef = useRef<MediaStream | null>(null);
  const audioRef = useRef<HTMLAudioElement | null>(null);
  const feedRef = useRef<HTMLDivElement | null>(null);
  const assistantDraftRef = useRef("");
  const pendingToolCallsRef = useRef(new Set<string>());
  const seenAssistantItemsRef = useRef(new Set<string>());
  const seenScreenshotsRef = useRef(new Set<string>());
  const seenUserItemsRef = useRef(new Set<string>());

  useEffect(() => {
    const ws = new WebSocket(`${location.protocol === "https:" ? "wss" : "ws"}://${location.host}/ws`);
    wsRef.current = ws;

    ws.addEventListener("open", () => {
      setConnected(true);
      ws.send(JSON.stringify({ type: "conductor.join", room }));
    });

    ws.addEventListener("close", () => setConnected(false));

    ws.addEventListener("message", (raw) => {
      const message = JSON.parse(raw.data) as ServerMessage;
      if (message.type === "fleet.snapshot") {
        const snapshot = message as FleetSnapshotMessage;
        setDevices(snapshot.devices);
        setEvents(snapshot.events);
        addNewScreenshots(snapshot.devices);
      }
    });

    return () => ws.close();
  }, [room]);

  useEffect(() => {
    let cancelled = false;

    async function loadInvite() {
      try {
        const response = await fetch(`/api/invite?room=${encodeURIComponent(room)}`);
        if (!response.ok) return;
        const result = (await response.json()) as InviteInfo;
        if (!cancelled) setInvite(result);
      } catch {
        if (!cancelled) setInvite(null);
      }
    }

    void loadInvite();
    return () => {
      cancelled = true;
    };
  }, [room]);

  useEffect(() => {
    feedRef.current?.scrollTo({ top: feedRef.current.scrollHeight, behavior: "smooth" });
  }, [messages, showHistory]);

  useEffect(() => {
    return () => closeRealtimeSession();
  }, []);

  const onlineCount = devices.filter((device) => device.status !== "offline").length;
  const listening = realtimeStatus !== "idle" && realtimeStatus !== "error";
  const micLive = listening && !micMuted && realtimeStatus !== "connecting";
  const faceState =
    aiBusy || realtimeStatus === "thinking" || realtimeStatus === "speaking"
      ? "thinking"
      : micLive
        ? "listening"
        : connected
          ? "ready"
          : "offline";
  const statusLabel =
    realtimeStatus === "connecting"
      ? "Connecting..."
      : realtimeStatus === "thinking"
        ? "Thinking..."
        : realtimeStatus === "speaking"
          ? "Speaking..."
          : realtimeStatus === "error"
            ? "Voice offline"
            : listening && micMuted
              ? "Mic muted"
              : listening
              ? "Realtime live"
              : connected
                ? `${onlineCount} connected`
                : "Offline";
  const latestLine = micMuted && listening ? aiResponse || "Muted. Computer is still connected." : voiceText || aiResponse || realtimeError || "Talk to Computer";

  function pushMessage(message: Omit<ChatMessage, "id" | "at">) {
    setMessages((prev) => [...prev, { ...message, id: nextId(), at: Date.now() }]);
  }

  function addNewScreenshots(snapshotDevices: DeviceInfo[]) {
    for (const device of snapshotDevices) {
      const screenshot = device.lastScreenshot;
      if (!screenshot || seenScreenshotsRef.current.has(screenshot)) continue;

      seenScreenshotsRef.current.add(screenshot);
      pushMessage({
        role: "assistant",
        text: `Screenshot from ${device.name}`,
        screenshot,
        tools: [{ name: "screenshot", args: { target: device.name }, result: device.lastResult }]
      });
    }
  }

  async function copyValue(label: string, value: string) {
    try {
      await navigator.clipboard.writeText(value);
      setCopied(label);
      window.setTimeout(() => setCopied(""), 1200);
    } catch {
      setCopied("");
    }
  }

  async function startRealtimeSession() {
    if (peerRef.current || realtimeStatus === "connecting") return;

    setRealtimeStatus("connecting");
    setRealtimeError("");
    setAiResponse("");
    setMicMuted(false);
    setVoiceText("Connecting to Computer...");

    try {
      if (!navigator.mediaDevices?.getUserMedia) {
        throw new Error("This browser cannot access the microphone.");
      }

      const pc = new RTCPeerConnection();
      peerRef.current = pc;

      pc.ontrack = (event) => {
        const audio = audioRef.current;
        if (!audio) return;
        audio.srcObject = event.streams[0];
        void audio.play().catch(() => undefined);
      };

      pc.onconnectionstatechange = () => {
        if (pc.connectionState === "failed") {
          setRealtimeStatus("error");
          setRealtimeError("Realtime connection failed.");
        }
      };

      const stream = await navigator.mediaDevices.getUserMedia({
        audio: {
          echoCancellation: true,
          noiseSuppression: true,
          autoGainControl: true
        }
      });
      mediaStreamRef.current = stream;
      for (const track of stream.getAudioTracks()) {
        track.enabled = true;
        pc.addTrack(track, stream);
      }

      const dataChannel = pc.createDataChannel("oai-events");
      dataChannelRef.current = dataChannel;
      dataChannel.addEventListener("open", () => {
        setRealtimeStatus("listening");
        setVoiceText("");
        setAiResponse("I'm listening.");
      });
      dataChannel.addEventListener("message", (event) => {
        try {
          handleRealtimeEvent(JSON.parse(event.data) as RealtimeEvent);
        } catch (error) {
          const message = error instanceof Error ? error.message : String(error);
          setRealtimeError(message);
        }
      });
      dataChannel.addEventListener("close", () => {
        if (peerRef.current) setRealtimeStatus("idle");
      });

      const offer = await pc.createOffer();
      await pc.setLocalDescription(offer);
      if (!offer.sdp) throw new Error("Could not create WebRTC offer.");

      const response = await fetch(`/api/rooms/${encodeURIComponent(room)}/realtime/session`, {
        method: "POST",
        headers: { "Content-Type": "application/sdp" },
        body: offer.sdp
      });
      const answerSdp = await response.text();
      if (!response.ok) throw new Error(answerSdp || `Realtime session failed with ${response.status}.`);

      await pc.setRemoteDescription({ type: "answer", sdp: answerSdp });
    } catch (error) {
      closeRealtimeSession();
      const message = error instanceof Error ? error.message : String(error);
      setRealtimeStatus("error");
      setRealtimeError(message);
      setVoiceText("");
      pushMessage({ role: "assistant", text: `Realtime failed: ${message}` });
    }
  }

  function stopRealtimeSession() {
    closeRealtimeSession();
    setRealtimeStatus("idle");
    setAiBusy(false);
    setMicMuted(false);
    setVoiceText("");
    setAiResponse("");
    setRealtimeError("");
  }

  function toggleVoice() {
    if (listening) {
      stopRealtimeSession();
      return;
    }
    void startRealtimeSession();
  }

  function toggleMicMute() {
    if (!listening || realtimeStatus === "connecting") return;

    const nextMuted = !micMuted;
    mediaStreamRef.current?.getAudioTracks().forEach((track) => {
      track.enabled = !nextMuted;
    });
    setMicMuted(nextMuted);
    setVoiceText("");
    setAiResponse(nextMuted ? "Muted. Computer is still connected." : "Mic live.");
  }

  function closeRealtimeSession() {
    assistantDraftRef.current = "";
    pendingToolCallsRef.current.clear();
    seenAssistantItemsRef.current.clear();
    seenUserItemsRef.current.clear();

    dataChannelRef.current?.close();
    dataChannelRef.current = null;

    mediaStreamRef.current?.getTracks().forEach((track) => track.stop());
    mediaStreamRef.current = null;

    peerRef.current?.close();
    peerRef.current = null;

    if (audioRef.current) audioRef.current.srcObject = null;
  }

  function handleRealtimeEvent(event: RealtimeEvent) {
    if (typeof event.type !== "string") return;

    switch (event.type) {
      case "session.created":
      case "session.updated":
        setRealtimeStatus("listening");
        break;
      case "input_audio_buffer.speech_started":
        setRealtimeStatus("listening");
        setVoiceText("");
        break;
      case "input_audio_buffer.speech_stopped":
        setRealtimeStatus("thinking");
        setAiBusy(true);
        break;
      case "conversation.item.input_audio_transcription.completed":
        handleUserTranscript(event);
        break;
      case "response.created":
        setAiBusy(true);
        setRealtimeStatus("thinking");
        break;
      case "response.output_audio_transcript.delta":
      case "response.output_text.delta":
        appendAssistantDelta(stringFrom(event.delta, ""));
        setRealtimeStatus("speaking");
        break;
      case "response.output_audio_transcript.done":
        finalizeAssistantText(event, stringFrom(event.transcript, ""));
        break;
      case "response.output_text.done":
        finalizeAssistantText(event, stringFrom(event.text, ""));
        break;
      case "response.function_call_arguments.done":
        void handleRealtimeToolCall({
          name: stringFrom(event.name, ""),
          arguments: stringFrom(event.arguments, "{}"),
          call_id: stringFrom(event.call_id, "")
        });
        break;
      case "response.output_item.done":
        handleOutputItemDone(event);
        break;
      case "response.done":
        setAiBusy(false);
        if (peerRef.current) setRealtimeStatus("listening");
        break;
      case "error":
        handleRealtimeError(event);
        break;
    }
  }

  function handleUserTranscript(event: RealtimeEvent) {
    const transcript = stringFrom(event.transcript, "").trim();
    if (!transcript) return;

    const key = stringFrom(event.item_id, transcript);
    if (seenUserItemsRef.current.has(key)) return;
    seenUserItemsRef.current.add(key);

    setVoiceText(transcript);
    pushMessage({ role: "user", text: transcript });
  }

  function appendAssistantDelta(delta: string) {
    if (!delta) return;
    assistantDraftRef.current += delta;
    setAiResponse(assistantDraftRef.current);
  }

  function finalizeAssistantText(event: RealtimeEvent, text: string) {
    const key = stringFrom(event.item_id, "");
    if (key && seenAssistantItemsRef.current.has(key)) return;
    if (key) seenAssistantItemsRef.current.add(key);

    const finalText = (text || assistantDraftRef.current).trim();
    assistantDraftRef.current = "";
    if (!finalText) return;

    setAiResponse(finalText);
    pushMessage({ role: "assistant", text: finalText });
  }

  function handleOutputItemDone(event: RealtimeEvent) {
    const item = objectFrom(event.item);
    if (item.type !== "function_call") return;

    void handleRealtimeToolCall({
      name: stringFrom(item.name, ""),
      arguments: stringFrom(item.arguments, "{}"),
      call_id: stringFrom(item.call_id, "")
    });
  }

  async function handleRealtimeToolCall(call: RealtimeFunctionCall) {
    if (!call.call_id || !call.name || pendingToolCallsRef.current.has(call.call_id)) return;

    pendingToolCallsRef.current.add(call.call_id);
    setAiBusy(true);
    setRealtimeStatus("thinking");

    const args = safeJsonObject(call.arguments);
    const toolCall: ToolCall = { name: call.name, args };

    try {
      const response = await fetch(`/api/rooms/${encodeURIComponent(room)}/realtime/tool`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ name: call.name, arguments: args })
      });
      const raw = await response.text();
      const payload = safeJsonObject(raw);
      const result = response.ok
        ? payload.result ?? payload
        : { ok: false, error: stringFrom(payload.error, raw || `Tool failed with ${response.status}.`) };

      toolCall.result = result;
      const screenshot = screenshotFromResult(result);
      if (screenshot) seenScreenshotsRef.current.add(screenshot);
      pushMessage({ role: "assistant", text: `Tool: ${toolLabel(toolCall)}`, tools: [toolCall], screenshot });
      sendRealtimeToolOutput(call.call_id, result);
    } catch (error) {
      const result = { ok: false, error: error instanceof Error ? error.message : String(error) };
      toolCall.result = result;
      pushMessage({ role: "assistant", text: `Tool failed: ${toolLabel(toolCall)}`, tools: [toolCall] });
      sendRealtimeToolOutput(call.call_id, result);
    } finally {
      pendingToolCallsRef.current.delete(call.call_id);
    }
  }

  function sendRealtimeToolOutput(callId: string, result: unknown) {
    const dataChannel = dataChannelRef.current;
    if (!dataChannel || dataChannel.readyState !== "open") return;

    dataChannel.send(
      JSON.stringify({
        type: "conversation.item.create",
        item: {
          type: "function_call_output",
          call_id: callId,
          output: JSON.stringify(result)
        }
      })
    );
    dataChannel.send(JSON.stringify({ type: "response.create" }));
  }

  function handleRealtimeError(event: RealtimeEvent) {
    const error = objectFrom(event.error);
    const message = stringFrom(error.message, stringFrom(event.message, "Realtime error."));
    setRealtimeError(message);
    setVoiceText("");
    setAiBusy(false);
    setRealtimeStatus(peerRef.current ? "listening" : "error");
    pushMessage({ role: "assistant", text: message });
  }

  return (
    <div className="app">
      <audio ref={audioRef} className="model-audio" autoPlay />
      <button
        className="history-btn"
        type="button"
        onClick={() => setShowHistory(true)}
        title="Conversation history"
        aria-label="Open history"
      >
        <History size={18} />
      </button>

      <div className="mode-tabs" role="tablist" aria-label="Computah views">
        <button
          className={activeTab === "voice" ? "mode-tab active" : "mode-tab"}
          type="button"
          role="tab"
          aria-selected={activeTab === "voice"}
          onClick={() => setActiveTab("voice")}
        >
          <Mic size={15} /> Voice
        </button>
        <button
          className={activeTab === "connections" ? "mode-tab active" : "mode-tab"}
          type="button"
          role="tab"
          aria-selected={activeTab === "connections"}
          onClick={() => setActiveTab("connections")}
        >
          <Monitor size={15} /> Connections
        </button>
      </div>

      {activeTab === "voice" ? (
        <>
          <main className={`stage ${faceState}`}>
            <div className="status-chip">
              <span className={connected ? "dot online" : "dot"} />
              <span>{statusLabel}</span>
            </div>

            <div className="face-area" aria-live="polite">
              <Face state={faceState} />
              <p className="transcript">{latestLine}</p>
            </div>
          </main>

          <div className="mic-dock">
            <div className="voice-controls">
              <button
                className={!listening ? "mic-toggle muted" : micMuted ? "mic-toggle silenced" : "mic-toggle live"}
                type="button"
                onClick={listening ? toggleMicMute : toggleVoice}
                title={!listening ? "Start realtime voice" : micMuted ? "Unmute microphone" : "Mute microphone"}
                aria-label={!listening ? "Start realtime voice" : micMuted ? "Unmute microphone" : "Mute microphone"}
                aria-pressed={listening ? micMuted : false}
                disabled={realtimeStatus === "connecting"}
              >
                {listening && !micMuted ? <Mic size={26} /> : <MicOff size={26} />}
              </button>
              {listening && (
                <button
                  className="end-voice"
                  type="button"
                  onClick={stopRealtimeSession}
                  title="End realtime session"
                  aria-label="End realtime session"
                >
                  <PhoneOff size={20} />
                </button>
              )}
            </div>
            <span className="mic-hint">
              {realtimeStatus === "connecting"
                ? "Connecting..."
                : listening && micMuted
                  ? "Muted - session still live"
                  : listening
                    ? "Mic live - tap to mute"
                    : "Tap to talk"}
            </span>
          </div>
        </>
      ) : (
        <ConnectionsView
          copied={copied}
          devices={devices}
          events={events}
          invite={invite}
          onlineCount={onlineCount}
          room={room}
          serverConnected={connected}
          onCopy={copyValue}
        />
      )}

      <div className={showHistory ? "scrim open" : "scrim"} onClick={() => setShowHistory(false)} />

      <aside className={showHistory ? "history-pane open" : "history-pane"} aria-hidden={!showHistory}>
        <header className="history-head">
          <span className="history-title">
            <History size={16} /> History
          </span>
          <button className="history-close" type="button" onClick={() => setShowHistory(false)} title="Close" aria-label="Close history">
            <X size={18} />
          </button>
        </header>
        <div className="history-feed" ref={feedRef}>
          {messages.length === 0 ? (
            <p className="history-empty">Nothing yet. Say a command to begin.</p>
          ) : (
            messages.map((message) => <ChatBubble key={message.id} message={message} />)
          )}
        </div>
      </aside>
    </div>
  );
}

function ChatBubble({ message }: { message: ChatMessage }) {
  const time = new Date(message.at).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
  return (
    <div className={`bubble-row ${message.role}`}>
      <div className="bubble">
        <p>{message.text}</p>
        {message.tools && message.tools.length > 0 && (
          <div className="tool-chips">
            {message.tools.map((tool, index) => (
              <span className="tool-chip" key={index}>
                {toolLabel(tool)}
              </span>
            ))}
          </div>
        )}
        {message.screenshot && (
          <a className="screenshot-preview" href={message.screenshot} target="_blank" rel="noreferrer">
            <img src={message.screenshot} alt={message.text} />
          </a>
        )}
      </div>
      <time>{time}</time>
    </div>
  );
}

function ConnectionsView({
  copied,
  devices,
  events,
  invite,
  onlineCount,
  room,
  serverConnected,
  onCopy
}: {
  copied: string;
  devices: DeviceInfo[];
  events: FleetEvent[];
  invite: InviteInfo | null;
  onlineCount: number;
  room: string;
  serverConnected: boolean;
  onCopy: (label: string, value: string) => void | Promise<void>;
}) {
  const offlineCount = devices.filter((device) => device.status === "offline").length;
  const busyCount = devices.filter((device) => device.status === "busy").length;

  return (
    <main className="connections-page">
      <section className="connections-head">
        <div>
          <p className="eyebrow">Room</p>
          <h1>{room}</h1>
        </div>
        <div className="connection-stats" aria-label="Connection status">
          <Stat value={serverConnected ? "live" : "off"} label="server" tone={serverConnected ? "online" : "offline"} />
          <Stat value={String(onlineCount)} label="online" tone="online" />
          <Stat value={String(busyCount)} label="busy" tone="busy" />
          <Stat value={String(offlineCount)} label="offline" tone="offline" />
        </div>
      </section>

      <section className="connections-grid">
        <div className="connect-panel">
          <div className="panel-head">
            <span>
              <Download size={16} /> Invite
            </span>
            {invite && (
              <a className="download-link" href={invite.appDownloadUrl}>
                <Download size={15} /> Agent
              </a>
            )}
          </div>

          <CopyRow
            copied={copied}
            label="Join page"
            value={invite?.conductorUrl ?? `${location.origin}/join?room=${encodeURIComponent(room)}`}
            onCopy={onCopy}
          />
          <CopyRow copied={copied} label="Terminal" value={invite?.installCommand ?? "Loading..."} onCopy={onCopy} mono />
        </div>

        <div className="connect-panel fleet-panel">
          <div className="panel-head">
            <span>
              <Monitor size={16} /> Macs
            </span>
            <span className="panel-count">{devices.length}</span>
          </div>

          <div className="device-list">
            {devices.length === 0 ? (
              <p className="empty-state">No agents have joined this room yet.</p>
            ) : (
              devices.map((device) => <DeviceCard key={device.id} device={device} />)
            )}
          </div>
        </div>

        <div className="connect-panel events-panel">
          <div className="panel-head">
            <span>
              <Activity size={16} /> Events
            </span>
            <span className="panel-count">{events.length}</span>
          </div>

          <div className="event-list">
            {events.length === 0 ? (
              <p className="empty-state">No room events yet.</p>
            ) : (
              events.slice(0, 18).map((event) => <EventRow key={event.id} event={event} />)
            )}
          </div>
        </div>
      </section>
    </main>
  );
}

function Stat({ value, label, tone }: { value: string; label: string; tone: "online" | "busy" | "offline" }) {
  return (
    <div className={`stat ${tone}`}>
      <strong>{value}</strong>
      <span>{label}</span>
    </div>
  );
}

function CopyRow({
  copied,
  label,
  mono,
  value,
  onCopy
}: {
  copied: string;
  label: string;
  mono?: boolean;
  value: string;
  onCopy: (label: string, value: string) => void | Promise<void>;
}) {
  return (
    <div className="copy-row">
      <span className="copy-label">{label}</span>
      <code className={mono ? "copy-value mono" : "copy-value"}>{value}</code>
      <button className="copy-btn" type="button" onClick={() => void onCopy(label, value)} title={`Copy ${label}`} aria-label={`Copy ${label}`}>
        {copied === label ? "Copied" : <Copy size={15} />}
      </button>
    </div>
  );
}

function DeviceCard({ device }: { device: DeviceInfo }) {
  return (
    <article className={`device-card ${device.status}`}>
      <div className="device-main">
        <span className={`device-light ${device.status}`} />
        <div>
          <h2>{device.name}</h2>
          <p>{device.hostname || device.platform}</p>
        </div>
      </div>
      <div className="device-meta">
        <span>{device.capabilities.length} tools</span>
        <span>{timeAgo(device.lastSeen)}</span>
      </div>
      {device.lastResult && <p className="last-result">{device.lastResult}</p>}
    </article>
  );
}

function EventRow({ event }: { event: FleetEvent }) {
  return (
    <div className={`event-row ${event.kind}`}>
      <span className="event-dot" />
      <p>{event.message}</p>
      <time>{timeAgo(event.at)}</time>
    </div>
  );
}

function toolLabel(tool: ToolCall): string {
  const args = tool.args ?? {};
  const detail =
    (args.action as string) ||
    (args.app as string) ||
    (args.url as string) ||
    (args.text as string) ||
    (args.tool as string) ||
    "";
  return detail ? `${tool.name} · ${detail}` : tool.name;
}

function Face({ state }: { state: string }) {
  return (
    <div className={`face ${state}`}>
      <div className="ring ring-outer" aria-hidden="true" />
      <div className="ring ring-mid" aria-hidden="true" />
      <div className="pulse" aria-hidden="true" />
      <div className="halo" aria-hidden="true" />
      <div className="face-core">
        <div className="eyes">
          <span />
          <span />
        </div>
        <div className="mouth" aria-hidden="true">
          <i />
          <i />
          <i />
          <i />
          <i />
        </div>
      </div>
    </div>
  );
}

function safeJsonObject(value: string): Record<string, unknown> {
  try {
    return objectFrom(JSON.parse(value));
  } catch {
    return {};
  }
}

function objectFrom(value: unknown): Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value) ? (value as Record<string, unknown>) : {};
}

function stringFrom(value: unknown, fallback: string) {
  return typeof value === "string" && value.trim() ? value.trim() : fallback;
}

function screenshotFromResult(result: unknown) {
  if (typeof result !== "object" || result === null) return undefined;
  const screenshot = (result as { screenshot?: unknown }).screenshot;
  return typeof screenshot === "string" && screenshot.startsWith("data:image/") ? screenshot : undefined;
}

function timeAgo(timestamp: number) {
  const seconds = Math.max(0, Math.round((Date.now() - timestamp) / 1000));
  if (seconds < 10) return "now";
  if (seconds < 60) return `${seconds}s ago`;
  const minutes = Math.round(seconds / 60);
  if (minutes < 60) return `${minutes}m ago`;
  const hours = Math.round(minutes / 60);
  if (hours < 24) return `${hours}h ago`;
  const days = Math.round(hours / 24);
  return `${days}d ago`;
}

createRoot(document.getElementById("root")!).render(<App />);
