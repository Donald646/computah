export const DEFAULT_ROOM = "demo";

export type AgentCapability =
  | "open_url"
  | "open_app"
  | "quit_app"
  | "say"
  | "notify"
  | "screenshot"
  | "click"
  | "type_text"
  | "press_key"
  | "agent_log"
  | "list_apps"
  | "get_app_state"
  | "drag"
  | "set_value"
  | "scroll"
  | "perform_secondary_action"
  | "mcp_tool";

export type FleetAction = AgentCapability;

export type DeviceStatus = "online" | "busy" | "offline";

export type DeviceInfo = {
  id: string;
  name: string;
  room: string;
  role?: string;
  platform: string;
  hostname?: string;
  capabilities: AgentCapability[];
  status: DeviceStatus;
  connectedAt: number;
  lastSeen: number;
  lastScreenshot?: string;
  lastResult?: string;
};

export type CommandTarget =
  | { type: "all" }
  | { type: "device"; deviceId: string };

export type FleetCommand = {
  id: string;
  room: string;
  target: CommandTarget;
  action: FleetAction;
  args: Record<string, unknown>;
  createdAt: number;
  origin: "conductor" | "api";
};

export type CommandResult = {
  commandId: string;
  deviceId: string;
  action: FleetAction;
  ok: boolean;
  output?: string;
  error?: string;
  screenshot?: string;
  completedAt: number;
};

export type FleetEvent = {
  id: string;
  room: string;
  kind: "join" | "leave" | "command" | "result" | "status";
  deviceId?: string;
  message: string;
  details?: Record<string, unknown>;
  at: number;
};

export type ConductorJoinMessage = {
  type: "conductor.join";
  room: string;
};

export type AgentJoinMessage = {
  type: "agent.join";
  room: string;
  device: Omit<DeviceInfo, "room" | "status" | "connectedAt" | "lastSeen">;
};

export type AgentStatusMessage = {
  type: "agent.status";
  deviceId: string;
  status: DeviceStatus;
  lastResult?: string;
};

export type CommandDispatchMessage = {
  type: "command.dispatch";
  command: Omit<FleetCommand, "id" | "createdAt" | "origin">;
};

export type CommandExecuteMessage = {
  type: "command.execute";
  command: FleetCommand;
};

export type CommandResultMessage = {
  type: "command.result";
  result: CommandResult;
};

export type FleetSnapshotMessage = {
  type: "fleet.snapshot";
  room: string;
  devices: DeviceInfo[];
  events: FleetEvent[];
};

export type ServerAckMessage = {
  type: "server.ack";
  message: string;
};

export type ServerErrorMessage = {
  type: "server.error";
  message: string;
};

export type ClientMessage =
  | ConductorJoinMessage
  | AgentJoinMessage
  | AgentStatusMessage
  | CommandDispatchMessage
  | CommandResultMessage;

export type ServerMessage =
  | FleetSnapshotMessage
  | CommandExecuteMessage
  | ServerAckMessage
  | ServerErrorMessage;

export function newId(prefix = "id"): string {
  return `${prefix}_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 9)}`;
}

export function isTargeted(command: FleetCommand, deviceId: string): boolean {
  return command.target.type === "all" || command.target.deviceId === deviceId;
}
