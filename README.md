# Computah

Voice-controlled fleet orchestration for MacBooks.

Computah is a hackathon scaffold for turning a room of opted-in Macs into one coordinated surface. The Conductor runs the room server and dashboard; each Mac runs a local agent that can open apps, open URLs, speak, show notifications, capture screenshots, and optionally click/type/press keys after Accessibility permission.

The agent uses `mac-use` for richer native computer-use actions when available. That adds app listing, app state snapshots, accessibility tree inspection, drag, scroll, direct value setting, and secondary accessibility actions. Simple URL/app/speech/notification actions stay lightweight.

## Run the Conductor

```bash
npm install
npm run dev
```

Open `http://localhost:8787`.

## Join a Mac

Fast path: open `/join`, download `Computah Agent.app`, unzip it, and open it.

The app downloads the current agent, installs dependencies in `~/.computah-agent`, and joins the room with input tools enabled.

Terminal fallback:

```bash
npm install
npm run agent -- join --server ws://CONDUCTOR_IP:8787/ws --room demo --name "Alex Mac"
```

For click/type/keyboard control:

```bash
npm run agent -- join --server ws://CONDUCTOR_IP:8787/ws --room demo --name "Alex Mac" --allow-input
```

Then enable the app or terminal in `System Settings > Privacy & Security > Accessibility`.

Screenshots require `System Settings > Privacy & Security > Screen Recording`.

For local rehearsal, run multiple agents with different ids:

```bash
npm run agent -- join --server ws://localhost:8787/ws --room demo --name "Research Mac" --id research
npm run agent -- join --server ws://localhost:8787/ws --room demo --name "Demo Mac" --id demo-screen
```

## Command API

The dashboard uses WebSockets, but you can also dispatch commands with HTTP:

```bash
curl -X POST http://localhost:8787/api/rooms/demo/commands \
  -H "Content-Type: application/json" \
  -d '{"target":{"type":"all"},"action":"notify","args":{"text":"Computah online"}}'
```

## AI Commands

Computah exposes fleet actions as AI tools through `POST /api/rooms/:room/ai`.

Without an API key, the dashboard uses a local fallback parser. For OpenAI tool calling:

```bash
cp .env.example .env
# paste a fresh key into .env
npm run preview
```

Then use the dashboard prompt:

```text
open github on Toby Mac
tell everyone launch mode
take a screenshot of Toby Mac
what is on Arc on Donald Mac
list running apps on Toby Mac
```

The server sends function tools to the OpenAI Responses API, executes returned tool calls against the Computah fleet, then asks the model for a short summary.

## Native App Direction

The smooth onboarding path is a signed `Computah Agent.app`:

1. User opens a join link.
2. Downloads `Computah Agent.app`.
3. Opens it and sees the room already filled in.
4. Grants Screen Recording and Accessibility in-app.
5. The app joins the room automatically.

Implementation-wise this is an Electron or Swift app that wraps the current `agent/index.ts` protocol. The join link can pass `server`, `room`, and `name` through a config file or custom URL scheme such as `computah://join?...`.

## Signing The Agent App

For outside-the-App-Store distribution, Apple expects a Developer ID Application certificate plus notarization. The current download endpoint can sign and notarize the generated `Computah Agent.app` when these environment variables are set:

```bash
COMPUTAH_CODESIGN_IDENTITY="Developer ID Application: Your Name (TEAMID)"
COMPUTAH_NOTARY_PROFILE="computah-notary"
```

Create the certificate in Apple Developer, install it in Keychain, then confirm the exact identity:

```bash
security find-identity -v -p codesigning
```

Store notarization credentials in Keychain once:

```bash
xcrun notarytool store-credentials computah-notary \
  --apple-id "you@example.com" \
  --team-id "TEAMID" \
  --password "app-specific-password"
```

Then restart the server:

```bash
npm run preview
```

When someone downloads `Computah Agent.app`, the server will:

1. Build the room-specific `.app`.
2. Sign it with hardened runtime and timestamp.
3. Submit the zip to Apple notarization.
4. Staple the notarization ticket.
5. Return the signed, stapled zip.

For a production-grade app, prefer a stable signed Swift or Electron app with a room-code or `computah://join?...` flow. The current generated app is great for hackathon onboarding, but notarizing every unique download can add a short wait.

## Why This Exists

Existing macOS MCP projects give one machine "hands." Computah adds the fleet layer: a registry, command router, dashboard, and voice-ready tool surface for coordinating many opted-in machines.
