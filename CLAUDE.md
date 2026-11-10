# About llm-bridge-devin

## What it is

llm-bridge-devin runs Devin for Terminal behind llm-bridge-server. llm-bridge-server spawns it once per session as `llm-bridge-devin` from PATH; it reads harness-protocol NDJSON on stdin, writes `msg.Event` NDJSON on stdout, and drives one `devin acp` (Agent Client Protocol, JSON-RPC over stdio) per session. The README lists what Devin lacks next to Claude Code and Codex and what the wrapper does about each gap; keep that table true when either side changes.

# How it works

## The permission gate is a hook, and the wrapper refuses to run without it

bridge-server's `injectDevinHookSettings` sends `devin_hooks` (a hooks.v1 tree whose first PreToolUse entry curls `/permission/devin-prehook/{bridge_id}`) and `devin_permission_url`. The wrapper writes the tree into the config it starts devin with (`devin --config <file> acp`) and puts the session in devin's `bypass` mode, so the hook is the only judge. Devin runs PreToolUse hooks in every mode, over ACP too, and obeys Claude Code's `hookSpecificOutput.permissionDecision` answer. `HandleStart` refuses a start with no `devin_hooks.PreToolUse`; never weaken that, because bypass with no hook is an ungated agent. `TestStartRefusesWithoutPermissionGate` pins it.

## What was measured on devin 3000.12.43

- `--config` replaces `~/.config/devin/config.json`; the wrapper copies the user's file in first.
- Top-level `disabled_tools` in the config removes tools from the model's list; it is read only when a session starts.
- `devin models list --format json` is `{"families":[…]}`; each effort is its own variant (`claude-opus-5-5-high`). The session's `model` config option lists what the account may use.
- ACP `usage` has tokens but no cost, credits or ACUs. Do not estimate a cost from list prices.
- ACP takes no system prompt; it rides the first prompt as an embedded resource. `~/.devin/global_rules.md` is always on but machine-wide, so it cannot carry one session's prompt. Devin reads the project's `AGENTS.md` up to the repo root.
- Stdio MCP servers in `session/new` work, inherit devin's environment, and are named `mcp__<server>__<tool>`.
- One ACP turn in four stalled after its first tool call with no frame for minutes; `devin -p` never did. The watchdog (`DEVIN_TURN_IDLE_TIMEOUT_SEC`, default 600) exists for that.
- `session/load` replays history as `session/update` notifications before it answers; `-import-history` relies on that.

Re-measure before relying on any of these after a devin upgrade.

# Working in this repo

## Tests and live checks

`go test ./...` runs against a fake devin: the test binary re-run with `FAKE_DEVIN_BEHAVIOR` (normal, hang, die). It makes no model call. The watchdog test takes about 30 s because it waits out `cancelGracePeriod`. For a live check, run the built binary with a `start` whose `devin_hooks` points at a local script that prints the allow answer, then a `message`, and read the events; a free Devin account can use only Devin's own models (`swe-2-flash-medium`). `go.mod` replaces `github.com/kayushkin/llm-bridge` with `../llm-bridge`, so the repos must sit side by side. Deploy with `./deploy.sh`.
