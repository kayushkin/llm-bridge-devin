# llm-bridge-devin

Runs [Devin for Terminal](https://docs.devin.ai/cli) behind llm-bridge-server.
The wrapper speaks the bridge's harness protocol (NDJSON requests on stdin,
canonical `msg.Event` NDJSON on stdout) and drives `devin acp`, Devin's
Agent Client Protocol server, over its stdio.

Built and measured against devin 3000.12.43.

## How it works

- **One `devin acp` per bridge session.** `start` spawns it with
  `devin --config <session config> acp`, sends `initialize`, then
  `session/new` (or `session/load` on resume), switches the session to
  Devin's `bypass` mode and sets the model with `session/set_config_option`.
- **The permission gate is a PreToolUse hook.** bridge-server sends
  `devin_hooks`, a hooks tree in Devin's `hooks.v1` shape whose first entry
  calls `/permission/devin-prehook/{bridge_id}`. The wrapper writes it into the
  session config. Devin runs PreToolUse hooks for every tool call in every
  mode, bypass included, and honors Claude Code's
  `hookSpecificOutput.permissionDecision` answer, so the server's existing
  prehook decides. Bypass only stops Devin asking a second time through
  `session/request_permission`. When it asks anyway, the wrapper sends that
  question to the same gate (`devin_permission_url`) and refuses if the gate
  cannot be reached. A start without `devin_hooks` is refused.
- **Session config.** `--config` replaces Devin's user config file, so the
  wrapper copies `~/.config/devin/config.json`, puts the bridge's hooks ahead
  of the user's, and adds `disabled_tools` to Devin's own top-level
  `disabled_tools`, which removes those tools from the model's list.
- **Turns run one at a time.** A message that arrives during a turn waits.
  `session/prompt` blocks until the turn ends and returns the stop reason and
  token usage, which become the `result` event. SIGINT sends `session/cancel`.
- **Watchdog.** If Devin sends nothing for `DEVIN_TURN_IDLE_TIMEOUT_SEC`
  (default 600; 0 turns it off), the wrapper cancels the turn, kills Devin if
  the cancel is not answered within 30 s, and reports `TURN_IDLE_TIMEOUT`. The
  next message restarts Devin and reloads the session.
- **Models.** The bridge sends a model-store id (`claude-opus-5-5`) and an
  effort. Devin names one variant per effort (`claude-opus-5-5-high`).
  `devin models list --format json` is the catalog. A family with no effort
  takes Devin's first-listed variant. A Devin variant id (`swe-2-flash-medium`)
  passes through unchanged. The session's `model` config option lists what the
  account may use, and anything else fails with `MODEL_UNAVAILABLE`.

## Modes

| Flag | What it does |
|---|---|
| (none) | Session mode, as above |
| `-oneshot` | One stateless call over ACP in a throwaway directory: Ask mode, every tool blocked by a hook, the session deleted afterwards. A schema is refused (Devin cannot force one). |
| `-discover`, `-changed-since <RFC3339>` | Reads Devin's `sessions.db` read-only (`DEVIN_SESSIONS_DIR` or `$XDG_DATA_HOME/devin/cli`) |
| `-import-history <id>` | Loads the session through `session/load` and translates what Devin replays, with a `user_message` and a `result` for each turn |
| `LLMBRIDGE_PTY_MODE=1` | Runs Devin's own terminal UI, with `--resume` when the session has an id |

## What Devin lacks, compared with Claude Code and Codex

| Gap | What the wrapper does |
|---|---|
| No cost in ACP: usage has tokens but no dollars, credits or ACUs | Reports tokens and leaves `cost` empty, rather than estimating from list prices |
| No system prompt in ACP | Sends the system prompt as an embedded resource ahead of a new session's first message. Devin reads the project's `AGENTS.md` itself. Its one always-on rules file, `~/.devin/global_rules.md`, applies to every Devin session on the machine, so it cannot carry one session's prompt. |
| No fork in ACP (`/fork` works only in the TUI) | Refuses with `FORK_UNSUPPORTED` |
| `disabled_tools` read only when a session starts | A mid-session change is refused, not silently ignored |
| No schema-forced output | `-oneshot` refuses a schema |
| One in four early ACP turns stalled after a tool call (measured 2026-11-10, never in print mode) | Watchdog, then reload |
| The free plan offers only Devin's own models (`swe-2-flash-medium`, `swe-1-6-slow`) | `MODEL_UNAVAILABLE` names what the account offers |

## Tests

`go test ./...` runs the unit tests against a fake `devin` (the test binary
re-run with `FAKE_DEVIN_BEHAVIOR`), which covers translation, the gate refusal,
the watchdog's kill, crash recovery, resume without replay, and model
switching. No model is called.

## Deploy

`./deploy.sh` runs the tests, installs `~/bin/llm-bridge-devin`, checks that
`-oneshot` refuses an empty prompt, and publishes the wrapper for runner
machines. The bridge spawns the new binary for the next session; nothing
restarts.
