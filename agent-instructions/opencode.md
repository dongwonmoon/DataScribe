# OpenCode Instructions

This file supplements root `AGENTS.md` for OpenCode sessions. Root instructions
and active owner documents remain authoritative and must not be duplicated or
overridden here.

## Scope

- Default work to bounded implementation, testing, performance, efficiency,
  maintainability, and code-mapping tasks.
- Do not decide product scope, privacy, raw-data sampling, hosted credential
  handling, or architecture policy without explicit authorization.
- Keep changes small and explain material algorithm or architecture choices so
  the user can inspect the reasoning and learn from it.

## OpenCode Boundary

- Use OpenCode-native capabilities only. Do not claim Codex Skills, hooks,
  agents, or tool results.
- Keep provider credentials, personal model selection, and authentication out
  of committed project configuration.

## Session Environment

- The user develops either on the Mac mini directly or from an iPad over
  SSH + tmux + tailscale (they say which). On iPad sessions:
  - `open <file>` / GUI side effects are useless to them — show file
    content in the terminal (sed/grep excerpts, limited output) instead.
  - Prefer non-interactive CLI flags; `--interactive` prompts are painful
    over iPad keyboards.
  - Long commands survive disconnects (tmux + persistent shell); don't
    restart work after an SSH drop.
