# Claude Instructions

## How to work in this project

- **Think before coding** — if the request is ambiguous, state assumptions and ask before writing code
- **Simplicity first** — minimum viable code; no speculative abstractions, no unasked features
- **Surgical changes** — edit only what was requested; leave unrelated code alone; match existing style
- **Goal-driven** — confirm success criteria before starting; loop until the verifiable goal is met

---

## What this fork changed from upstream (ahujasid/ableton-mcp)

This is a hardened personal fork. The changes below are **intentional and permanent**.
When reviewing upstream updates, apply logic only — never pull these systems back in.

### REMOVED: Entire telemetry system — do not restore

The following files were deleted and must never be re-added:
- `MCP_Server/telemetry.py`
- `MCP_Server/telemetry_decorator.py`
- `MCP_Server/config.py`

**Why:** The original code collected user prompts (exact text sent to Claude), every MIDI
note created (pitch, start, duration, velocity), instrument/effect URIs loaded, track and
clip names, file paths, platform info, and a persistent machine UUID — and sent all of it
to a third-party Supabase database controlled by the upstream developer. Consent defaulted
to `True` (opt-in), and `config.py` containing the live Supabase credentials was excluded
from the GitHub repo but shipped inside the PyPI wheel, making it invisible to code review.

**What was also stripped from `server.py`:**
- `from .telemetry import record_startup` import
- `from .telemetry_decorator import telemetry_tool, rich_telemetry_tool` import
- `record_startup()` call in `server_lifespan`
- All `@telemetry_tool(...)` and `@rich_telemetry_tool(...)` decorators on every tool function
- All `user_prompt: str = ""` parameters (these existed solely to pass prompts to telemetry)
- All docstring lines referencing `user_prompt` / telemetry
- `supabase>=2.0.0` from `pyproject.toml` dependencies

### CHANGED: Network binding — do not revert

`AbletonMCP_Remote_Script/__init__.py`:
- `HOST` changed from `"0.0.0.0"` to `"127.0.0.1"`

**Why:** The original bound to all network interfaces, exposing the unauthenticated Ableton
control socket to every device on the local network. Localhost-only binding ensures only
processes on the same machine can connect.

### ADDED: Receive buffer size cap — keep this

`AbletonMCP_Remote_Script/__init__.py`:
- `MAX_BUFFER_SIZE = 10 * 1024 * 1024` (10 MB hard cap on the receive buffer)

**Why:** The original accumulated data indefinitely until valid JSON arrived. A malformed or
oversized payload could grow the buffer without limit and exhaust Ableton's memory.

---

## How to handle upstream updates

Do NOT run `git merge upstream/main` or `git pull upstream`. The upstream repo will continue
to ship the telemetry system and the `0.0.0.0` binding. Merging directly would reintroduce
all of the above.

**Correct process:**
1. Check what changed: `gh repo view ahujasid/ableton-mcp` or compare commits on GitHub
2. Read the diff — understand what the upstream change actually does
3. Apply only the useful logic manually to this fork, on our terms
4. The sections above tell you exactly what to leave out every time
