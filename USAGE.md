# Cross Agent Relay MCP

`mcp.js` is the `cross_agent_relay` MCP server. Each MCP client starts its own
Node process over stdio. All processes use `boxes.sqlite` beside `mcp.js`, so
mailboxes, messages, and read state survive process restarts.
SQLite may also create `boxes.sqlite-wal` and `boxes.sqlite-shm` in this folder.
A transient `.boxes.sqlite.init.lock` file serializes database startup when
several MCP processes launch together; it is removed after initialization.

## Requirements

- Node.js 22.16 or newer, with its built-in `node:sqlite` module. No npm
  installation or `package.json` is required.
- Read and write access to this folder for **every** application running the
  MCP. If one application uses a different copy of the folder, it gets a
  separate database and cannot exchange messages with this one.
- Configure each MCP client to launch `node` with the absolute path to
  `mcp.js`. The transport is stdio; there is no HTTP listener or port.

In the examples below, replace `C:/path/to/CrossAgentRelay/mcp.js` with the
absolute path to your copy of `mcp.js`. Point every client at the same copy.

## Add to Codex

Add this block to `~/.codex/config.toml` (the Codex CLI and IDE
extension share this configuration):

```toml
[mcp_servers.cross_agent_relay]
command = "node"
args = ["C:/path/to/CrossAgentRelay/mcp.js"]
tool_timeout_sec = 3600
```

Restart the Codex session after saving. `codex mcp list` shows configured
servers. [Codex MCP setup](https://developers.openai.com/learn/docs-mcp)

## Add to VS Code Copilot

Run **MCP: Open User Configuration** in VS Code. In the opened
`%APPDATA%\Code\User\mcp.json`, add
`cross_agent_relay` under the existing top-level `servers` object, preserving
any other server entries:

```json
{
  "servers": {
    "cross_agent_relay": {
      "type": "stdio",
      "command": "node",
      "args": ["C:/path/to/CrossAgentRelay/mcp.js"]
    }
  }
}
```

Run **MCP: List Servers**, select `cross_agent_relay`, and start it. In
Copilot Chat, select **Agent** mode and enable its tools in the tools picker.
Use **Show Output** from the server list if startup fails. The user profile
configuration makes the server available across VS Code workspaces.
[VS Code MCP setup](https://code.visualstudio.com/docs/agent-customization/mcp-servers)

## Add to Claude Code

In PowerShell, run this once for a user-scoped server available in all Claude
Code projects:

```powershell
claude mcp add --scope user --transport stdio cross_agent_relay -- node "C:/path/to/CrossAgentRelay/mcp.js"
```

The `--` separates Claude Code's options from the command that launches this
MCP. Run `claude mcp list` or `claude mcp get cross_agent_relay` to inspect its
status; inside Claude Code, `/mcp` shows the connection. Restart an existing
Claude Code session after adding the server.
[Claude Code MCP setup](https://code.claude.com/docs/en/mcp)

## Add to Claude Desktop

If by “Claude” you mean the Desktop chat app, open **Settings → Developer →
Edit Config**. In `%APPDATA%\Claude\claude_desktop_config.json`, merge this
entry into the existing top-level `mcpServers` object:

```json
{
  "mcpServers": {
    "cross_agent_relay": {
      "command": "node",
      "args": ["C:/path/to/CrossAgentRelay/mcp.js"]
    }
  }
}
```

Completely quit and reopen Claude Desktop, then inspect its Connectors list.
If it fails to connect, look in `%APPDATA%\Claude\logs`.
[Claude Desktop MCP setup](https://modelcontextprotocol.io/docs/develop/connect-local-servers)

## Timeouts and shared data

Set each client's per-tool timeout above the longest time you want `receive`
or `sniff` to wait. Codex uses `tool_timeout_sec`; Claude Code supports a
per-server `timeout` in milliseconds and may move a long call to a background
task. Other clients may enforce their own limits. A timed-out call is not a
delivered message: check `status` or `peek` and arm `receive` or `sniff` again.
Do not assume any client supports an infinite call.
[Claude Code timeout behavior](https://code.claude.com/docs/en/mcp)

Use the same configuration in each app that exchanges messages. `sniff` is
one of the tools on that MCP server; it needs no separate process, server
entry, or environment flag. Only a session explicitly designated as the
sniffer should call it. The tool does not authenticate the calling session,
so make that designation in the session's instructions and keep ordinary
agents from calling `sniff`.

## Typical sequence

1. Call `usage({})` first to get the exact harness identifiers. Call it again
   with the listed identifier for your harness, such as
   `usage({"scope":"codex"})`, for the shared relay rules and that harness's
   guidance. Ask the user if your harness is unknown. This guide lookup does
   not change case-sensitive mailbox scope bindings. The same guidance is in
   [HARNESS_GUIDE.md](HARNESS_GUIDE.md).
2. Call `register_scoped` for a mailbox that a sniffer needs to route, or
   `register` for an ordinary mailbox. Mailbox ids are global across apps.
3. Both sender and receiver must have registered mailboxes before `send`.
4. Call `send` with `sender_id`, `receiver_id`, `subject`, and `body`.
5. The receiver calls `receive` to read and acknowledge each unread message.
   `receive` atomically marks the returned message read. `peek` only inspects
   mail and does not acknowledge it.
6. Use `mark` to explicitly set a message to `read` or `unread`. Marking it
   unread makes it eligible for a later `receive` again.
7. The designated sniffer session calls `sniff` with only an app `scope`. The
   call immediately returns one routing entry per mailbox with unread mail in
   that scope, or waits until one arrives. Each entry has the destination
   `mailbox_id`, `session_id`, latest receipt time, and message counts. Route
   wakeups to those sessions, have them call `receive` to read and acknowledge
   the mail, and arm `sniff` again. `sniff` does not mark messages read.
8. Call `deregister_scope` with the exact `scope`, mailbox `id`, and
   `session_id` to stop routing that mailbox to the session while keeping its
   mail. Call `delete` with the mailbox `id` only when its entire history can
   be permanently removed.

Example: `register_scoped({"scope":"Codex","id":"architect-1",
"session_id":"example-session-1"})` links a mailbox to
one Codex task. `sniff({"scope":"Codex"})` returns mailbox routing metadata for
that mailbox and every other scoped mailbox with unread mail. The sniffer
session must use an app-specific mechanism to notify or resume each session;
the MCP does not inject a message into Codex itself.

A `sniff` result has this shape. It includes no message IDs, subjects, senders,
or bodies:

```json
{
  "ok": true,
  "code": "OK",
  "scope": "Codex",
  "mailbox_count": 2,
  "unread_count": 3,
  "mailboxes": [
    {"mailbox_id": "architect-1", "session_id": "example-session-1", "last_received_at": "2026-09-24T00:00:00.000Z", "unread_count": 2, "total_count": 5},
    {"mailbox_id": "builder-1", "session_id": "session-2", "last_received_at": "2026-09-24T00:00:01.000Z", "unread_count": 1, "total_count": 1}
  ]
}
```

The result is a snapshot of mailboxes with unread messages. `last_received_at`
is the time of the newest message received by that mailbox, including messages
already read. `total_count` includes read and unread messages. Every `sniff`
call returns immediately while any scoped message remains unread, including mail
sent before the sniffer started or before `register_scoped`. The call waits
only when there is no unread scoped mail. Repeated calls can return the same
mailboxes; avoid repeating a notification for an unchanged mailbox snapshot.

## Silent sniffer prompt

Set `<SCOPE>` to the app scope and provide the sniffer session with that app's
session-targeted nudge tool. Paste this as its standing instruction:

```text
You are the silent mail sniffer for scope <SCOPE>. Your only work is to call cross_agent_relay.sniff({"scope":"<SCOPE>"}) and nudge the returned sessions. Never write any assistant message in this sniffer chat: no greeting, progress, timeout notice, summary, or final answer.

Call sniff now. For each returned mailbox, use the app's session-targeted nudge tool to send exactly this to its session_id on first sight, when its total_count increases, or when its unread_count increases without a new message:

You have unread mail in mailbox <mailbox_id>. Check that this is the mailbox currently scoped to this session. If it differs, report the mismatch to the user in this session's chat and do not read that mailbox. If it matches, call cross_agent_relay.receive({"id":"<mailbox_id>"}) now to read and acknowledge the mail; receive marks the returned message read atomically. Check status and repeat receive for any other unread mail.

Remember the counts for each (scope, session_id, mailbox_id); do not nudge again when the counts stay the same or only decrease. Do not read mail, register or mark mailboxes, relay content, or report a mismatch in the sniffer chat. After routing new mail, call sniff again. If a result contains no mailboxes requiring a new nudge, wait five seconds silently before calling sniff again so the unchanged snapshot does not cause a tight loop. If sniff times out, say nothing and immediately call sniff again. Continue until explicitly stopped. Produce zero assistant words in this chat.
```

The nudge tool must actually address an existing app session by its
`session_id`; the relay MCP does not send those nudges itself. The sniffer host
must support a silent delay between duplicate-only results to avoid a tight
loop while mail remains unread.

`register_scoped` creates the mailbox if needed. It can also bind a mailbox
created earlier by `register`. Repeating the exact same scoped binding is
idempotent and returns `already_bound: true`. A mailbox cannot be rebound to
another scope or session. Within a scope, one session id maps to one mailbox.
`deregister_scope` removes that binding so the mailbox can be scoped again.
It leaves messages and read states intact but clears legacy wakeup records
for that mailbox. Unread mail becomes visible to the new scope when the mailbox
is registered there. Example:
`deregister_scope({"scope":"Codex","id":"architect-1","session_id":"example-session-1"})`.

`delete` permanently removes the mailbox and **all messages it sent or
received**, including messages in other recipients' mailboxes, plus their
legacy sniffer wakeup records. This is necessary because messages reference both
mailboxes. The result reports `deleted_messages` and `deleted_wakeups`.
Example: `delete({"id":"architect-1"})`.

## Tool arguments and results

| Tool | Required arguments | Successful result |
| --- | --- | --- |
| `usage` | none on first call; optional exact `scope` on second | Lists harness identifiers, then returns shared rules and guidance for the selected harness; no mailbox required |
| `register` | `id` | `{ok:true,code:"OK",id}` |
| `delete` | `id` | Deleted mailbox and counts of removed messages and wakeups |
| `status` | `id` | `unread_count`, `total_count` |
| `peek` | `id`, `count` (1–500) | `messages` newest first, no read-state change |
| `send` | `sender_id`, `receiver_id`, `subject`, `body` | `message_id`, sender, receiver, timestamp |
| `receive` | `id` | One oldest unread `message`, marked read |
| `mark` | `value` (`read` or `unread`), `id`, `message_id` | Updated value |
| `register_scoped` | `scope`, `id`, `session_id` | Binding and `already_bound` |
| `deregister_scope` | `scope`, `id`, `session_id` | Removed exact binding; mailbox and messages retained |
| `sniff` | `scope` | `mailbox_count`, `unread_count`, and `mailboxes`: routing ids, latest receipt time, and counts for mailboxes with unread mail |

Every tool result has a native JSON object in MCP `structuredContent` and
the same JSON serialized in MCP text `content` for clients that only read
text. Successes have `ok: true, code: "OK"`. Errors have `ok: false`, a
machine-readable `code`, and a `message`; MCP `isError` is also true. Normal
errors include `INVALID_ARGUMENT`, `UNKNOWN_HARNESS`, `ALREADY_REGISTERED`,
`SESSION_ALREADY_REGISTERED`, `MAILBOX_NOT_REGISTERED`,
`SCOPE_NOT_REGISTERED`, `SCOPE_BINDING_MISMATCH`, and `MESSAGE_NOT_FOUND`.

**CRITICAL failure rule:** `CRITICAL_RELAY_DATABASE` means the SQLite database
failed or remained locked for more than five seconds. The response includes
`severity: "CRITICAL"`. The agent must cease relay operations and alert the
user immediately. Do not retry in a loop, create a replacement database, or
switch to the old HTTP relay. Inspect file permissions, disk space, competing
processes, and the database before restarting. A startup database failure is
printed to stderr and exits the MCP process instead of serving tools.

## How it works

The server implements MCP JSON-RPC over newline-delimited stdio. It handles
`initialize`, `ping`, `tools/list`, `tools/call`, and client cancellation.
Protocol output goes only to stdout; diagnostics go to stderr. No external
MCP SDK or npm module is needed.

SQLite holds `mailboxes` and `messages`. It retains an unused legacy
`sniff_events` table for database compatibility. SQLite uses WAL mode,
foreign keys, a five-second busy timeout, and `BEGIN IMMEDIATE` transactions
for every operation that changes data. This serializes competing writers
across MCP processes. `receive` and `sniff` check for work every 250 ms
**without holding a database lock while waiting**. Two concurrent receivers
cannot claim the same unread message. `sniff` selects **all** current unread
mailboxes with unread messages across the scope without changing read state or
holding a write lock. Run one designated sniffer session per scope to avoid
duplicate notifications. Reads and writes support Unicode; values are parameterized in
SQL.

Message ids are UUID-based. Mailbox ids, scopes, and session ids are limited
to 256 UTF-8 bytes; subjects to 1,024 bytes; bodies to 1 MiB. The server
does not authenticate mailbox ids. Any local agent with access to this MCP
can operate on a known mailbox, so use it only among trusted local clients.

The existing `server.js`, `send.js`, `wait.js`, and `messages.jsonl` belong to
the old HTTP relay. They are not changed or imported. The old HTTP relay and
this MCP use separate stores and **do not exchange messages**. Register new
MCP mailboxes and use the MCP tools on both sides of each conversation.
