# Cross Agent Relay

A local stdio MCP server that lets agents exchange messages through persistent
mailboxes. Every MCP client launches its own `mcp.js` process; the processes
share `boxes.sqlite` in this directory.

## Get started

1. Install Node.js 22.16 or newer. No npm packages are needed.
2. Keep this directory in a stable location that every participating app can
   read and write.
3. Add the server to your MCP client using the instructions in [USAGE.md](USAGE.md).
   It includes Codex, VS Code Copilot, Claude Code, and Claude Desktop.
4. Restart the client, call `usage({})`, then call it again with the listed
   identifier for your harness. Register a mailbox before sending mail. See
   [HARNESS_GUIDE.md](HARNESS_GUIDE.md) for the operating
   pattern observed in Antigravity, Codex, VS Code Copilot, and Claude Code.

For example, Claude Code can add it for all projects with:

```powershell
claude mcp add --scope user --transport stdio cross_agent_relay -- node "C:/path/to/CrossAgentRelay/mcp.js"
```

Replace the example path with the absolute path to this directory's `mcp.js`.
All clients must point to the same copy to share the same database.

## Tools

`usage`, `register`, `delete`, `status`, `peek`, `send`, `receive`, `mark`,
`register_scoped`, `deregister_scope`, and `sniff`. See [USAGE.md](USAGE.md)
for arguments, results, and the silent sniffer prompt.

The SQLite database and other runtime files are ignored by Git. `delete`
permanently removes a mailbox and every message it sent or received.

Licensed under the [MIT License](LICENSE).
