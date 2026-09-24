'use strict';

/**
 * Cross Agent Relay: one stdio MCP server per client, one shared SQLite file.
 *
 * Invariants:
 * - Every write is a SQLite transaction; a wait never holds a database lock.
 * - A message is sent only when both mailboxes exist.
 * - A receive claims one unread message and marks it read atomically.
 * - A sniff reports every currently unread message in a scope without changing it.
 * - An unexpected storage error makes this process refuse further tool work.
 *
 * stdout is exclusively for MCP JSON-RPC. Diagnostics go to stderr.
 */

const fs = require('node:fs');
const path = require('node:path');
const { randomUUID } = require('node:crypto');
const { DatabaseSync } = require('node:sqlite');

const CONFIG = {
  name: 'cross_agent_relay',
  version: '1.0.0',
  database: path.join(__dirname, 'boxes.sqlite'),
  startupLock: path.join(__dirname, '.boxes.sqlite.init.lock'),
  lockTimeoutMs: 5000,
  pollMs: 250,
  maxIdBytes: 256,
  maxSubjectBytes: 1024,
  maxBodyBytes: 1024 * 1024,
  maxPeekCount: 500,
  protocols: new Set(['2025-11-25', '2025-06-18', '2025-03-26'])
};

const CRITICAL_MESSAGE =
  'CRITICAL: relay database failed or stayed locked for more than 5 seconds. ' +
  'Stop relay operations and alert the user immediately. Inspect boxes.sqlite ' +
  'and the MCP process before restarting.';

const SCHEMA = `
  CREATE TABLE IF NOT EXISTS mailboxes (
    id TEXT PRIMARY KEY,
    scope TEXT,
    session_id TEXT,
    created_at TEXT NOT NULL,
    CHECK ((scope IS NULL AND session_id IS NULL) OR
           (scope IS NOT NULL AND session_id IS NOT NULL)),
    UNIQUE (scope, session_id)
  );
  CREATE TABLE IF NOT EXISTS messages (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    id TEXT NOT NULL UNIQUE,
    sender_id TEXT NOT NULL REFERENCES mailboxes(id),
    receiver_id TEXT NOT NULL REFERENCES mailboxes(id),
    subject TEXT NOT NULL,
    body TEXT NOT NULL,
    is_read INTEGER NOT NULL DEFAULT 0 CHECK (is_read IN (0, 1)),
    sent_at TEXT NOT NULL,
    read_at TEXT
  );
  CREATE INDEX IF NOT EXISTS messages_receiver_unread
    ON messages(receiver_id, is_read, seq);
  CREATE INDEX IF NOT EXISTS messages_unread_by_seq
    ON messages(is_read, seq);
  -- Legacy wakeup table retained so existing databases remain compatible.
  CREATE TABLE IF NOT EXISTS sniff_events (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    scope TEXT NOT NULL,
    mailbox_id TEXT NOT NULL REFERENCES mailboxes(id),
    session_id TEXT NOT NULL,
    message_id TEXT NOT NULL REFERENCES messages(id),
    sent_at TEXT NOT NULL
  );
  CREATE INDEX IF NOT EXISTS sniff_events_scope
    ON sniff_events(scope, seq);
`;

class ToolError extends Error {
  constructor(code, message) {
    super(message);
    this.code = code;
  }
}

function success(fields) {
  return { ok: true, code: 'OK', ...fields };
}

function failure(code, message, fields = {}) {
  return { ok: false, code, message, ...fields };
}

function criticalFailure() {
  return failure('CRITICAL_RELAY_DATABASE', CRITICAL_MESSAGE, { severity: 'CRITICAL' });
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function textArg(value, name, maxBytes = CONFIG.maxIdBytes) {
  if (typeof value !== 'string' || !value.trim()) {
    throw new ToolError('INVALID_ARGUMENT', `${name} must be a nonempty string`);
  }
  const text = value.trim();
  if (Buffer.byteLength(text, 'utf8') > maxBytes) {
    throw new ToolError('INVALID_ARGUMENT', `${name} exceeds ${maxBytes} UTF-8 bytes`);
  }
  return text;
}

function bodyArg(value) {
  if (typeof value !== 'string' || value.length === 0) {
    throw new ToolError('INVALID_ARGUMENT', 'body must be a nonempty string');
  }
  if (Buffer.byteLength(value, 'utf8') > CONFIG.maxBodyBytes) {
    throw new ToolError('INVALID_ARGUMENT', `body exceeds ${CONFIG.maxBodyBytes} UTF-8 bytes`);
  }
  return value;
}

function peekCountArg(value) {
  if (!Number.isInteger(value) || value < 1 || value > CONFIG.maxPeekCount) {
    throw new ToolError('INVALID_ARGUMENT', `count must be an integer from 1 to ${CONFIG.maxPeekCount}`);
  }
  return value;
}

function markValueArg(value) {
  if (value !== 'read' && value !== 'unread') {
    throw new ToolError('INVALID_ARGUMENT', 'value must be "read" or "unread"');
  }
  return value;
}

function formatMessage(row) {
  return {
    id: row.id,
    sender_id: row.sender_id,
    receiver_id: row.receiver_id,
    subject: row.subject,
    body: row.body,
    read: Boolean(row.is_read),
    sent_at: row.sent_at,
    read_at: row.read_at
  };
}

// SQLite cannot switch to WAL while another connection is opening the same
// fresh database. This short filesystem mutex serializes startup only; SQLite
// handles all message-level concurrency after initialization.
async function acquireStartupLock() {
  const deadline = Date.now() + CONFIG.lockTimeoutMs;
  while (true) {
    try {
      return fs.openSync(CONFIG.startupLock, 'wx');
    } catch (err) {
      if (err.code !== 'EEXIST') throw err;
      if (Date.now() >= deadline) {
        throw new Error('startup mutex remained locked for more than 5 seconds');
      }
      await sleep(50);
    }
  }
}

function releaseStartupLock(fd) {
  let cleanupError;
  try { fs.closeSync(fd); } catch (err) { cleanupError = err; }
  try { fs.unlinkSync(CONFIG.startupLock); } catch (err) { cleanupError ||= err; }
  if (cleanupError) throw cleanupError;
}

class RelayStore {
  constructor(database) {
    this.database = database;
    this.statements = new Map();
  }

  static async open() {
    const lock = await acquireStartupLock();
    let database;
    try {
      database = new DatabaseSync(CONFIG.database, { open: true, timeout: CONFIG.lockTimeoutMs });
      database.exec(`PRAGMA busy_timeout = ${CONFIG.lockTimeoutMs}`);
      if (database.prepare('PRAGMA journal_mode').get().journal_mode !== 'wal') {
        database.exec('PRAGMA journal_mode = WAL');
      }
      database.exec('PRAGMA synchronous = NORMAL');
      database.exec('PRAGMA foreign_keys = ON');
      database.exec(SCHEMA);
      return new RelayStore(database);
    } catch (err) {
      if (database) {
        try { database.close(); } catch (closeError) {
          console.error(`[${CONFIG.name}] database close failed`, closeError);
        }
      }
      throw err;
    } finally {
      releaseStartupLock(lock);
    }
  }

  close() {
    this.database.close();
  }

  statement(sql) {
    let statement = this.statements.get(sql);
    if (!statement) {
      statement = this.database.prepare(sql);
      this.statements.set(sql, statement);
    }
    return statement;
  }

  write(operation) {
    this.database.exec('BEGIN IMMEDIATE');
    try {
      const result = operation();
      this.database.exec('COMMIT');
      return result;
    } catch (err) {
      try {
        this.database.exec('ROLLBACK');
      } catch (rollbackError) {
        throw new AggregateError([err, rollbackError], 'SQLite transaction and rollback both failed');
      }
      throw err;
    }
  }

  mailbox(id) {
    return this.statement('SELECT id, scope, session_id FROM mailboxes WHERE id = ?').get(id);
  }

  requireMailbox(id, role = 'mailbox') {
    const mailbox = this.mailbox(id);
    if (!mailbox) {
      throw new ToolError('MAILBOX_NOT_REGISTERED', `${role} "${id}" is not registered`);
    }
    return mailbox;
  }

  register(id) {
    return this.write(() => {
      if (this.mailbox(id)) {
        throw new ToolError('ALREADY_REGISTERED', `mailbox "${id}" is already registered`);
      }
      this.statement('INSERT INTO mailboxes(id, created_at) VALUES (?, ?)')
        .run(id, new Date().toISOString());
      return { id };
    });
  }

  registerScoped(scope, id, sessionId) {
    return this.write(() => {
      const claimed = this.statement('SELECT id FROM mailboxes WHERE scope = ? AND session_id = ?')
        .get(scope, sessionId);
      if (claimed && claimed.id !== id) {
        throw new ToolError('SESSION_ALREADY_REGISTERED',
          `session "${sessionId}" in scope "${scope}" is linked to mailbox "${claimed.id}"`);
      }
      const current = this.mailbox(id);
      if (current && current.scope === scope && current.session_id === sessionId) {
        return { id, scope, session_id: sessionId, already_bound: true };
      }
      if (current && current.scope !== null) {
        throw new ToolError('ALREADY_REGISTERED', `mailbox "${id}" is already scoped to another session`);
      }
      if (current) {
        this.statement('UPDATE mailboxes SET scope = ?, session_id = ? WHERE id = ?')
          .run(scope, sessionId, id);
      } else {
        this.statement('INSERT INTO mailboxes(id, scope, session_id, created_at) VALUES (?, ?, ?, ?)')
          .run(id, scope, sessionId, new Date().toISOString());
      }
      return { id, scope, session_id: sessionId, already_bound: false };
    });
  }

  deregisterScope(scope, id, sessionId) {
    return this.write(() => {
      const mailbox = this.requireMailbox(id);
      if (mailbox.scope === null) {
        throw new ToolError('SCOPE_NOT_REGISTERED', `mailbox "${id}" has no scope binding`);
      }
      if (mailbox.scope !== scope || mailbox.session_id !== sessionId) {
        throw new ToolError('SCOPE_BINDING_MISMATCH',
          `mailbox "${id}" is not bound to session "${sessionId}" in scope "${scope}"`);
      }
      this.statement('DELETE FROM sniff_events WHERE mailbox_id = ?').run(id);
      this.statement('UPDATE mailboxes SET scope = NULL, session_id = NULL WHERE id = ?').run(id);
      return { id, scope, session_id: sessionId, deregistered: true };
    });
  }

  deleteMailbox(id) {
    return this.write(() => {
      this.requireMailbox(id);
      const deletedWakeups = this.statement(`
        DELETE FROM sniff_events
        WHERE mailbox_id = ? OR message_id IN (
          SELECT id FROM messages WHERE sender_id = ? OR receiver_id = ?
        )
      `).run(id, id, id).changes;
      const deletedMessages = this.statement(
        'DELETE FROM messages WHERE sender_id = ? OR receiver_id = ?'
      ).run(id, id).changes;
      this.statement('DELETE FROM mailboxes WHERE id = ?').run(id);
      return { id, deleted_messages: deletedMessages, deleted_wakeups: deletedWakeups };
    });
  }

  status(id) {
    this.requireMailbox(id);
    const counts = this.statement(`
      SELECT COUNT(*) AS total,
             COALESCE(SUM(CASE WHEN is_read = 0 THEN 1 ELSE 0 END), 0) AS unread
      FROM messages WHERE receiver_id = ?
    `).get(id);
    return { id, unread_count: counts.unread, total_count: counts.total };
  }

  peek(id, count) {
    this.requireMailbox(id);
    const messages = this.statement(`
      SELECT id, sender_id, receiver_id, subject, body, is_read, sent_at, read_at
      FROM messages WHERE receiver_id = ? ORDER BY seq DESC LIMIT ?
    `).all(id, count).map(formatMessage);
    return { id, count: messages.length, messages };
  }

  send(senderId, receiverId, subject, body) {
    return this.write(() => {
      this.requireMailbox(senderId, 'sender');
      this.requireMailbox(receiverId, 'receiver');
      const messageId = `msg_${randomUUID()}`;
      const sentAt = new Date().toISOString();
      this.statement(`
        INSERT INTO messages(id, sender_id, receiver_id, subject, body, sent_at)
        VALUES (?, ?, ?, ?, ?, ?)
      `).run(messageId, senderId, receiverId, subject, body, sentAt);
      return {
        message_id: messageId,
        sender_id: senderId,
        receiver_id: receiverId,
        sent_at: sentAt
      };
    });
  }

  claimUnread(id) {
    // The read avoids taking a write lock on every idle poll. The selection is
    // repeated inside the transaction because another process may win the race.
    const available = this.statement(
      'SELECT 1 FROM messages WHERE receiver_id = ? AND is_read = 0 LIMIT 1'
    ).get(id);
    if (!available) return null;
    return this.write(() => {
      const row = this.statement(`
        SELECT id, sender_id, receiver_id, subject, body, is_read, sent_at, read_at
        FROM messages WHERE receiver_id = ? AND is_read = 0 ORDER BY seq ASC LIMIT 1
      `).get(id);
      if (!row) return null;
      const readAt = new Date().toISOString();
      this.statement('UPDATE messages SET is_read = 1, read_at = ? WHERE id = ?')
        .run(readAt, row.id);
      row.is_read = 1;
      row.read_at = readAt;
      return formatMessage(row);
    });
  }

  mark(id, messageId, value) {
    return this.write(() => {
      this.requireMailbox(id);
      const found = this.statement('SELECT id FROM messages WHERE id = ? AND receiver_id = ?')
        .get(messageId, id);
      if (!found) {
        throw new ToolError('MESSAGE_NOT_FOUND', `message "${messageId}" is not in mailbox "${id}"`);
      }
      const isRead = value === 'read' ? 1 : 0;
      this.statement('UPDATE messages SET is_read = ?, read_at = ? WHERE id = ? AND receiver_id = ?')
        .run(isRead, isRead ? new Date().toISOString() : null, messageId, id);
      return { id, message_id: messageId, value };
    });
  }

  sniffUnread(scope) {
    const messages = this.statement(`
      SELECT m.id AS message_id, m.receiver_id AS mailbox_id,
             b.session_id, m.sender_id, m.subject, m.sent_at
      FROM messages AS m
      JOIN mailboxes AS b ON b.id = m.receiver_id
      WHERE b.scope = ? AND m.is_read = 0
      ORDER BY m.seq ASC
    `).all(scope);
    return messages.length ? { scope, count: messages.length, messages } : null;
  }
}

async function waitForItem(check, call, operation) {
  while (!call.cancelled) {
    const item = check();
    if (item) return item;
    await sleep(CONFIG.pollMs);
  }
  throw new ToolError('CANCELLED', `${operation} was cancelled by the client`);
}

const stringSchema = (description) => ({ type: 'string', description });
const inputSchema = (properties, required) => ({
  type: 'object', properties, required, additionalProperties: false
});
const outputSchema = {
  type: 'object',
  properties: { ok: { type: 'boolean' }, code: { type: 'string' } },
  required: ['ok', 'code'],
  additionalProperties: true
};

// Keep the public schema and implementation together so tools/list cannot
// advertise a tool that tools/call does not implement.
const TOOL_DEFINITIONS = [
  {
    name: 'register',
    description: 'Create one persistent mailbox. An existing id returns ALREADY_REGISTERED. Stop and alert the user on any CRITICAL relay error.',
    inputSchema: inputSchema({ id: stringSchema('Globally unique mailbox id') }, ['id']),
    run: (args, store) => store.register(textArg(args.id, 'id'))
  },
  {
    name: 'delete',
    description: 'Permanently delete a registered mailbox, every message it sent or received, and related legacy wakeup records. This cannot be undone.',
    inputSchema: inputSchema({ id: stringSchema('Mailbox id to delete permanently') }, ['id']),
    run: (args, store) => store.deleteMailbox(textArg(args.id, 'id'))
  },
  {
    name: 'status',
    description: 'Get unread and total message counts for a registered mailbox.',
    inputSchema: inputSchema({ id: stringSchema('Mailbox id') }, ['id']),
    run: (args, store) => store.status(textArg(args.id, 'id'))
  },
  {
    name: 'peek',
    description: 'Return the last count messages, newest first, without changing read status.',
    inputSchema: inputSchema({
      id: stringSchema('Mailbox id'),
      count: { type: 'integer', minimum: 1, maximum: CONFIG.maxPeekCount }
    }, ['id', 'count']),
    run: (args, store) => store.peek(textArg(args.id, 'id'), peekCountArg(args.count))
  },
  {
    name: 'send',
    description: 'Send one subject and body from a registered sender to a registered receiver.',
    inputSchema: inputSchema({
      sender_id: stringSchema('Registered sender mailbox id'),
      receiver_id: stringSchema('Registered receiver mailbox id'),
      subject: stringSchema('Message subject'),
      body: stringSchema('Message body')
    }, ['sender_id', 'receiver_id', 'subject', 'body']),
    run: (args, store) => store.send(
      textArg(args.sender_id, 'sender_id'),
      textArg(args.receiver_id, 'receiver_id'),
      textArg(args.subject, 'subject', CONFIG.maxSubjectBytes),
      bodyArg(args.body)
    )
  },
  {
    name: 'receive',
    description: 'Wait for one unread message, return the oldest one, and atomically mark it read. Re-arm with another call after it returns. Client tool timeouts still apply.',
    inputSchema: inputSchema({ id: stringSchema('Registered receiver mailbox id') }, ['id']),
    run: async (args, store, call) => {
      const id = textArg(args.id, 'id');
      store.requireMailbox(id);
      return { id, message: await waitForItem(() => store.claimUnread(id), call, 'receive') };
    }
  },
  {
    name: 'mark',
    description: 'Mark a message in a registered mailbox read or unread.',
    inputSchema: inputSchema({
      value: { type: 'string', enum: ['read', 'unread'] },
      id: stringSchema('Mailbox id'),
      message_id: stringSchema('Message id')
    }, ['value', 'id', 'message_id']),
    run: (args, store) => store.mark(
      textArg(args.id, 'id'),
      textArg(args.message_id, 'message_id'),
      markValueArg(args.value)
    )
  },
  {
    name: 'register_scoped',
    description: 'Create or bind a mailbox to one app scope and session id so the sniff tool can route its mail.',
    inputSchema: inputSchema({
      scope: stringSchema('App name, for example Codex or Claude Code'),
      id: stringSchema('Globally unique mailbox id'),
      session_id: stringSchema('Unique session id within this scope')
    }, ['scope', 'id', 'session_id']),
    run: (args, store) => store.registerScoped(
      textArg(args.scope, 'scope'),
      textArg(args.id, 'id'),
      textArg(args.session_id, 'session_id')
    )
  },
  {
    name: 'deregister_scope',
    description: 'Remove the exact scope and session binding from a mailbox while retaining the mailbox and its messages.',
    inputSchema: inputSchema({
      scope: stringSchema('Current app scope'),
      id: stringSchema('Mailbox id'),
      session_id: stringSchema('Currently bound session id')
    }, ['scope', 'id', 'session_id']),
    run: (args, store) => store.deregisterScope(
      textArg(args.scope, 'scope'),
      textArg(args.id, 'id'),
      textArg(args.session_id, 'session_id')
    )
  },
  {
    name: 'sniff',
    description: 'Use only from a designated sniffer session. Return all currently unread message metadata in the scope immediately; wait only when none exists. Does not mark mail read.',
    inputSchema: inputSchema({ scope: stringSchema('App scope to monitor') }, ['scope']),
    run: async (args, store, call) => {
      const scope = textArg(args.scope, 'scope');
      return waitForItem(() => store.sniffUnread(scope), call, 'sniff');
    }
  }
];

const TOOL_BY_NAME = new Map(TOOL_DEFINITIONS.map((tool) => [tool.name, tool]));
const PUBLIC_TOOLS = TOOL_DEFINITIONS.map(({ name, description, inputSchema: schema }) => ({
  name, description, inputSchema: schema, outputSchema
}));

function toolResult(payload) {
  return {
    content: [{ type: 'text', text: JSON.stringify(payload) }],
    structuredContent: payload,
    isError: !payload.ok
  };
}

class StdioMcpServer {
  constructor(store) {
    this.store = store;
    this.buffer = '';
    this.calls = new Map();
    this.storageFailed = false;
  }

  start() {
    process.stdin.setEncoding('utf8');
    process.stdin.on('data', (chunk) => this.onData(chunk));
    process.stdin.on('end', () => this.shutdown());
  }

  send(message) {
    process.stdout.write(`${JSON.stringify(message)}\n`);
  }

  respond(id, result) {
    this.send({ jsonrpc: '2.0', id, result });
  }

  protocolError(id, code, message) {
    this.send({ jsonrpc: '2.0', id, error: { code, message } });
  }

  onData(chunk) {
    this.buffer += chunk;
    let end;
    while ((end = this.buffer.indexOf('\n')) !== -1) {
      const line = this.buffer.slice(0, end).trim();
      this.buffer = this.buffer.slice(end + 1);
      if (!line) continue;
      let request;
      try {
        request = JSON.parse(line);
      } catch (_) {
        this.protocolError(null, -32700, 'Parse error');
        continue;
      }
      void this.handle(request).catch((err) => this.failStorage(err));
    }
  }

  async handle(request) {
    if (!request || request.jsonrpc !== '2.0' || typeof request.method !== 'string') {
      if (request && Object.hasOwn(request, 'id')) {
        this.protocolError(request.id, -32600, 'Invalid Request');
      }
      return;
    }
    if (request.method === 'notifications/cancelled') {
      const key = JSON.stringify(request.params && request.params.requestId);
      const call = this.calls.get(key);
      if (call) call.cancelled = true;
      return;
    }
    if (!Object.hasOwn(request, 'id')) return;

    const id = request.id;
    switch (request.method) {
      case 'initialize': {
        const requested = request.params && request.params.protocolVersion;
        this.respond(id, {
          protocolVersion: CONFIG.protocols.has(requested) ? requested : '2025-11-25',
          capabilities: { tools: {} },
          serverInfo: { name: CONFIG.name, version: CONFIG.version },
          instructions: 'Relay data is shared through boxes.sqlite. On CRITICAL_RELAY_DATABASE, cease relay operations and alert the user immediately. receive marks one message read. Only a designated sniffer session should call sniff. sniff returns all unread routing metadata in a scope; re-arm after routing the result.'
        });
        return;
      }
      case 'ping':
        this.respond(id, {});
        return;
      case 'tools/list':
        this.respond(id, { tools: PUBLIC_TOOLS });
        return;
      case 'tools/call':
        await this.callTool(id, request.params);
        return;
      default:
        this.protocolError(id, -32601, 'Method not found');
    }
  }

  async callTool(id, params) {
    if (this.storageFailed) {
      this.respond(id, toolResult(criticalFailure()));
      return;
    }
    const name = params && params.name;
    const args = params && params.arguments;
    const tool = TOOL_BY_NAME.get(name);
    if (!tool) {
      this.respond(id, toolResult(failure('UNKNOWN_TOOL', `unknown tool "${name}"`)));
      return;
    }
    if (!args || typeof args !== 'object' || Array.isArray(args)) {
      this.respond(id, toolResult(failure('INVALID_ARGUMENT', 'arguments must be a JSON object')));
      return;
    }

    const key = JSON.stringify(id);
    const call = { cancelled: false };
    this.calls.set(key, call);
    let payload;
    try {
      payload = this.storageFailed
        ? criticalFailure()
        : success(await tool.run(args, this.store, call));
    } catch (err) {
      payload = err instanceof ToolError && !this.storageFailed
        ? failure(err.code, err.message)
        : this.failStorage(err);
    } finally {
      this.calls.delete(key);
    }
    this.respond(id, toolResult(payload));
  }

  failStorage(err) {
    if (!this.storageFailed) {
      this.storageFailed = true;
      console.error(`[${CONFIG.name}] ${CRITICAL_MESSAGE}`, err);
    }
    return criticalFailure();
  }

  shutdown() {
    for (const call of this.calls.values()) call.cancelled = true;
    try { this.store.close(); } catch (err) {
      console.error(`[${CONFIG.name}] database close failed`, err);
    }
    process.exit();
  }
}

RelayStore.open()
  .then((store) => new StdioMcpServer(store).start())
  .catch((err) => {
    console.error(`[${CONFIG.name}] ${CRITICAL_MESSAGE}`, err);
    process.exitCode = 1;
  });
