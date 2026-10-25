#!/usr/bin/env node
// claude-sdk-worker.mjs — NDJSON sidecar for wackyacp's SDK-direct driver.
//
// Go owns wackypub state (prompt composition, skills, scratchpad storage,
// locks); this worker owns the Claude Agent SDK call. One request frame per
// turn on stdin, event frames back out on stdout. Nothing here talks to disk
// except through the scratchpad tools, which round-trip to Go so there is
// exactly one implementation of entry IDs and size limits.

import { createRequire } from 'node:module';
import { pathToFileURL } from 'node:url';

const sdkDir = process.env.WACKYACP_CLAUDE_SDK_DIR
  || '/home/dranbo/node_modules/.pnpm/node_modules/@anthropic-ai/claude-agent-sdk';
const sdkEntry = sdkDir + '/sdk.mjs';

const out = (obj) => process.stdout.write(JSON.stringify(obj) + '\n');
const fail = (id, code, message) => out({ type: 'error', id, code, message });

let sdk;
try {
  sdk = await import(pathToFileURL(sdkEntry).href);
} catch (err) {
  out({
    type: 'error',
    code: 'worker_failed',
    message: 'cannot import Claude Agent SDK from ' + sdkEntry + ': ' + (err && err.message),
  });
  process.exit(1);
}

const { query, createSdkMcpServer, tool } = sdk;

const pendingTool = new Map();
let toolReqSeq = 0;

// askGo forwards one scratchpad tool call to the Go driver and waits for its
// answer. Scratchpad truth lives in wackypub, so the worker never reads files.
function askGo(name, args) {
  const requestId = ++toolReqSeq;
  return new Promise((resolve, reject) => {
    pendingTool.set(requestId, { resolve, reject });
    out({ type: 'tool_request', requestId, name, args: args || {} });
  });
}

function textResult(text) {
  return { content: [{ type: 'text', text: String(text == null ? '' : text) }] };
}

// scratchpadServer builds the in-process MCP server exposing wackypub
// scratchpads as native tools. If zod cannot be resolved the worker still
// runs; claude simply has no scratchpad tools and gets told so once per turn.
function scratchpadServer() {
  let z;
  try {
    const require = createRequire(pathToFileURL(sdkEntry).href);
    z = require('zod').z || require('zod');
  } catch (err) {
    return { server: null, reason: 'zod not resolvable from ' + sdkDir + ': ' + err.message };
  }

  const server = createSdkMcpServer({
    name: 'wackypub',
    alwaysLoad: true,
    tools: [
      tool('create_scratchpad', 'Store text in your wackypub scratchpad. Returns the entry id.', {
        text: z.string().describe('Entry body'),
        createdBy: z.string().optional().describe('Attribution string'),
      }, async (a) => textResult(await askGo('create_scratchpad', a))),

      tool('get_scratchpad', 'Read a scratchpad entry, optionally a line window.', {
        id: z.string().describe('Entry id returned by create_scratchpad or list_scratchpads'),
        skipLines: z.number().int().optional().describe('Zero-based lines to skip'),
        numLines: z.number().int().optional().describe('Lines to return'),
      }, async (a) => textResult(await askGo('get_scratchpad', a))),

      tool('list_scratchpads', 'List scratchpad entries with sizes and usage.', {},
        async () => textResult(await askGo('list_scratchpads', {}))),

      tool('search_scratchpad', 'Search one entry for a line match without reading it whole.', {
        id: z.string(),
        query: z.string(),
        regex: z.boolean().optional(),
        caseSensitive: z.boolean().optional(),
        maxResults: z.number().int().optional(),
      }, async (a) => textResult(await askGo('search_scratchpad', a))),

      tool('diff_scratchpad', 'Unified diff between two scratchpad entries.', {
        beforeId: z.string(),
        afterId: z.string(),
      }, async (a) => textResult(await askGo('diff_scratchpad', a))),

      tool('delete_scratchpad', 'Remove a scratchpad entry.', { id: z.string() },
        async (a) => textResult(await askGo('delete_scratchpad', a))),
    ],
  });
  return { server, reason: '' };
}

const scratchpad = scratchpadServer();
const mcpServers = scratchpad.server ? { wackypub: scratchpad.server } : {};

let active = null;

function summarize(input) {
  try {
    const s = typeof input === 'string' ? input : JSON.stringify(input);
    return s.length > 200 ? s.slice(0, 200) + '…' : s;
  } catch {
    return '';
  }
}

function stopReasonFrom(msg) {
  if (msg.subtype === 'success') return 'end_turn';
  if (msg.subtype === 'error_max_turns') return 'max_turns';
  return 'refusal';
}

function usageFrom(msg) {
  const u = msg.usage || {};
  const input = u.input_tokens || u.inputTokens || 0;
  const output = u.output_tokens || u.outputTokens || 0;
  return {
    inputTokens: input,
    outputTokens: output,
    totalTokens: u.total_tokens || u.totalTokens || (input + output),
  };
}

async function runPrompt(req) {
  const spec = req.systemPrompt || {};
  const options = {
    cwd: req.cwd,
    additionalDirectories: req.additionalDirectories || [],
    settingSources: req.settingSources || ['user', 'project', 'local'],
    permissionMode: req.permissionMode || 'bypassPermissions',
    allowDangerouslySkipPermissions: true,
    includePartialMessages: true,
    mcpServers,
  };
  if (Array.isArray(req.skills)) options.skills = req.skills;

  // append keeps Claude Code's own instructions; replace sends only wackypub's.
  if (spec.mode === 'replace') {
    options.systemPrompt = { type: 'custom', prompt: spec.content || '', snapshot: !!spec.snapshot };
  } else {
    options.systemPrompt = {
      type: 'preset',
      preset: 'claude_code',
      append: spec.content || '',
      snapshot: !!spec.snapshot,
    };
  }
  if (req.resume) options.resume = req.sessionId;
  else if (req.sessionId) options.sessionId = req.sessionId;

  let sessionId = req.sessionId || '';
  let resultMsg = null;
  try {
    if (!scratchpad.server) {
      out({ type: 'event', id: req.id, kind: 'warning', text: 'scratchpad tools unavailable: ' + scratchpad.reason });
    }
    const q = query({ prompt: req.prompt, options });
    active = q;
    for await (const msg of q) {
      if (msg.session_id) sessionId = msg.session_id;
      if (msg.type === 'stream_event') {
        const delta = msg.event && msg.event.delta;
        if (delta && delta.type === 'text_delta' && delta.text) {
          out({ type: 'event', id: req.id, kind: 'text', text: delta.text });
        }
      } else if (msg.type === 'assistant') {
        for (const block of (msg.message && msg.message.content) || []) {
          if (block.type === 'tool_use') {
            out({
              type: 'event', id: req.id, kind: 'tool_call',
              callId: block.id, toolName: block.name, argsSummary: summarize(block.input),
            });
          }
        }
      } else if (msg.type === 'user') {
        for (const block of (msg.message && msg.message.content) || []) {
          if (block.type === 'tool_result') {
            out({
              type: 'event', id: req.id, kind: 'tool_update',
              callId: block.tool_use_id, status: block.is_error ? 'failed' : 'completed',
            });
          }
        }
      } else if (msg.type === 'result') {
        resultMsg = msg;
      }
    }
    if (!resultMsg) {
      fail(req.id, 'turn_failed', 'SDK stream ended without a result message');
      return;
    }
    out({
      type: 'result', id: req.id, sessionId,
      stopReason: stopReasonFrom(resultMsg), usage: usageFrom(resultMsg),
    });
  } catch (err) {
    const message = String((err && err.message) || err);
    if (req.resume && /resume|session|transcript/i.test(message)) {
      fail(req.id, 'resume_failed', message);
    } else {
      fail(req.id, 'turn_failed', message);
    }
  } finally {
    active = null;
  }
}

out({ type: 'ready', sdk: sdkDir, scratchpadTools: Boolean(scratchpad.server) });

const readline = require_readline();
async function require_readline() {
  const { createInterface } = await import('node:readline');
  const rl = createInterface({ input: process.stdin, crlfDelay: Infinity });
  rl.on('line', (line) => {
    if (!line.trim()) return;
    let frame;
    try {
      frame = JSON.parse(line);
    } catch (err) {
      out({ type: 'error', code: 'bad_frame', message: 'worker cannot parse request: ' + err.message });
      return;
    }
    if (frame.type === 'tool_response') {
      const waiter = pendingTool.get(frame.requestId);
      if (waiter) {
        pendingTool.delete(frame.requestId);
        if (frame.ok) waiter.resolve(frame.text); else waiter.reject(new Error(frame.error || 'tool call failed'));
      }
      return;
    }
    if (frame.op === 'cancel') {
      if (active && typeof active.interrupt === 'function') active.interrupt();
      return;
    }
    if (frame.op === 'prompt') {
      runPrompt(frame);
      return;
    }
    out({ type: 'error', id: frame.id, code: 'bad_op', message: 'unknown worker op ' + frame.op });
  });
  rl.on('close', () => process.exit(0));
}
