'use strict';

/**
 * qq_export
 * LiteLoaderQQNT main-process plugin.
 *
 * No npm dependencies. It starts a localhost web UI and calls QQNT IPC directly.
 * Output is QCE-compatible JSON and chunked JSONL. The history-reading stage stores
 * raw QQNT messages first and does not intentionally resolve remote media URLs.
 */

const fs = require('node:fs');
const fsp = require('node:fs/promises');
const path = require('node:path');
const os = require('node:os');
const http = require('node:http');
const https = require('node:https');
const crypto = require('node:crypto');
const readline = require('node:readline');
const { URL } = require('node:url');
const electron = require('electron');

const PLUGIN_NAME = 'qq_export';
const VERSION = '1.1.1';
const DEFAULT_PORT = Math.max(1, Number(process.env.QQ_EXPORT_PORT || 18765));
const LISTEN_HOST = String(process.env.QQ_EXPORT_HOST || '127.0.0.1');
const DEFAULT_INCREMENTAL_OVERLAP_MS = Math.max(0, Number(process.env.QQ_EXPORT_INCREMENTAL_OVERLAP_MS || 10 * 60 * 1000));
const DEFAULT_INCREMENTAL_OVERLAP_SEQ = Math.max(0, Number(process.env.QQ_EXPORT_INCREMENTAL_OVERLAP_SEQ || 2000));
const MAX_MESSAGES_PER_CHUNK = 50000;
const MAX_BYTES_PER_CHUNK = 50 * 1024 * 1024;
const QCE_APP_NAME = 'qq_export / https://github.com/mkzi-nya/qq_export/';
const QCE_COPYRIGHT = 'https://github.com/mkzi-nya/qq_export/';

const NTClass = {
  NT_API: 'ns-ntApi',
  NODE_STORE_API: 'ns-NodeStoreApi'
};

const newEventName = {
  'ns-ntApi': 'ntApi',
  'ns-FsApi': 'FileApi',
  'ns-OsApi': 'OsApi',
  'ns-HotUpdateApi': 'HotUpdateApi',
  'ns-BusinessApi': 'BusinessApi',
  'ns-NodeStoreApi': 'NodeStoreApi',
  'ns-QQEXApi': 'QQEXApi'
};

const ReceiveCmd = {
  FRIENDS: 'onBuddyListChange',
  GROUPS_STORE: 'onGroupListUpdate'
};

const state = {
  started: false,
  server: null,
  port: 0,
  dataDir: '',
  exportsDir: '',
  historyFile: '',
  tasks: new Map(),
  logs: [],
  hookInstalled: false
};

function log(...args) {
  const line = `[${new Date().toISOString()}] ${args.map(a => {
    if (typeof a === 'string') return a;
    try { return JSON.stringify(a, jsonReplacer); } catch { return String(a); }
  }).join(' ')}`;
  state.logs.push(line);
  if (state.logs.length > 5000) state.logs.splice(0, state.logs.length - 5000);
  console.log(`[${PLUGIN_NAME}]`, ...args);
}

function jsonReplacer(_key, value) {
  if (value instanceof Map) return Object.fromEntries(value);
  if (value instanceof Set) return Array.from(value);
  if (typeof value === 'bigint') return value.toString();
  return value;
}

function writeJson(file, obj) {
  fs.writeFileSync(file, JSON.stringify(obj, jsonReplacer), 'utf8');
}

function appendJsonl(stream, obj) {
  const line = JSON.stringify(obj, jsonReplacer) + '\n';
  stream.write(line);
  return Buffer.byteLength(line, 'utf8');
}

function writeLine(stream, line) {
  return new Promise((resolve, reject) => {
    if (stream.write(line)) return resolve();
    stream.once('drain', resolve);
    stream.once('error', reject);
  });
}

function endStream(stream) {
  return new Promise((resolve, reject) => {
    stream.end(resolve);
    stream.once('error', reject);
  });
}

function ensureDir(dir) {
  fs.mkdirSync(dir, { recursive: true });
}

function safeName(name) {
  return String(name || 'unknown')
    .replace(/[\\/:*?"<>|\x00-\x1F]/g, '_')
    .replace(/\s+/g, ' ')
    .slice(0, 120);
}

function nowMs() { return Date.now(); }
function sleep(ms) { return new Promise(resolve => setTimeout(resolve, ms)); }

function minMaxNumbers(values) {
  let min = 0;
  let max = 0;
  for (const v of values) {
    const n = Number(v) || 0;
    if (!n) continue;
    if (!min || n < min) min = n;
    if (!max || n > max) max = n;
  }
  return { min, max };
}

function msgSeqRange(messages) {
  let min = 0;
  let max = 0;
  for (const m of messages || []) {
    const n = getRawMsgSeq(m);
    if (!n) continue;
    if (!min || n < min) min = n;
    if (!max || n > max) max = n;
  }
  return { min, max };
}

function msgTimeRange(messages) {
  let min = 0;
  let max = 0;
  for (const m of messages || []) {
    const n = getRawMsgTimestamp(m);
    if (!n) continue;
    if (!min || n < min) min = n;
    if (!max || n > max) max = n;
  }
  return { min, max };
}

function getDataRoot() {
  const liteData = globalThis.LiteLoader?.path?.data || globalThis.LiteLoader?.path?.profile;
  if (liteData) return path.join(liteData, 'qq_export');
  return path.join(os.homedir(), '.qq_export');
}

function initDirs() {
  state.dataDir = getDataRoot();
  state.exportsDir = path.join(state.dataDir, 'exports');
  state.historyFile = path.join(state.dataDir, 'history.json');
  ensureDir(state.dataDir);
  ensureDir(state.exportsDir);
  if (!fs.existsSync(state.historyFile)) writeJson(state.historyFile, []);
}

function getBuildVersion() {
  const candidates = [
    globalThis?.versions?.curVersion,
    globalThis?.versions?.version,
    globalThis?.LiteLoader?.versions?.qqnt,
    globalThis?.authData?.version,
    process.env.QQNT_VERSION
  ].filter(Boolean).map(String);
  for (const s of candidates) {
    const m = s.match(/(\d{4,6})/g);
    if (m && m.length) return Number(m[m.length - 1]);
  }
  return 0;
}

const hookApiCallbacks = new Map();
const receiveHooks = new Map();

function getChannel() {
  const events = electron.ipcMain.eventNames();
  for (const ch of ['IPC_UP_2', 'IPC_UP_3', 'RM_IPCFROM_RENDERER2', 'RM_IPCFROM_RENDERER3']) {
    if (events.includes(ch)) return ch;
  }
  return null;
}

function registerReceiveHook(method, hookFunc) {
  const id = crypto.randomUUID();
  const methods = Array.isArray(method) ? method : [method];
  receiveHooks.set(id, { methods, hookFunc });
  return id;
}

function removeReceiveHook(id) {
  receiveHooks.delete(id);
}

function installIpcHook() {
  if (state.hookInstalled) return;
  const originalEmit = electron.ipcMain.emit;
  const senderPatched = Symbol('llqqnt_qce_sender_patched');
  electron.ipcMain.emit = new Proxy(originalEmit, {
    apply(target, thisArg, args) {
      try {
        const eventObj = args[1];
        if (eventObj?.sender && !eventObj.sender[senderPatched]) {
          eventObj.sender[senderPatched] = true;
          const originalSend = eventObj.sender.send;
          eventObj.sender.send = new Proxy(originalSend, {
            apply(sendTarget, sendThis, sendArgs) {
              try {
                const meta = sendArgs[1];
                const payload = sendArgs[2];
                const callbackId = meta?.callbackId;
                if (callbackId && hookApiCallbacks.has(callbackId)) {
                  Promise.resolve(hookApiCallbacks.get(callbackId)(payload)).catch(err => log('callback hook error', err?.stack || err));
                  hookApiCallbacks.delete(callbackId);
                } else if (payload && ['IPC_DOWN_2', 'IPC_DOWN_3', 'RM_IPCFROM_MAIN2', 'RM_IPCFROM_MAIN3'].includes(sendArgs[0])) {
                  const arr = Array.isArray(payload) ? payload : [payload];
                  for (const item of arr) {
                    for (const hook of receiveHooks.values()) {
                      if (hook.methods.includes(item?.cmdName)) {
                        Promise.resolve(hook.hookFunc(item.payload)).catch(err => log('receive hook error', err?.stack || err));
                      }
                    }
                  }
                }
              } catch (err) {
                log('sender.send proxy error', err?.stack || err);
              }
              return sendTarget.apply(sendThis, sendArgs);
            }
          });
        }
      } catch (err) {
        log('ipc emit proxy error', err?.stack || err);
      }
      return target.apply(thisArg, args);
    }
  });
  state.hookInstalled = true;
  log('QQNT IPC hook installed');
}

function invoke(cmdName, payload = [], options = {}) {
  const className = options.className || NTClass.NT_API;
  const channel = options.channel || getChannel();
  const timeout = options.timeout || 10000;
  return new Promise((resolve, reject) => {
    if (!channel) return reject(new Error('no ntqq api channel found'));
    const build = getBuildVersion();
    let eventName = `${className}-${channel[channel.length - 1]}`;
    let sendPayload = [cmdName, ...payload];
    if (build >= 32690) {
      eventName = newEventName[className] || className.split('-')[1] || className;
      sendPayload = options.registerEvent ? cmdName : { cmdName, cmdType: 'invoke', payload };
    }
    if (options.registerEvent) eventName += '-register';
    const callbackId = crypto.randomUUID();
    let receiveHookId;
    const timer = setTimeout(() => {
      if (receiveHookId) removeReceiveHook(receiveHookId);
      hookApiCallbacks.delete(callbackId);
      reject(new Error(`ntqq api timeout ${channel}, ${eventName}, ${cmdName}, ${JSON.stringify(payload, jsonReplacer)}`));
    }, timeout);

    if (!options.cbCmd) {
      hookApiCallbacks.set(callbackId, result => {
        clearTimeout(timer);
        resolve(result);
      });
    } else {
      const afterFirstCmd = options.afterFirstCmd ?? true;
      let firstResult;
      const waitReceive = () => {
        receiveHookId = registerReceiveHook(options.cbCmd, received => {
          if (options.cmdCB && !options.cmdCB(received, firstResult)) return;
          removeReceiveHook(receiveHookId);
          clearTimeout(timer);
          resolve(received);
        });
      };
      if (!afterFirstCmd) waitReceive();
      hookApiCallbacks.set(callbackId, result => {
        firstResult = result;
        const ok = result?.result === 0 || ['undefined', 'number', 'boolean'].includes(typeof result) || result?.data || result?.groupList || result?.buddyCategory;
        if (ok) {
          if (afterFirstCmd) waitReceive();
        } else {
          clearTimeout(timer);
          if (receiveHookId) removeReceiveHook(receiveHookId);
          reject(new Error(`call failed ${cmdName}: ${JSON.stringify(result, jsonReplacer)}`));
        }
      });
    }

    try {
      electron.ipcMain.emit(channel, { sender: { send: () => {} } }, {
        type: 'request',
        callbackId,
        eventName,
        peerId: Number(channel.slice(-1))
      }, sendPayload);
    } catch (err) {
      clearTimeout(timer);
      if (receiveHookId) removeReceiveHook(receiveHookId);
      hookApiCallbacks.delete(callbackId);
      reject(err);
    }
  });
}

async function invokeRetry(cmdName, payload, options, task, what = '历史接口') {
  const retries = Math.max(0, Number(options.retries ?? 5));
  let lastErr;
  let count = Number(options.count || 0);
  for (let i = 0; i <= retries; i++) {
    if (task?.stopRequested) throw new Error('STOP_REQUESTED');
    try {
      return await invoke(cmdName, payload, options);
    } catch (err) {
      lastErr = err;
      const msg = err?.message || String(err);
      if (i >= retries) break;
      if (task) pushTaskLog(task, `${what}失败，重试 ${i + 1}/${retries}：${msg}`);
      await sleep(Math.min(12000, 700 * (i + 1) + Math.floor(Math.random() * 500)));
      if (count && i >= 2) {
        count = Math.max(50, Math.floor(count / 2));
        const arg = payload?.[0];
        if (arg && typeof arg === 'object' && 'cnt' in arg) arg.cnt = count;
      }
    }
  }
  throw lastErr;
}

function pushTaskLog(task, message) {
  const line = `[${new Date().toLocaleTimeString()}] ${message}`;
  task.logs.push(line);
  if (task.logs.length > 5000) task.logs.splice(0, task.logs.length - 5000);
  log(`task ${task.id}`, message);
}

function updateTask(task, patch) {
  if (patch && Object.prototype.hasOwnProperty.call(patch, 'progress')) {
    const p = Number(patch.progress) || 0;
    patch.progress = Math.max(0, Math.min(100, Math.round(p * 10) / 10));
    patch.progressPercent = `${patch.progress.toFixed(patch.progress % 1 ? 1 : 0)}%`;
  }
  Object.assign(task, patch, { updatedAt: new Date().toISOString() });
  if (!task.progressPercent) task.progressPercent = `${Number(task.progress || 0).toFixed(0)}%`;
}

function shortError(err) {
  const msg = err?.message || String(err || 'unknown');
  return msg.length > 420 ? msg.slice(0, 420) + '...' : msg;
}

function fmtDuration(ms) {
  if (!Number.isFinite(ms)) return '?ms';
  if (ms < 1000) return `${ms}ms`;
  return `${(ms / 1000).toFixed(2)}s`;
}

function rangeLabel(win) {
  return win ? `${win.lo}-${win.hi}` : '-';
}

function rawBatchSummary(batch) {
  const arr = Array.isArray(batch) ? batch : [];
  const seqs = arr.map(getRawMsgSeq).filter(Boolean);
  const times = arr.map(getRawMsgTimestamp).filter(Boolean);
  return {
    count: arr.length,
    minSeq: seqs.length ? Math.min(...seqs) : 0,
    maxSeq: seqs.length ? Math.max(...seqs) : 0,
    minTime: times.length ? new Date(Math.min(...times)).toISOString() : '',
    maxTime: times.length ? new Date(Math.max(...times)).toISOString() : ''
  };
}

function getSelfInfo() {
  return {
    uid: String(globalThis?.authData?.uid || ''),
    uin: String(globalThis?.authData?.uin || ''),
    nick: String(globalThis?.authData?.nick || globalThis?.authData?.nickname || '')
  };
}


function mapValue(obj, key) {
  if (!obj || key === undefined || key === null) return undefined;
  if (obj instanceof Map) return obj.get(key) ?? obj.get(String(key));
  return obj[key] ?? obj[String(key)];
}

function objectValues(obj) {
  if (!obj) return [];
  if (obj instanceof Map) return Array.from(obj.values());
  if (Array.isArray(obj)) return obj;
  if (typeof obj === 'object') return Object.values(obj);
  return [];
}

function uniqueSessions(list) {
  const map = new Map();
  for (const s of list) {
    if (!s) continue;
    const key = `${s.type}:${s.uid || s.uin || s.id}`;
    if (!key || key.endsWith(':')) continue;
    const old = map.get(key) || {};
    map.set(key, { ...old, ...s, label: s.label || `${s.name || ''} ${s.uin || s.id || ''} ${s.uid || ''}` });
  }
  return Array.from(map.values());
}

async function invokeFirst(candidates) {
  let lastErr;
  for (const c of candidates) {
    try { return await invoke(c.cmdName, c.payload || [], c.options || {}); }
    catch (err) { lastErr = err; log('candidate ipc failed', c.cmdName, err?.message || err); }
  }
  throw lastErr || new Error('all ipc candidates failed');
}

function parseBuddyListResult(res) {
  const out = [];
  const simpleInfos = res?.userSimpleInfos || res?.userSimpleInfo || res?.buddySimpleInfos || {};

  // Old NodeStore shape: { data: [{ buddyList: [...] }] }
  const oldLists = Array.isArray(res?.data) ? res.data.flatMap(x => Array.isArray(x?.buddyList) ? x.buddyList : []) : [];
  for (const item of oldLists) {
    const uid = String(item?.uid || item?.userUid || item?.buddyUid || item?.peerUid || '');
    const info = mapValue(simpleInfos, uid) || item;
    const uin = String(item?.uin || info?.uin || item?.qid || info?.qid || '');
    const name = String(item?.remark || item?.nick || item?.nickname || info?.remark || info?.nick || info?.nickname || uid || uin || '好友');
    if (uid || uin) out.push({ type: 'private', id: uin || uid, uid, uin, name, label: `${name} ${uin || uid} ${uid}` });
  }

  // Newer NodeStore shape: { buddyCategory: [{ buddyUids: [...] }], userSimpleInfos: { uid: {...} } }
  const cats = Array.isArray(res?.buddyCategory) ? res.buddyCategory : [];
  const uids = [];
  for (const cat of cats) {
    if (Array.isArray(cat?.buddyUids)) uids.push(...cat.buddyUids);
    if (Array.isArray(cat?.buddyList)) uids.push(...cat.buddyList.map(x => x?.uid || x?.userUid || x?.buddyUid || x));
  }
  for (const rawUid of uids) {
    const uid = String(rawUid || '');
    const info = mapValue(simpleInfos, uid) || {};
    const uin = String(info?.uin || info?.qid || '');
    const name = String(info?.remark || info?.nick || info?.nickname || info?.name || uid || uin || '好友');
    if (uid || uin) out.push({ type: 'private', id: uin || uid, uid, uin, name, label: `${name} ${uin || uid} ${uid}` });
  }

  // Fallback: some builds only return userSimpleInfos.
  for (const info of objectValues(simpleInfos)) {
    const uid = String(info?.uid || info?.userUid || info?.buddyUid || '');
    const uin = String(info?.uin || info?.qid || '');
    const name = String(info?.remark || info?.nick || info?.nickname || info?.name || uid || uin || '好友');
    if (uid || uin) out.push({ type: 'private', id: uin || uid, uid, uin, name, label: `${name} ${uin || uid} ${uid}` });
  }
  return uniqueSessions(out);
}

async function getFriends() {
  const candidates = [
    { cmdName: 'getBuddyList', payload: [false], options: { className: NTClass.NODE_STORE_API, cbCmd: ReceiveCmd.FRIENDS, afterFirstCmd: false, timeout: 18000 } },
    { cmdName: 'getBuddyList', payload: [], options: { className: NTClass.NODE_STORE_API, cbCmd: ReceiveCmd.FRIENDS, afterFirstCmd: false, timeout: 18000 } }
  ];
  const res = await invokeFirst(candidates);
  const parsed = parseBuddyListResult(res);
  log(`friends parsed: ${parsed.length}`);
  return parsed;
}

function parseGroupListResult(res) {
  const arr = Array.isArray(res?.groupList) ? res.groupList : Array.isArray(res?.data) ? res.data : Array.isArray(res) ? res : [];
  return uniqueSessions(arr.map(g => {
    const id = String(g?.groupCode || g?.groupUin || g?.groupId || g?.uin || g?.id || '');
    const name = String(g?.groupName || g?.remarkName || g?.name || id || '群聊');
    return { type: 'group', id, uid: id, uin: id, name, memberCount: Number(g?.memberNum || g?.memberCount || 0), label: `${name} ${id}` };
  }).filter(x => x.id));
}

async function getGroups() {
  const res = await invokeFirst([
    { cmdName: 'getGroupList', payload: [], options: { className: NTClass.NODE_STORE_API, cbCmd: ReceiveCmd.GROUPS_STORE, afterFirstCmd: false, timeout: 18000 } },
    { cmdName: 'nodeIKernelGroupService/getGroupList', payload: [], options: { timeout: 18000 } }
  ]);
  const parsed = parseGroupListResult(res);
  log(`groups parsed: ${parsed.length}`);
  return parsed;
}

async function getRecentContacts(limit = 300) {
  try {
    const res = await invoke('nodeIKernelRecentContactService/getRecentContactListSnapShot', [{ count: limit }], { timeout: 12000 });
    const list = res?.info?.changedList || res?.changedList || res?.info?.list || res?.list || [];
    const out = [];
    for (const x of list) {
      const chatType = Number(x?.peer?.chatType || x?.chatType || x?.msg?.peer?.chatType || 0);
      const peerUid = String(x?.peer?.peerUid || x?.peerUid || x?.senderUid || x?.msg?.peer?.peerUid || '');
      const uin = String(x?.senderUin || x?.uin || x?.peerUin || '');
      const name = String(x?.remark || x?.nick || x?.nickName || x?.showName || x?.peerName || x?.contactName || peerUid || uin || '最近会话');
      if (!peerUid && !uin) continue;
      if (chatType === 2 || /^[1-9]\d{4,}$/.test(peerUid) && !peerUid.startsWith('u_')) out.push({ type: 'group', id: peerUid || uin, uid: peerUid || uin, uin: peerUid || uin, name, label: `${name} ${peerUid || uin}` });
      else out.push({ type: 'private', id: uin || peerUid, uid: peerUid, uin, name, label: `${name} ${uin || peerUid} ${peerUid}` });
    }
    log(`recent contacts parsed: ${out.length}`);
    return uniqueSessions(out);
  } catch (err) {
    log('recent contacts failed', err?.message || err);
    return [];
  }
}

async function getUidByUin(uin, groupCode = '') {
  const u = String(uin || '').trim();
  if (!u || u.startsWith('u_')) return u;
  const tries = [
    async () => {
      const r = await invoke('nodeIKernelGroupService/getUidByUins', [{ uinList: [u] }], { timeout: 12000 });
      return mapValue(r?.uids, u) || mapValue(r?.uidInfo, u) || '';
    },
    async () => {
      const r = await invoke('nodeIKernelProfileService/getUidByUin', [{ callFrom: 'FriendsServiceImpl', uin: [u] }], { timeout: 12000 });
      return mapValue(r, u) || mapValue(r?.uids, u) || mapValue(r?.uidInfo, u) || '';
    },
    async () => {
      const r = await invoke('nodeIKernelUixConvertService/getUid', [{ uins: [u] }], { timeout: 12000 });
      return mapValue(r?.uidInfo, u) || mapValue(r?.uids, u) || '';
    },
    async () => {
      const r = await invoke('nodeIKernelProfileService/getUserDetailInfoByUin', [{ uin: u }], { timeout: 15000 });
      return r?.info?.uid || r?.detail?.uid || r?.uid || '';
    }
  ];
  for (const fn of tries) {
    try {
      const uid = String(await fn() || '');
      if (uid && !uid.includes('*')) return uid;
    } catch (err) { log('getUidByUin candidate failed', err?.message || err); }
  }
  try {
    const sessions = await getRecentContacts(500);
    const found = sessions.find(s => s.type === 'private' && (s.uin === u || s.id === u));
    if (found?.uid) return found.uid;
  } catch {}
  if (groupCode) log('getUidByUin failed in group context', groupCode, u);
  return '';
}

async function getSessions(query = '') {
  const [friends, groups, recent] = await Promise.allSettled([getFriends(), getGroups(), getRecentContacts(300)]);
  let arr = [];
  if (friends.status === 'fulfilled') arr = arr.concat(friends.value);
  else log('getFriends failed', friends.reason?.message || friends.reason);
  if (groups.status === 'fulfilled') arr = arr.concat(groups.value);
  else log('getGroups failed', groups.reason?.message || groups.reason);
  if (recent.status === 'fulfilled') arr = arr.concat(recent.value);
  arr = uniqueSessions(arr);
  const qRaw = String(query || '').trim();
  const q = qRaw.toLowerCase();
  if (q) arr = arr.filter(s => [s.id, s.uid, s.uin, s.name, s.label].some(v => String(v || '').toLowerCase().includes(q)));
  if (qRaw && /^\d{5,}$/.test(qRaw)) {
    if (!arr.some(s => s.type === 'private' && (s.uin === qRaw || s.id === qRaw))) {
      arr.unshift({ type: 'private', id: qRaw, uid: '', uin: qRaw, name: `手动私聊 ${qRaw}`, label: `手动私聊 ${qRaw}` });
    }
    if (!arr.some(s => s.type === 'group' && s.id === qRaw)) {
      arr.unshift({ type: 'group', id: qRaw, uid: qRaw, uin: qRaw, name: `手动群聊 ${qRaw}`, label: `手动群聊 ${qRaw}` });
    }
  }
  return arr.slice(0, 1000);
}

async function resolvePeer(options) {
  const chatTypeRaw = options.chatType ?? options.sessionType;
  const chatType = chatTypeRaw === 'group' || Number(chatTypeRaw) === 2 ? 2 : 1;
  let peerUid = String(options.peerUid || options.uid || options.id || options.sessionId || options.uin || '').trim();
  let displayName = String(options.name || options.sessionName || '').trim();
  let uin = String(options.uin || options.id || options.sessionId || '').trim();
  if (!peerUid) throw new Error('缺少 QQ/群号/UID');

  if (chatType === 1) {
    if (!peerUid.startsWith('u_')) {
      const sessions = await getSessions(peerUid);
      const found = sessions.find(s => s.type === 'private' && (s.uin === peerUid || s.id === peerUid || s.name === peerUid || s.uid === peerUid));
      if (found) {
        peerUid = found.uid || found.id;
        uin = found.uin || found.id || uin;
        displayName = displayName || found.name;
      }
    }
    if (!peerUid.startsWith('u_')) {
      const uid = await getUidByUin(peerUid);
      if (uid) {
        uin = uin || peerUid;
        peerUid = uid;
      }
    }
    if (!peerUid.startsWith('u_')) {
      throw new Error(`无法把 QQ 号 ${peerUid} 转换为 UID。请先在 QQNT 打开该聊天，或在会话列表中选择该好友后重试。`);
    }
  }

  if (chatType === 2) {
    const sessions = await getSessions(peerUid);
    const found = sessions.find(s => s.type === 'group' && (s.id === peerUid || s.uin === peerUid || s.name === peerUid));
    if (found) {
      peerUid = found.id || found.uid || peerUid;
      uin = found.uin || found.id || uin || peerUid;
      displayName = displayName || found.name;
    }
  }
  return {
    chatType,
    peerUid,
    guildId: '',
    uin,
    name: displayName || uin || peerUid,
    typeName: chatType === 2 ? 'group' : 'private'
  };
}

function makePeer(peer) {
  return { chatType: peer.chatType, peerUid: peer.peerUid, guildId: peer.guildId || '' };
}

function getRawMsgSeq(msg) {
  const v = msg?.msgSeq ?? msg?.msg_seq ?? msg?.seq ?? msg?.sequence ?? 0;
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
}

function getRawMsgId(msg) {
  return String(msg?.msgId || msg?.msg_id || msg?.id || `${getRawMsgSeq(msg)}-${msg?.msgTime || ''}`);
}

function timestampFromMsgId(msgId) {
  try {
    const s = String(msgId || '');
    if (!/^\d+$/.test(s)) return 0;
    const sec = Number(BigInt(s) >> 32n);
    if (sec > 946684800 && sec < 4102444800) return sec * 1000;
  } catch {}
  return 0;
}

function getRawMsgTimestamp(msg) {
  let v = msg?.msgTime ?? msg?.time ?? msg?.timestamp ?? msg?.msg_time ?? 0;
  if (typeof v === 'string' && /^\d+$/.test(v)) v = Number(v);
  let n = Number(v);
  if (!Number.isFinite(n) || n <= 0) n = timestampFromMsgId(getRawMsgId(msg));
  if (!Number.isFinite(n) || n <= 0) n = nowMs();
  if (n < 100000000000) n *= 1000;
  return Math.floor(n);
}

function isRawSystem(msg) {
  if (msg?.msgType === 5 || msg?.msgType === 10 || msg?.subMsgType === 12) return true;
  const elems = Array.isArray(msg?.elements) ? msg.elements : [];
  return elems.some(e => Number(e?.elementType || e?.type) === 8 || e?.grayTipElement || e?.tipElement || e?.grayTip);
}

function isRawRecalled(msg) {
  if (msg?.recalled || msg?.isRevoke || msg?.isRecall || msg?.msgStatus === 4) return true;
  const elems = Array.isArray(msg?.elements) ? msg.elements : [];
  return elems.some(e => {
    const sub = Number(e?.grayTipElement?.subElementType || e?.grayTipElement?.subType || e?.subElementType || e?.subType || 0);
    const type = Number(e?.elementType || e?.type || 0);
    const text = JSON.stringify(e, jsonReplacer).toLowerCase();
    return type === 8 && (sub === 1 || text.includes('revoke') || text.includes('撤回'));
  });
}

function flattenStrings(obj, limit = 8) {
  const out = [];
  const seen = new Set();
  function walk(x) {
    if (out.length >= limit || x == null) return;
    if (typeof x === 'string') {
      const s = x.trim();
      if (s && s.length < 500 && !seen.has(s)) { seen.add(s); out.push(s); }
      return;
    }
    if (typeof x !== 'object') return;
    if (Array.isArray(x)) { for (const y of x) walk(y); return; }
    for (const [k, v] of Object.entries(x)) {
      if (/^(md5|uuid|fileUuid|fileMd5|fileId|elemId|msgId)$/i.test(k)) continue;
      walk(v);
    }
  }
  walk(obj);
  return out;
}

function findFirstUrl(obj) {
  let found = '';
  function walk(x) {
    if (found || x == null) return;
    if (typeof x === 'string') {
      if (/^https?:\/\//i.test(x)) found = x;
      return;
    }
    if (typeof x !== 'object') return;
    if (Array.isArray(x)) { for (const y of x) walk(y); return; }
    for (const v of Object.values(x)) walk(v);
  }
  walk(obj);
  return found;
}

function findFirstLocalPath(obj) {
  let found = '';
  function walk(x) {
    if (found || x == null) return;
    if (typeof x === 'string') {
      if ((x.startsWith('/') || /^[A-Za-z]:[\\/]/.test(x)) && fs.existsSync(x)) found = x;
      return;
    }
    if (typeof x !== 'object') return;
    if (Array.isArray(x)) { for (const y of x) walk(y); return; }
    for (const v of Object.values(x)) walk(v);
  }
  walk(obj);
  return found;
}

function pick(obj, keys, fallback = '') {
  for (const k of keys) {
    const v = obj?.[k];
    if (v !== undefined && v !== null && String(v) !== '') return v;
  }
  return fallback;
}


function escapeHtmlFast(text) {
  const s = String(text || '');
  return s.replace(/[&<>"']/g, ch => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[ch]));
}

function formatQceTime(ms) {
  const d = new Date(Number(ms) || Date.now());
  return d.toISOString();
}

function stripUndefined(obj) {
  if (Array.isArray(obj)) return obj.map(stripUndefined).filter(v => v !== undefined);
  if (!obj || typeof obj !== 'object') return obj;
  const out = {};
  for (const [k, v] of Object.entries(obj)) {
    if (v === undefined || v === null || v === '') continue;
    const vv = stripUndefined(v);
    if (vv === undefined) continue;
    if (Array.isArray(vv)) { out[k] = vv; continue; }
    if (typeof vv === 'object' && Object.keys(vv).length === 0) continue;
    out[k] = vv;
  }
  return out;
}

function normalizeUrlForDedupe(url) {
  try {
    const u = new URL(String(url));
    return `${u.protocol}//${u.host}${u.pathname}`;
  } catch { return String(url || ''); }
}

function sha1File(file) {
  try {
    const h = crypto.createHash('sha1');
    h.update(fs.readFileSync(file));
    return h.digest('hex');
  } catch { return ''; }
}

function shortStableId(value, len = 12) {
  const s = String(value || '').trim();
  if (!s) return '';
  if (/^[a-f0-9]{8,}$/i.test(s)) return s.toLowerCase().slice(0, len);
  return crypto.createHash('sha1').update(s).digest('hex').slice(0, len);
}

function isEmojiResource(resource) {
  if (!resource) return false;
  return resource.type === 'emoji' || resource.kind === 'emoji' || resource.category === 'emoji' || resource.originalType === 'market_face' || !!resource.emojiId || !!resource.emojiPackageId || !!resource.tabName;
}

function emojiStableNameHint(resource) {
  const primary = resource?.md5 || resource?.fileMd5 || resource?.md5HexStr || resource?.sha1 || resource?.hash || resource?.key || resource?.emojiId || resource?.url || resource?.localPath || resource?.path || resource?.filename || resource?.fileName;
  return shortStableId(primary, 12);
}

function appendNameSuffix(filename, suffix) {
  const cleanSuffix = String(suffix || '').replace(/[^a-zA-Z0-9_-]/g, '').slice(0, 24);
  if (!cleanSuffix) return filename;
  const ext = path.extname(filename);
  const base = ext ? filename.slice(0, -ext.length) : filename;
  if (base.endsWith(`_${cleanSuffix}`)) return filename;
  return `${base}_${cleanSuffix}${ext}`;
}

function qceMessageType(raw, elements) {
  const msgType = Number(raw?.msgType ?? raw?.messageType ?? raw?.msgTypeEnum ?? 0);
  switch (msgType) {
    case 1:
    case 2:
      return 'text';
    case 3:
      return 'file';
    case 4:
    case 7:
      return 'video';
    case 5:
      return 'system';
    case 6:
      return 'audio';
    case 8:
      return 'forward';
    case 9:
      return 'reply';
    case 11:
      return 'json';
    default:
      if (elements.some(e => e.type === 'file')) return 'file';
      if (elements.some(e => e.type === 'video')) return 'video';
      if (elements.some(e => e.type === 'audio')) return 'audio';
      if (elements.some(e => e.type === 'reply')) return 'reply';
      if (elements.some(e => e.type === 'forward')) return 'forward';
      if (elements.some(e => e.type === 'json')) return 'json';
      if (elements.some(e => e.type === 'system')) return 'system';
      return msgType ? `type_${msgType}` : 'text';
  }
}

function qceResourceFromElement(el) {
  if (!['image', 'file', 'video', 'audio', 'market_face'].includes(el.type)) return null;
  const d = el.data || {};
  const resourceType = el.type === 'market_face' ? 'image' : el.type;
  return stripUndefined({
    type: resourceType,
    kind: el.type === 'market_face' ? 'emoji' : undefined,
    category: el.type === 'market_face' ? 'emoji' : undefined,
    originalType: el.type === 'market_face' ? 'market_face' : undefined,
    filename: d.filename || d.fileName || d.name || '未知',
    size: Number(d.size || d.fileSize || 0) || 0,
    url: d.url,
    localPath: d.localPath || d.path,
    width: d.width ? Number(d.width) || 0 : undefined,
    height: d.height ? Number(d.height) || 0 : undefined,
    duration: d.duration ? Number(d.duration) || 0 : undefined,
    md5: d.md5 || d.fileMd5 || d.md5HexStr || d.key || d.emojiId,
    key: d.key || d.emojiId,
    emojiId: d.emojiId,
    emojiPackageId: d.emojiPackageId,
    tabName: d.tabName
  });
}

function qceTextHtmlFromElement(el) {
  const d = el.data || {};
  switch (el.type) {
    case 'text': {
      const t = d.text || '';
      return { text: t, html: escapeHtmlFast(t) };
    }
    case 'face': {
      const t = `[表情${d.id ?? ''}]`;
      return { text: t, html: t };
    }
    case 'market_face': {
      const t = `[${d.name || '表情'}]`;
      return { text: t, html: t };
    }
    case 'image': {
      const filename = d.filename || '图片';
      const t = `[图片:${filename}]`;
      return { text: t, html: `<img alt="${escapeHtmlFast(filename)}" class="image">` };
    }
    case 'file': {
      const filename = d.filename || '文件';
      const t = `[文件:${filename}]`;
      return { text: t, html: `<span class="file">${escapeHtmlFast(t)}</span>` };
    }
    case 'video': {
      const filename = d.filename || '视频';
      const t = `[视频:${filename}]`;
      return { text: t, html: `<span class="video">${escapeHtmlFast(t)}</span>` };
    }
    case 'audio': {
      const t = `[语音:${d.duration || 0}秒]`;
      return { text: t, html: `<span class="audio">${escapeHtmlFast(t)}</span>` };
    }
    case 'reply': {
      const t = '[回复消息]';
      return { text: t, html: `<div class="reply">${t}</div>` };
    }
    case 'forward': {
      const t = '[转发消息]';
      return { text: t, html: `<div class="forward">${t}</div>` };
    }
    case 'location': {
      const t = '[位置消息]';
      return { text: t, html: `<div class="location">${t}</div>` };
    }
    case 'json': {
      const t = '[JSON消息]';
      return { text: t, html: `<div class="json">${t}</div>` };
    }
    case 'system': {
      const t = d.text || d.summary || '系统消息';
      return { text: t, html: `<div class="system">${escapeHtmlFast(t)}</div>` };
    }
    default: {
      const t = d.text || d.summary || d.content || '';
      return { text: t, html: t ? `<span>${escapeHtmlFast(t)}</span>` : '' };
    }
  }
}

function normalizeElement(elem, idx) {
  const elementType = Number(elem?.elementType || elem?.type || 0);
  if (elem?.textElement || elementType === 1) {
    const t = elem.textElement || elem;
    return { type: 'text', data: { text: String(pick(t, ['content', 'text', 'str', 'atText'], '')) } };
  }
  if (elem?.picElement || elementType === 2) {
    const p = elem.picElement || elem;
    const url = findFirstUrl(p);
    const localPath = findFirstLocalPath(p);
    const filename = String(pick(p, ['fileName', 'fileNameMd5', 'md5', 'uuid'], `image_${idx}.jpg`));
    return { type: 'image', data: { filename, size: Number(pick(p, ['fileSize', 'size'], 0)) || 0, width: Number(pick(p, ['picWidth', 'width'], 0)) || 0, height: Number(pick(p, ['picHeight', 'height'], 0)) || 0, md5: pick(p, ['md5HexStr', 'md5'], ''), ...(url ? { url } : {}), ...(localPath ? { localPath } : {}) } };
  }
  if (elem?.fileElement || elementType === 3) {
    const f = elem.fileElement || elem;
    const url = findFirstUrl(f);
    const localPath = findFirstLocalPath(f);
    const filename = String(pick(f, ['fileName', 'name'], `file_${idx}`));
    return { type: 'file', data: { filename, size: Number(pick(f, ['fileSize', 'size'], 0)) || 0, md5: pick(f, ['fileMd5', 'md5'], ''), ...(url ? { url } : {}), ...(localPath ? { localPath } : {}) } };
  }
  if (elem?.pttElement || elementType === 4) {
    const a = elem.pttElement || elem;
    const url = findFirstUrl(a);
    const localPath = findFirstLocalPath(a);
    const filename = String(pick(a, ['fileName', 'fileNameMd5', 'md5'], `audio_${idx}.silk`));
    return { type: 'audio', data: { filename, size: Number(pick(a, ['fileSize', 'size'], 0)) || 0, duration: Number(pick(a, ['duration', 'voiceDuration'], 0)) || 0, ...(url ? { url } : {}), ...(localPath ? { localPath } : {}) } };
  }
  if (elem?.videoElement || elementType === 5) {
    const v = elem.videoElement || elem;
    const url = findFirstUrl(v);
    const localPath = findFirstLocalPath(v);
    const filename = String(pick(v, ['fileName', 'fileNameMd5', 'md5'], `video_${idx}.mp4`));
    return { type: 'video', data: { filename, size: Number(pick(v, ['fileSize', 'size'], 0)) || 0, duration: Number(pick(v, ['duration'], 0)) || 0, thumbSize: Number(pick(v, ['thumbSize'], 0)) || 0, ...(url ? { url } : {}), ...(localPath ? { localPath } : {}) } };
  }
  if (elem?.faceElement || elementType === 6) {
    const f = elem.faceElement || elem;
    const id = pick(f, ['faceIndex', 'id'], '');
    return { type: 'face', data: { id, name: String(pick(f, ['faceText', 'text', 'summary'], id ? `表情${id}` : '表情')) } };
  }
  if (elem?.replyElement || elementType === 7) {
    const r = elem.replyElement || elem;
    return { type: 'reply', data: { messageId: pick(r, ['msgId', 'messageId'], ''), referencedMessageId: pick(r, ['sourceMsgId', 'replyMsgId'], ''), senderUin: pick(r, ['senderUin'], ''), senderName: pick(r, ['senderName'], ''), content: flattenStrings(r, 5).join(' '), timestamp: Number(pick(r, ['msgTime', 'timestamp'], 0)) || 0 } };
  }
  if (elem?.grayTipElement || elementType === 8) {
    const g = elem.grayTipElement || elem;
    const parts = flattenStrings(g, 12);
    const text = parts.find(s => /撤回|入群|退群|戳|禁言|加入|退出|revoke/i.test(s)) || parts.join(' ') || '系统消息';
    return { type: 'system', data: { text, summary: text, elementType } };
  }
  if (elem?.arkElement || elementType === 10) {
    const a = elem.arkElement || elem;
    const content = String(pick(a, ['bytesData', 'json', 'content'], '')) || flattenStrings(a, 5).join(' ');
    return { type: 'json', data: { content, title: 'JSON消息', summary: content || 'JSON消息' } };
  }
  if (elem?.marketFaceElement || elementType === 11) {
    const m = elem.marketFaceElement || elem;
    const url = findFirstUrl(m);
    const localPath = findFirstLocalPath(m);
    const name = String(pick(m, ['faceName', 'name', 'emojiName', 'fileName'], `sticker_${idx}`));
    return { type: 'market_face', data: { name, tabName: pick(m, ['tabName'], ''), key: pick(m, ['key'], ''), emojiId: pick(m, ['emojiId'], ''), emojiPackageId: pick(m, ['emojiPackageId'], ''), filename: name, kind: 'emoji', category: 'emoji', ...(url ? { url } : {}), ...(localPath ? { localPath } : {}) } };
  }
  if (elem?.shareLocationElement || elementType === 12) {
    return { type: 'location', data: { title: '位置消息', summary: '分享了位置' } };
  }
  if (elem?.structLongMsgElement || elementType === 13) {
    return { type: 'json', data: { content: '[长消息]', title: '长消息', summary: '长消息' } };
  }
  if (elem?.multiForwardMsgElement || elementType === 16) {
    const mf = elem.multiForwardMsgElement || elem;
    return { type: 'forward', data: { title: '转发消息', resId: pick(mf, ['resId'], ''), summary: pick(mf, ['xmlContent'], '') || '转发消息' } };
  }
  const text = flattenStrings(elem, 4).join(' ');
  return { type: 'system', data: { elementType, summary: text || `未知消息:${elementType || 'unknown'}`, text: text || `未知消息:${elementType || 'unknown'}` } };
}

function normalizeMessage(raw, peer) {
  const timestamp = getRawMsgTimestamp(raw) || Date.now();
  const seq = String(getRawMsgSeq(raw) || raw?.msgSeq || '');
  const msgId = String(getRawMsgId(raw) || `${timestamp}-${seq}`);
  const recalled = isRawRecalled(raw);
  const system = isRawSystem(raw);
  const self = getSelfInfo();
  const senderUidRaw = raw?.senderUid || raw?.senderUidStr || raw?.fromUid || raw?.uid || '';
  let senderUid = String(senderUidRaw || (system ? 'system' : peer.peerUid) || raw?.peerUid || raw?.peerUin || 'unknown');
  if (!senderUid || senderUid === '0') senderUid = system ? 'system' : String(peer.peerUid || peer.uin || 'unknown');
  let senderUin = String(raw?.senderUin || raw?.fromUin || raw?.uin || (senderUid === self.uid ? self.uin : '') || '');
  if (!senderUin && /^\d+$/.test(senderUid)) senderUin = senderUid;
  if (!senderUin && system) senderUin = '0';
  const senderName = String((raw?.sendMemberName && String(raw.sendMemberName).trim()) || (raw?.sendRemarkName && String(raw.sendRemarkName).trim()) || (raw?.sendNickName && String(raw.sendNickName).trim()) || raw?.senderNick || raw?.senderName || (system ? '系统' : (senderUin || senderUid || '未知')));
  const elementsRaw = Array.isArray(raw?.elements) ? raw.elements : Array.isArray(raw?.msgElements) ? raw.msgElements : [];
  const parsedElements = elementsRaw.map((e, i) => normalizeElement(e, i)).filter(Boolean);
  const resources = [];
  const parts = [];
  const mentions = [];
  for (const el of parsedElements) {
    const rh = qceTextHtmlFromElement(el);
    if (rh.text) parts.push(rh.text);
    const r = qceResourceFromElement(el);
    if (r) resources.push(r);
    if (el.type === 'at') {
      const d = el.data || {};
      mentions.push({ uid: String(d.uid || d.uin || ''), ...(d.uin ? { uin: String(d.uin) } : {}), name: String(d.name || d.uid || ''), type: d.type === 'all' ? 'all' : 'user' });
    }
  }
  let text = parts.join('').trim();
  if (recalled && !text) text = '[已撤回]';
  if (!text) text = system ? '[系统消息]' : '[空消息]';
  const elements = [];
  if (text) elements.push({ type: 'text', data: { text } });
  for (const el of parsedElements) {
    if (el.type === 'text') continue;
    elements.push(el);
  }
  const sender = stripUndefined({
    uid: senderUid || 'unknown',
    uin: senderUin || undefined,
    name: senderName || senderUid || '未知',
    nickname: raw?.sendNickName ? String(raw.sendNickName) : undefined,
    groupCard: raw?.sendMemberName ? String(raw.sendMemberName) : undefined,
    remark: raw?.sendRemarkName ? String(raw.sendRemarkName) : undefined
  });
  // QCE CleanMessage: keep content.html/elements/resources/mentions even when empty.
  return {
    id: msgId,
    seq,
    timestamp,
    time: formatQceTime(timestamp),
    sender: {
      uid: sender.uid || 'unknown',
      ...(sender.uin ? { uin: sender.uin } : {}),
      name: sender.name || '未知用户',
      ...(sender.nickname ? { nickname: sender.nickname } : {}),
      ...(sender.groupCard ? { groupCard: sender.groupCard } : {}),
      ...(sender.remark ? { remark: sender.remark } : {})
    },
    type: qceMessageType(raw, parsedElements),
    content: { text, html: '', elements, resources, mentions },
    recalled,
    system
  };
}

function filterByOptions(raw, opts) {
  const ts = getRawMsgTimestamp(raw);
  const seq = getRawMsgSeq(raw);
  if (opts.incremental && (opts.fromMs || opts.fromSeq)) {
    const inTimeOverlap = !!opts.fromMs && ts >= opts.fromMs;
    const inSeqOverlap = !!opts.fromSeq && seq >= opts.fromSeq;
    if (!inTimeOverlap && !inSeqOverlap) return false;
  } else if (opts.fromMs && ts < opts.fromMs) return false;
  if (opts.toMs && ts > opts.toMs) return false;
  const system = isRawSystem(raw);
  const recalled = isRawRecalled(raw);
  if (!opts.includeSystem && system) return false;
  // includeRecalled=false means do not include locally retained deleted message bodies.
  // Revoke gray-tip/system messages are still kept when includeSystem=true.
  if (!opts.includeRecalled && recalled && !system) return false;
  return true;
}

async function fetchLatestPage(peer, count, task, includeRecalled, retries = 5, ctx = {}) {
  const started = Date.now();
  pushTaskLog(task, `[IPC] latest begin cnt=${count} includeRecalled=${!!includeRecalled}${ctx.tag ? ` tag=${ctx.tag}` : ''}`);
  const res = await invokeRetry('nodeIKernelMsgService/getAioFirstViewLatestMsgs', [{ peer: makePeer(peer), cnt: count }], {
    timeout: 15000,
    retries,
    count
  }, task, '读取最新页');
  const arr = Array.isArray(res?.msgList) ? res.msgList : [];
  const sum = rawBatchSummary(arr);
  pushTaskLog(task, `[IPC] latest ok cost=${fmtDuration(Date.now() - started)} count=${sum.count} seq=${sum.minSeq}-${sum.maxSeq} time=${sum.minTime || '-'}..${sum.maxTime || '-'}${ctx.tag ? ` tag=${ctx.tag}` : ''}`);
  return arr;
}

async function fetchBySeq(peer, seq, count, task, includeRecalled, retries = 5, ctx = {}) {
  const arg = { peer: makePeer(peer), msgSeq: String(Math.max(0, Number(seq) || 0)), cnt: count, queryOrder: true, incloudeDeleteMsg: !!includeRecalled };
  const started = Date.now();
  const label = ctx.window ? ` window=${rangeLabel(ctx.window)}` : '';
  const wid = ctx.workerId ? ` worker=${ctx.workerId}` : '';
  const page = ctx.page ? ` page=${ctx.page}` : '';
  pushTaskLog(task, `[IPC] history begin seq=${arg.msgSeq} cnt=${arg.cnt}${wid}${label}${page}`);
  try {
    const res = await invokeRetry('nodeIKernelMsgService/getMsgsBySeqAndCount', [arg], {
      timeout: 16000,
      retries,
      count
    }, task, '历史接口');
    const arr = Array.isArray(res?.msgList) ? res.msgList : [];
    const sum = rawBatchSummary(arr);
    pushTaskLog(task, `[IPC] history ok cost=${fmtDuration(Date.now() - started)} requestedSeq=${arg.msgSeq} finalCnt=${arg.cnt} returned=${sum.count} seq=${sum.minSeq}-${sum.maxSeq} time=${sum.minTime || '-'}..${sum.maxTime || '-'}${wid}${label}${page}`);
    return arr;
  } catch (err) {
    pushTaskLog(task, `[IPC] history failed cost=${fmtDuration(Date.now() - started)} seq=${arg.msgSeq} cnt=${arg.cnt}${wid}${label}${page} error=${shortError(err)}`);
    throw err;
  }
}

function crossedLowerBoundary(messages, opts) {
  if (!Array.isArray(messages) || !messages.length) return false;
  const tr = msgTimeRange(messages);
  const sr = msgSeqRange(messages);
  const timeCrossed = !opts.fromMs || (!!tr.min && tr.min < opts.fromMs);
  const seqCrossed = !opts.fromSeq || (!!sr.min && sr.min < opts.fromSeq);
  return timeCrossed && seqCrossed;
}

async function fetchSequential(peer, opts, task) {
  const out = [];
  const started = Date.now();
  pushTaskLog(task, `顺序历史拉取开始：batch=${Number(opts.batchCount || 200)} retry=${Number(opts.historyRetryCount || 5)} timeRange=${opts.fromMs ? new Date(opts.fromMs).toISOString() : '-'}..${opts.toMs ? new Date(opts.toMs).toISOString() : '-'}`);
  let latest = await fetchLatestPage(peer, Number(opts.batchCount || 200), task, opts.includeRecalled, Number(opts.historyRetryCount || 5), { tag: 'sequential-initial' });
  if (!latest.length) {
    pushTaskLog(task, '顺序历史拉取结束：最新页为空');
    return out;
  }
  let cursor = msgSeqRange(latest).min;
  let accepted = 0;
  for (const msg of latest) { if (filterByOptions(msg, opts)) { out.push(msg); accepted++; } }
  pushTaskLog(task, `顺序页 #1：raw=${latest.length} accepted=${accepted} cursor=${cursor} total=${out.length}`);
  let pages = 1;
  while (!task.stopRequested) {
    if ((opts.fromMs || opts.fromSeq) && crossedLowerBoundary(latest, opts)) {
      const minTime = msgTimeRange(latest).min;
      const minSeq = msgSeqRange(latest).min;
      pushTaskLog(task, `顺序停止：已越过增量重叠边界 minPageTime=${minTime ? new Date(minTime).toISOString() : '-'} minPageSeq=${minSeq || '-'}`);
      break;
    }
    if (!cursor || cursor <= 1) { pushTaskLog(task, `顺序停止：游标到边界 cursor=${cursor}`); break; }
    const requestSeq = cursor - 1;
    const batch = await fetchBySeq(peer, requestSeq, Number(opts.batchCount || 200), task, opts.includeRecalled, Number(opts.historyRetryCount || 5), { page: pages + 1 });
    if (!batch.length) { pushTaskLog(task, `顺序停止：接口返回空页 requestSeq=${requestSeq}`); break; }
    latest = batch;
    cursor = msgSeqRange(batch).min;
    accepted = 0;
    for (const msg of batch) { if (filterByOptions(msg, opts)) { out.push(msg); accepted++; } }
    pages++;
    updateTask(task, { messageCount: out.length, progress: Math.min(70, 5 + pages) });
    pushTaskLog(task, `顺序页 #${pages}：raw=${batch.length} accepted=${accepted} cursor=${cursor} total=${out.length}`);
  }
  pushTaskLog(task, `顺序历史拉取完成：pages=${pages} accepted=${out.length} cost=${fmtDuration(Date.now() - started)} stopped=${!!task.stopRequested}`);
  return out;
}

function makeWindows(maxSeq, windowSize, minSeq = 1) {
  const wins = [];
  let hi = maxSeq;
  const floor = Math.max(1, Number(minSeq || 1));
  while (hi >= floor) {
    const lo = Math.max(floor, hi - windowSize + 1);
    wins.push({ lo, hi });
    hi = lo - 1;
  }
  return wins;
}

async function fetchWindow(peer, win, opts, task, workerId = 0) {
  const out = [];
  const started = Date.now();
  let cursor = win.hi;
  let emptyCount = 0;
  let pages = 0;
  let rawTotal = 0;
  let inWindowTotal = 0;
  let reason = 'finished';
  pushTaskLog(task, `[W${workerId}] 窗口开始 ${rangeLabel(win)} batch=${Number(opts.batchCount || 150)} retry=${Number(opts.historyRetryCount || 5)}`);
  while (!task.stopRequested && cursor >= win.lo) {
    pages++;
    const requestSeq = cursor;
    const batch = await fetchBySeq(peer, requestSeq, Number(opts.batchCount || 150), task, opts.includeRecalled, Number(opts.historyRetryCount || 5), { window: win, workerId, page: pages });
    rawTotal += batch.length;
    const inWin = batch.filter(m => {
      const seq = getRawMsgSeq(m);
      return seq >= win.lo && seq <= win.hi;
    });
    inWindowTotal += inWin.length;
    let accepted = 0;
    for (const msg of inWin) { if (filterByOptions(msg, opts)) { out.push(msg); accepted++; } }
    const seqs = batch.map(getRawMsgSeq).filter(Boolean);
    let nextCursor = cursor;
    if (!seqs.length) {
      emptyCount++;
      nextCursor = cursor - Number(opts.batchCount || 150);
      pushTaskLog(task, `[W${workerId}] 窗口页 #${pages} ${rangeLabel(win)} raw=0 inWindow=0 accepted=0 empty=${emptyCount} cursor ${cursor}->${nextCursor}`);
      if (emptyCount >= 2) { reason = '连续空页'; break; }
      cursor = nextCursor;
    } else {
      const { min: minSeq, max: maxSeq } = minMaxNumbers(seqs);
      if (minSeq <= win.lo) {
        reason = `达到窗口下界 minSeq=${minSeq}`;
        pushTaskLog(task, `[W${workerId}] 窗口页 #${pages} ${rangeLabel(win)} raw=${batch.length} seq=${minSeq}-${maxSeq} inWindow=${inWin.length} accepted=${accepted} cursor=${cursor} stop=${reason} total=${out.length}`);
        break;
      }
      const next = minSeq - 1;
      nextCursor = next >= cursor ? cursor - Number(opts.batchCount || 150) : next;
      pushTaskLog(task, `[W${workerId}] 窗口页 #${pages} ${rangeLabel(win)} raw=${batch.length} seq=${minSeq}-${maxSeq} inWindow=${inWin.length} accepted=${accepted} cursor ${cursor}->${nextCursor} total=${out.length}`);
      cursor = nextCursor;
    }
    if ((opts.fromMs || opts.fromSeq) && inWin.length && crossedLowerBoundary(inWin, opts)) {
      const minTs = msgTimeRange(inWin).min;
      const minSeq = msgSeqRange(inWin).min;
      reason = `达到增量重叠边界 time=${minTs ? new Date(minTs).toISOString() : '-'} seq=${minSeq || '-'}`;
      break;
    }
  }
  if (task.stopRequested) reason = '提前停止';
  pushTaskLog(task, `[W${workerId}] 窗口结束 ${rangeLabel(win)} reason=${reason} pages=${pages} raw=${rawTotal} inWindow=${inWindowTotal} accepted=${out.length} cost=${fmtDuration(Date.now() - started)}`);
  return out;
}

async function fetchParallel(peer, opts, task) {
  const started = Date.now();
  const first = await fetchLatestPage(peer, Math.min(Number(opts.batchCount || 150), 200), task, opts.includeRecalled, Number(opts.historyRetryCount || 5), { tag: 'parallel-probe' });
  const maxSeq = msgSeqRange(first).max;
  if (!maxSeq) {
    pushTaskLog(task, '并行历史拉取停止：无法从最新页确定 seq 上界');
    return [];
  }
  const firstAccepted = first.filter(m => filterByOptions(m, opts));
  const windowSize = Number(opts.seqWindow || 0) || Math.max(25000, Math.ceil(maxSeq / 24));
  const windows = makeWindows(maxSeq, windowSize, opts.fromSeq || 1);
  const workers = Math.max(1, Math.min(4, Number(opts.historyWorkers || 2)));
  const results = [...firstAccepted];
  const failed = [];
  let index = 0;
  let done = 0;
  pushTaskLog(task, `并行历史拉取：seq 上界=${maxSeq}，窗口=${windowSize}，窗口数=${windows.length}，workers=${workers}，probeRaw=${first.length}，probeAccepted=${firstAccepted.length}`);
  pushTaskLog(task, `并行窗口列表：${windows.map(w => rangeLabel(w)).join(', ')}`);

  async function worker(workerId) {
    pushTaskLog(task, `[W${workerId}] worker started`);
    while (!task.stopRequested) {
      const win = windows[index++];
      if (!win) { pushTaskLog(task, `[W${workerId}] worker finished: no more windows`); return; }
      try {
        pushTaskLog(task, `[W${workerId}] 分配窗口 ${rangeLabel(win)}，队列剩余 ${Math.max(0, windows.length - index)}`);
        const arr = await fetchWindow(peer, win, opts, task, workerId);
        for (const item of arr) results.push(item);
        done++;
        updateTask(task, { messageCount: results.length, progress: Math.min(70, Math.round(done / windows.length * 70)) });
        pushTaskLog(task, `并行窗口完成 ${done}/${windows.length}：${rangeLabel(win)} accepted=${arr.length} 累计原始=${results.length} 失败窗口=${failed.length}`);
      } catch (err) {
        if (err?.message === 'STOP_REQUESTED') { pushTaskLog(task, `[W${workerId}] 收到停止请求`); return; }
        failed.push(win);
        done++;
        updateTask(task, { messageCount: results.length, progress: Math.min(70, Math.round(done / windows.length * 70)) });
        pushTaskLog(task, `并行窗口失败，稍后单线程补洞：${rangeLabel(win)}：${shortError(err)}`);
      }
    }
    pushTaskLog(task, `[W${workerId}] worker stopped by request`);
  }
  await Promise.all(Array.from({ length: workers }, (_, i) => worker(i + 1)));

  if (!task.stopRequested && failed.length) {
    pushTaskLog(task, `开始单线程补洞：${failed.length} 个窗口，batch 将降到 <=100`);
    const originalBatch = opts.batchCount;
    opts.batchCount = Math.min(100, Number(opts.batchCount || 150));
    let patched = 0;
    for (const win of failed) {
      if (task.stopRequested) break;
      try {
        const arr = await fetchWindow(peer, win, opts, task, 'patch');
        for (const item of arr) results.push(item);
        patched++;
        pushTaskLog(task, `补洞完成 ${patched}/${failed.length} ${rangeLabel(win)}：accepted=${arr.length}`);
      } catch (err) {
        pushTaskLog(task, `补洞仍失败 ${rangeLabel(win)}：${shortError(err)}`);
        task.incompleteWindows.push(win);
      }
    }
    opts.batchCount = originalBatch;
  }
  pushTaskLog(task, `并行历史拉取完成：rawIncludingProbe=${results.length} failed=${failed.length} incomplete=${task.incompleteWindows.length} cost=${fmtDuration(Date.now() - started)} stopped=${!!task.stopRequested}`);
  return results;
}

function dedupeAndSort(rawMessages) {
  const map = new Map();
  for (const m of rawMessages) {
    const key = getRawMsgId(m) || String(getRawMsgSeq(m));
    if (!map.has(key)) map.set(key, m);
  }
  return Array.from(map.values()).sort((a, b) => {
    const ta = getRawMsgTimestamp(a), tb = getRawMsgTimestamp(b);
    if (ta !== tb) return ta - tb;
    return getRawMsgSeq(a) - getRawMsgSeq(b);
  });
}


function normalizedSeqRange(messages) {
  return minMaxNumbers((messages || []).map(m => Number(m?.seq || 0)));
}

function canonicalJson(value) {
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(',')}]`;
  if (!value || typeof value !== 'object') return JSON.stringify(value);
  return `{${Object.keys(value).sort().map(k => `${JSON.stringify(k)}:${canonicalJson(value[k])}`).join(',')}}`;
}

function normalizedMessageIdentityKeys(msg) {
  const keys = [];
  const id = String(msg?.id || '').trim();
  const seq = String(msg?.seq || '').trim();
  const timestamp = Number(msg?.timestamp || 0) || 0;
  const sender = msg?.sender || {};
  const senderId = String(sender.uin || sender.uid || sender.id || 'unknown');
  const type = String(msg?.type || 'unknown');
  const content = msg?.content || {};
  const contentShape = {
    text: String(content.text || ''),
    elements: Array.isArray(content.elements) ? content.elements : [],
    resources: Array.isArray(content.resources) ? content.resources.map(r => ({
      type: r?.type || '', md5: r?.md5 || r?.fileMd5 || '', key: r?.key || '',
      name: r?.filename || r?.name || '', size: r?.size || r?.fileSize || 0
    })) : []
  };
  const contentHash = crypto.createHash('sha1').update(canonicalJson(contentShape)).digest('hex');
  if (id) keys.push(`id:${id}`);
  if (seq) keys.push(`seq:${seq}|sender:${senderId}|type:${type}|hash:${contentHash}`);
  else keys.push(`fp:${timestamp}|sender:${senderId}|type:${type}|hash:${contentHash}`);
  return keys;
}

function messageRichnessScore(msg) {
  const sender = msg?.sender || {};
  const content = msg?.content || {};
  return (
    String(content.text || '').length +
    (Array.isArray(content.elements) ? content.elements.length * 20 : 0) +
    (Array.isArray(content.resources) ? content.resources.length * 50 : 0) +
    Object.values(sender).filter(Boolean).length * 5
  );
}

function mergeNormalizedMessages(...messageLists) {
  const merged = [];
  const keyToIndex = new Map();
  for (const list of messageLists) {
    for (const msg of list || []) {
      if (!msg || typeof msg !== 'object') continue;
      const keys = normalizedMessageIdentityKeys(msg);
      let index = -1;
      for (const key of keys) {
        if (keyToIndex.has(key)) { index = keyToIndex.get(key); break; }
      }
      if (index < 0) {
        index = merged.length;
        merged.push(msg);
      } else if (messageRichnessScore(msg) > messageRichnessScore(merged[index])) {
        merged[index] = msg;
      }
      for (const key of normalizedMessageIdentityKeys(merged[index])) keyToIndex.set(key, index);
      for (const key of keys) keyToIndex.set(key, index);
    }
  }
  return merged.sort((a, b) => {
    const ta = Number(a?.timestamp || 0), tb = Number(b?.timestamp || 0);
    if (ta !== tb) return ta - tb;
    const sa = Number(a?.seq || 0), sb = Number(b?.seq || 0);
    if (sa !== sb) return sa - sb;
    return String(a?.id || '').localeCompare(String(b?.id || ''));
  });
}

async function readJsonlFile(file) {
  const out = [];
  const input = fs.createReadStream(file, { encoding: 'utf8' });
  const rl = readline.createInterface({ input, crlfDelay: Infinity });
  for await (const line of rl) {
    const text = line.trim();
    if (!text) continue;
    try { out.push(JSON.parse(text)); }
    catch (err) { throw new Error(`JSONL 解析失败 ${file}: ${err.message}`); }
  }
  return out;
}

async function loadMessagesFromOutputDir(outDir) {
  if (!outDir) return [];
  const manifestFile = path.join(outDir, 'manifest.json');
  if (fs.existsSync(manifestFile)) {
    const manifest = JSON.parse(await fsp.readFile(manifestFile, 'utf8'));
    const chunks = manifest?.chunked?.chunks || [];
    const all = [];
    for (const chunk of chunks) {
      const rel = chunk.relativePath || path.join(manifest.chunked?.chunksDir || 'chunks', chunk.fileName || '');
      const file = path.resolve(outDir, rel);
      if (!file.startsWith(path.resolve(outDir) + path.sep) && file !== path.resolve(outDir)) throw new Error(`非法 chunk 路径: ${rel}`);
      if (!fs.existsSync(file)) continue;
      all.push(...await readJsonlFile(file));
    }
    return all;
  }
  const jsonFile = path.join(outDir, 'export.json');
  if (fs.existsSync(jsonFile)) {
    const obj = JSON.parse(await fsp.readFile(jsonFile, 'utf8'));
    return Array.isArray(obj?.messages) ? obj.messages : [];
  }
  const directJsonl = path.join(outDir, 'history.jsonl');
  if (fs.existsSync(directJsonl)) return readJsonlFile(directJsonl);
  return [];
}

async function streamMessagesFromOutputDir(outDir, res) {
  const manifestFile = path.join(outDir, 'manifest.json');
  res.writeHead(200, {
    'content-type': 'application/x-ndjson; charset=utf-8',
    'access-control-allow-origin': '*',
    'cache-control': 'no-store'
  });
  if (fs.existsSync(manifestFile)) {
    const manifest = JSON.parse(await fsp.readFile(manifestFile, 'utf8'));
    for (const chunk of manifest?.chunked?.chunks || []) {
      const rel = chunk.relativePath || path.join(manifest.chunked?.chunksDir || 'chunks', chunk.fileName || '');
      const file = path.resolve(outDir, rel);
      if (!file.startsWith(path.resolve(outDir) + path.sep) && file !== path.resolve(outDir)) throw new Error(`非法 chunk 路径: ${rel}`);
      if (!fs.existsSync(file)) continue;
      await new Promise((resolve, reject) => {
        const input = fs.createReadStream(file);
        input.on('error', reject);
        input.on('end', resolve);
        input.pipe(res, { end: false });
      });
    }
    return res.end();
  }
  const messages = await loadMessagesFromOutputDir(outDir);
  for (const msg of messages) await writeLine(res, JSON.stringify(msg, jsonReplacer) + '\n');
  res.end();
}

function resolveOutputRelativeFile(outDir, relativePath) {
  const rel = String(relativePath || '').replace(/\\/g, '/').replace(/^\/+/, '');
  if (!rel || rel.includes('\0')) throw new Error('缺少输出文件路径');
  const root = path.resolve(outDir);
  const file = path.resolve(root, rel);
  if (file !== root && !file.startsWith(root + path.sep)) throw new Error(`非法输出文件路径: ${relativePath}`);
  return { rel, file };
}

async function readChunkedOutputDescriptor(outDir) {
  const manifestPath = path.join(outDir, 'manifest.json');
  if (!fs.existsSync(manifestPath)) throw new Error('该导出结果不是分块 JSONL，缺少 manifest.json');
  const manifest = JSON.parse(await fsp.readFile(manifestPath, 'utf8'));
  const files = [];
  const declared = new Set(['manifest.json']);
  for (const chunk of manifest?.chunked?.chunks || []) {
    const rel = String(chunk.relativePath || path.join(manifest.chunked?.chunksDir || 'chunks', chunk.fileName || '')).replace(/\\/g, '/');
    const { file } = resolveOutputRelativeFile(outDir, rel);
    if (!fs.existsSync(file)) throw new Error(`manifest 声明的 chunk 不存在: ${rel}`);
    const stat = await fsp.stat(file);
    declared.add(rel);
    files.push({
      index: Number(chunk.index || files.length + 1),
      fileName: chunk.fileName || path.basename(rel),
      relativePath: rel,
      count: Number(chunk.count || 0),
      bytes: stat.size,
      start: chunk.start || '',
      end: chunk.end || ''
    });
  }
  return { manifest, files, declared };
}

function contentTypeForOutputFile(file) {
  if (file.endsWith('.jsonl')) return 'application/x-ndjson; charset=utf-8';
  if (file.endsWith('.json')) return 'application/json; charset=utf-8';
  return 'application/octet-stream';
}

async function streamDeclaredOutputFile(record, relativePath, res) {
  const descriptor = await readChunkedOutputDescriptor(record.outputDir);
  const { rel, file } = resolveOutputRelativeFile(record.outputDir, relativePath);
  if (!descriptor.declared.has(rel)) throw new Error(`文件不在 manifest 声明中: ${rel}`);
  const stat = await fsp.stat(file);
  res.writeHead(200, {
    'content-type': contentTypeForOutputFile(file),
    'content-length': stat.size,
    'content-disposition': `attachment; filename*=UTF-8''${encodeURIComponent(path.basename(file))}`,
    'access-control-allow-origin': '*',
    'cache-control': 'no-store'
  });
  await new Promise((resolve, reject) => {
    const input = fs.createReadStream(file);
    input.on('error', reject);
    input.on('end', resolve);
    input.pipe(res);
  });
}

async function sendChunkedOutputManifest(record, res) {
  const descriptor = await readChunkedOutputDescriptor(record.outputDir);
  return sendJson(res, {
    ok: true,
    id: record.id || '',
    sessionType: record.sessionType || record.peer?.typeName || '',
    sessionId: String(record.sessionId || record.peer?.uin || record.peer?.peerUid || ''),
    sessionName: record.sessionName || record.peer?.name || '',
    format: 'chunked-jsonl',
    manifest: descriptor.manifest,
    files: descriptor.files
  });
}


function computeStats(messages) {
  const st = createStatsTracker();
  for (const m of messages) updateStatsTracker(st, m);
  return finalizeStats(st);
}

function makeChatInfo(peer, messagesOrCount) {
  const self = getSelfInfo();
  const result = {
    name: peer.name,
    type: peer.typeName,
    id: String(peer.uin || peer.peerUid || ''),
    uin: String(peer.uin || ''),
    uid: String(peer.peerUid || '')
  };
  if (self.uid) result.selfUid = self.uid;
  if (self.uin) result.selfUin = self.uin;
  if (self.name) result.selfName = self.name;
  if (peer.avatar) result.avatar = peer.avatar;
  if (peer.participantCount !== undefined) result.participantCount = peer.participantCount;
  return result;
}

function createStatsTracker() {
  return { total: 0, messageTypes: {}, senders: new Map(), min: 0, max: 0, resources: { total: 0, byType: {}, totalSize: 0 } };
}

function updateStatsTracker(st, msg) {
  st.total++;
  st.messageTypes[msg.type || 'unknown'] = (st.messageTypes[msg.type || 'unknown'] || 0) + 1;
  if (!st.min || msg.timestamp < st.min) st.min = msg.timestamp;
  if (!st.max || msg.timestamp > st.max) st.max = msg.timestamp;
  const uid = msg.sender?.uid || 'unknown';
  if (!st.senders.has(uid)) st.senders.set(uid, { uid, name: msg.sender?.name, messageCount: 0, percentage: 0 });
  const sender = st.senders.get(uid);
  sender.messageCount++;
  if (!sender.name && msg.sender?.name) sender.name = msg.sender.name;
  const resArr = msg.content?.resources || [];
  for (const r of resArr) {
    st.resources.total++;
    const rt = r.type || 'file';
    st.resources.byType[rt] = (st.resources.byType[rt] || 0) + 1;
    st.resources.totalSize += Number(r.size || r.fileSize || 0) || 0;
  }
}

function finalizeStats(st) {
  const total = st.total || 0;
  const sendersArray = Array.from(st.senders.values())
    .map(x => ({ ...x, percentage: total ? Math.round(x.messageCount / total * 10000) / 100 : 0 }))
    .sort((a, b) => b.messageCount - a.messageCount);
  return {
    totalMessages: total,
    timeRange: {
      start: st.min ? new Date(st.min).toISOString() : '',
      end: st.max ? new Date(st.max).toISOString() : '',
      durationDays: st.min && st.max ? Math.max(1, Math.round((st.max - st.min) / 86400000)) : 0
    },
    messageTypes: st.messageTypes,
    senders: sendersArray,
    resources: st.resources
  };
}

function qceMetadata() {
  return { name: QCE_APP_NAME, copyright: QCE_COPYRIGHT, version: VERSION };
}

function qceExportOptions(opts = {}) {
  return {
    includedFields: ['id', 'timestamp', 'sender', 'content', 'resources'],
    filters: {
      ...(opts.fromMs ? { startTime: Math.floor(opts.fromMs / 1000) } : {}),
      ...(opts.toMs ? { endTime: Math.floor(opts.toMs / 1000) } : {})
    },
    options: {
      includeResourceLinks: !!opts.exportResources,
      includeSystemMessages: !!opts.includeSystem,
      includeRecalledMessages: !!opts.includeRecalled,
      timeFormat: 'ISO',
      encoding: 'utf-8'
    }
  };
}

function resourceDedupeKey(resource) {
  const md5 = resource?.md5 || resource?.fileMd5 || resource?.md5HexStr || resource?.key || resource?.emojiId;
  if (md5) return `hash:${String(md5).toLowerCase()}`;
  const srcPath = resource?.path || resource?.localPath;
  if (srcPath) {
    try { return `path:${fs.realpathSync(srcPath)}`; } catch { return `path:${path.resolve(String(srcPath))}`; }
  }
  if (resource?.url) return `url:${normalizeUrlForDedupe(resource.url)}`;
  return `fallback:${resource?.type || 'file'}:${isEmojiResource(resource) ? 'emoji' : ''}:${resource?.filename || resource?.fileName || ''}:${resource?.size || resource?.fileSize || 0}`;
}

function shouldDownloadCandidate(resource, opts = {}) {
  return !!opts.exportResources;
}

function collectDownloadCandidatesFromMessage(msg, opts = {}) {
  const out = [];
  const seen = new Set();
  function add(x) {
    if (!x || !shouldDownloadCandidate(x, opts)) return;
    const key = resourceDedupeKey(x);
    if (seen.has(key)) return;
    seen.add(key);
    out.push(x);
  }
  for (const r of msg.content?.resources || []) add({ ...r, messageId: msg.id });
  for (const el of msg.content?.elements || []) {
    const d = el.data || {};
    if (el.type === 'market_face') add({ type: 'image', kind: 'emoji', category: 'emoji', originalType: 'market_face', filename: d.filename || d.name || d.emojiId || 'sticker', url: d.url, localPath: d.localPath, path: d.localPath, md5: d.key || d.emojiId || d.md5, key: d.key || d.emojiId, emojiId: d.emojiId, emojiPackageId: d.emojiPackageId, tabName: d.tabName, messageId: msg.id });
    else if (el.type === 'file') add({ type: 'file', filename: d.filename || d.fileName || 'file', size: d.size || d.fileSize || 0, url: d.url, localPath: d.localPath, path: d.localPath, md5: d.md5 || d.fileMd5, messageId: msg.id });
    else if (['image', 'audio', 'video'].includes(el.type)) add({ type: el.type, filename: d.filename || d.fileName || el.type, size: d.size || d.fileSize || 0, url: d.url, localPath: d.localPath, path: d.localPath, md5: d.md5 || d.fileMd5, messageId: msg.id });
  }
  return out;
}

function summarizeResourcesFromMessages(messages, opts = {}) {
  const all = [];
  const seen = new Set();
  for (const msg of messages) {
    for (const r of collectDownloadCandidatesFromMessage(msg, opts)) {
      const key = resourceDedupeKey(r);
      if (seen.has(key)) continue;
      seen.add(key);
      all.push(r);
    }
  }
  return all;
}

async function copyOrDownloadResource(resource, baseDir, usedNames, cache) {
  let key = resourceDedupeKey(resource);
  if (cache && cache.has(key)) return { ...resource, status: 'duplicate', localPath: cache.get(key) };
  const emojiResource = isEmojiResource(resource);
  let sub = 'files';
  if (emojiResource) sub = 'emojis';
  else if (resource.type === 'image') sub = 'images';
  else if (resource.type === 'audio') sub = 'audios';
  else if (resource.type === 'video') sub = 'videos';
  const dir = path.join(baseDir, 'resources', sub);
  ensureDir(dir);
  let ext = path.extname((resource.filename || resource.fileName) || '');
  if (!ext) {
    if (emojiResource || resource.type === 'image') ext = '.jpg';
    else if (resource.type === 'audio') ext = '.silk';
    else if (resource.type === 'video') ext = '.mp4';
    else ext = '.bin';
  }
  let name = safeName(path.basename((resource.filename || resource.fileName) || `${emojiResource ? 'emoji' : (resource.type || 'file')}${ext}`));
  if (!path.extname(name)) name += ext;
  if (emojiResource) {
    const hint = emojiStableNameHint(resource);
    name = appendNameSuffix(name, hint);
  }
  let base = name.replace(new RegExp(`${ext.replace('.', '\.')}$`), '');
  let finalName = name;
  let n = 1;
  const srcPath = resource.path || resource.localPath;
  let tempFile = '';
  let sourceFile = '';
  try {
    if (srcPath && fs.existsSync(srcPath)) {
      sourceFile = srcPath;
    } else if (resource.url && /^https?:\/\//i.test(resource.url)) {
      tempFile = path.join(dir, `.download_${crypto.randomUUID()}${ext}`);
      await downloadToFile(resource.url, tempFile);
      sourceFile = tempFile;
    }
    if (!sourceFile) return { ...resource, status: 'unresolved' };
    const hash = sha1File(sourceFile);
    if (hash) key = `sha1:${hash}`;
    if (cache && cache.has(key)) {
      if (tempFile) { try { fs.unlinkSync(tempFile); } catch {} }
      return { ...resource, status: 'duplicate', localPath: cache.get(key) };
    }
    if (emojiResource && hash) {
      finalName = appendNameSuffix(name, hash.slice(0, 12));
      base = finalName.replace(new RegExp(`${ext.replace('.', '\.')}$`), '');
    }
    while (usedNames.has(`${sub}/${finalName}`)) finalName = `${base}_${n++}${ext}`;
    usedNames.add(`${sub}/${finalName}`);
    const dst = path.join(dir, finalName);
    const rel = path.join('resources', sub, finalName).replace(/\\/g, '/');
    if (tempFile) fs.renameSync(tempFile, dst);
    else await fsp.copyFile(sourceFile, dst);
    if (cache) { cache.set(key, rel); cache.set(resourceDedupeKey(resource), rel); }
    return { ...resource, status: tempFile ? 'downloaded' : 'copied', localPath: rel };
  } catch (err) {
    if (tempFile) { try { fs.unlinkSync(tempFile); } catch {} }
    throw err;
  }
}

function downloadToFile(url, dst) {
  return new Promise((resolve, reject) => {
    const client = url.startsWith('https:') ? https : http;
    const req = client.get(url, res => {
      if ([301, 302, 303, 307, 308].includes(res.statusCode) && res.headers.location) {
        res.resume();
        return downloadToFile(new URL(res.headers.location, url).href, dst).then(resolve, reject);
      }
      if (res.statusCode < 200 || res.statusCode >= 300) {
        res.resume();
        return reject(new Error(`HTTP ${res.statusCode}`));
      }
      const ws = fs.createWriteStream(dst);
      res.pipe(ws);
      ws.on('finish', () => ws.close(resolve));
      ws.on('error', reject);
    });
    req.setTimeout(20000, () => req.destroy(new Error('download timeout')));
    req.on('error', reject);
  });
}

async function exportResources(messages, outDir, task, concurrency) {
  const all = [];
  for (const msg of messages) {
    for (const r of msg.content?.resources || []) {
      all.push({ ...r, messageId: msg.id });
    }
  }
  if (!all.length) { pushTaskLog(task, '资源导出跳过：消息中没有可解析的资源候选'); return []; }
  pushTaskLog(task, `开始导出资源：${all.length} 个候选文件，workers=${Math.max(1, Math.min(8, Number(concurrency || 3)))}`);
  const usedNames = new Set();
  const result = [];
  let idx = 0;
  const workers = Math.max(1, Math.min(8, Number(concurrency || 3)));
  async function worker() {
    while (!task.stopRequested) {
      const i = idx++;
      if (i >= all.length) return;
      const item = all[i];
      try {
        result.push(await copyOrDownloadResource(item, outDir, usedNames));
      } catch (err) {
        result.push({ ...item, status: 'failed', error: err?.message || String(err) });
      }
      if (i % 50 === 0) {
        const done = Math.min(i + 1, all.length);
        updateTask(task, { message: `资源导出 ${done}/${all.length}` });
        pushTaskLog(task, `资源导出进度 ${done}/${all.length}，最近=${item.type || 'file'}:${(item.filename || item.fileName) || item.url || item.path || ''}，status=${result[result.length - 1]?.status || 'unknown'}`);
      }
    }
  }
  await Promise.all(Array.from({ length: workers }, () => worker()));
  writeJson(path.join(outDir, 'resources_manifest.json'), result);
  const byStatus = result.reduce((m, r) => { m[r.status || 'unknown'] = (m[r.status || 'unknown'] || 0) + 1; return m; }, {});
  pushTaskLog(task, `资源导出完成：${JSON.stringify(byStatus)}`);
  return result;
}


async function writeSingleJson(outDir, peer, messages, opts, task = null) {
  const obj = {
    metadata: qceMetadata(),
    chatInfo: makeChatInfo(peer, messages),
    statistics: computeStats(messages),
    messages,
    exportOptions: qceExportOptions(opts)
  };
  const file = path.join(outDir, 'export.json');
  if (task) pushTaskLog(task, `开始写入 JSON：${file} messages=${messages.length}。JSON 是一次性完整文件，超大聊天建议只选 JSONL。`);
  writeJson(file, obj);
  if (task) pushTaskLog(task, `JSON 写入完成：${file}`);
  return file;
}

async function exportResourcesFromCandidates(all, outDir, task, concurrency) {
  if (!all.length) { pushTaskLog(task, '资源导出跳过：没有可解析的资源候选'); return []; }
  pushTaskLog(task, `开始导出资源：${all.length} 个去重候选，workers=${Math.max(1, Math.min(8, Number(concurrency || 3)))}`);
  const usedNames = new Set();
  const cache = new Map();
  const result = [];
  let idx = 0;
  const workers = Math.max(1, Math.min(8, Number(concurrency || 3)));
  async function worker() {
    while (!task.stopRequested) {
      const i = idx++;
      if (i >= all.length) return;
      const item = all[i];
      try {
        result.push(await copyOrDownloadResource(item, outDir, usedNames, cache));
      } catch (err) {
        result.push({ ...item, status: 'failed', error: err?.message || String(err) });
      }
      if (i % 50 === 0) {
        const done = Math.min(i + 1, all.length);
        updateTask(task, { message: `资源导出 ${done}/${all.length}` });
        pushTaskLog(task, `资源导出进度 ${done}/${all.length}，status=${result[result.length - 1]?.status || 'unknown'}`);
      }
    }
  }
  await Promise.all(Array.from({ length: workers }, () => worker()));
  writeJson(path.join(outDir, 'resources_manifest.json'), result);
  const byStatus = result.reduce((m, r) => { m[r.status || 'unknown'] = (m[r.status || 'unknown'] || 0) + 1; return m; }, {});
  pushTaskLog(task, `资源导出完成：${JSON.stringify(byStatus)}`);
  return result;
}

async function writeChunkedJsonlFromMessages(outDir, peer, messages, task = null) {
  return writeChunkedJsonlStream(outDir, peer, messages, task, { alreadyNormalized: true });
}

async function writeChunkedJsonlFromRaw(outDir, peer, rawMessages, task = null) {
  return writeChunkedJsonlStream(outDir, peer, rawMessages, task, { alreadyNormalized: false });
}

async function writeChunkedJsonlStream(outDir, peer, sourceMessages, task = null, options = {}) {
  const chunksDir = path.join(outDir, 'chunks');
  await fsp.rm(chunksDir, { recursive: true, force: true });
  ensureDir(chunksDir);
  const chunks = [];
  let chunkIndex = 0;
  let chunkLines = [];
  let chunkCount = 0;
  let chunkBytes = 0;
  let chunkStart = 0;
  let chunkEnd = 0;
  const statsTracker = createStatsTracker();
  const resources = [];
  const resourceSeen = new Set();

  async function writeCompletedChunk() {
    if (!chunkCount) return;
    chunkIndex++;
    const fileName = `chunk_${String(chunkIndex).padStart(4, '0')}.jsonl`;
    const tmp = path.join(chunksDir, `.${fileName}.${process.pid}.${Date.now()}.tmp`);
    const finalPath = path.join(chunksDir, fileName);
    await fsp.writeFile(tmp, chunkLines.join(''), 'utf8');
    await fsp.rename(tmp, finalPath);
    const info = {
      index: chunkIndex,
      fileName,
      relativePath: `chunks/${fileName}`,
      start: chunkStart ? new Date(chunkStart).toISOString() : '',
      end: chunkEnd ? new Date(chunkEnd).toISOString() : '',
      count: chunkCount,
      bytes: chunkBytes
    };
    chunks.push(info);
    if (task) {
      pushTaskLog(task, `JSONL chunk 完成：${fileName} count=${chunkCount} bytes=${chunkBytes}`);
      updateTask(task, { message: `已写入 ${fileName}`, messageCount: statsTracker.total, progress: Math.max(task.progress || 0, Math.min(94, 75 + chunks.length)) });
    }
    chunkLines = [];
    chunkCount = 0;
    chunkBytes = 0;
    chunkStart = 0;
    chunkEnd = 0;
    await new Promise(resolve => setImmediate(resolve));
  }

  const total = Array.isArray(sourceMessages) ? sourceMessages.length : 0;
  if (task) pushTaskLog(task, `开始写入 QCE chunked JSONL：source=${total || '?'} maxMessagesPerChunk=${MAX_MESSAGES_PER_CHUNK} maxBytesPerChunk=${MAX_BYTES_PER_CHUNK} fileName=chunks/chunk_0001.jsonl...`);
  let written = 0;

  for (const src of sourceMessages) {
    if (task?.stopRequested) break;
    const msg = options.alreadyNormalized ? src : normalizeMessage(src, peer);
    const line = JSON.stringify(msg, jsonReplacer) + '\n';
    const bytes = Buffer.byteLength(line, 'utf8');
    if (chunkCount > 0 && (chunkCount >= MAX_MESSAGES_PER_CHUNK || chunkBytes + bytes > MAX_BYTES_PER_CHUNK)) {
      await writeCompletedChunk();
    }
    chunkLines.push(line);
    chunkCount++;
    chunkBytes += bytes;
    if (!chunkStart || msg.timestamp < chunkStart) chunkStart = msg.timestamp;
    if (!chunkEnd || msg.timestamp > chunkEnd) chunkEnd = msg.timestamp;
    updateStatsTracker(statsTracker, msg);
    for (const r of collectDownloadCandidatesFromMessage(msg, task?.options || {})) {
      const key = resourceDedupeKey(r);
      if (!resourceSeen.has(key)) { resourceSeen.add(key); resources.push(r); }
    }
    written++;
    if (task && written % 5000 === 0) {
      const pct = total ? (75 + (written / total) * 18) : Math.min(93, 75 + Math.floor(written / 50000));
      updateTask(task, { message: `JSONL 处理中 ${written}/${total || '?'}`, messageCount: written, progress: pct });
      pushTaskLog(task, `JSONL 缓冲进度：${written}/${total || '?'}，当前chunk=${chunkCount}条/${chunkBytes}bytes`);
      await new Promise(resolve => setImmediate(resolve));
    }
  }
  await writeCompletedChunk();

  const stats = finalizeStats(statsTracker);
  const metadata = { ...qceMetadata(), exportTime: new Date().toISOString(), format: 'chunked-jsonl' };
  const manifest = {
    metadata,
    chatInfo: makeChatInfo(peer, stats.totalMessages),
    statistics: {
      totalMessages: stats.totalMessages,
      timeRange: stats.timeRange,
      messageTypes: stats.messageTypes,
      senders: stats.senders
    },
    chunked: {
      format: 'jsonl',
      chunksDir: 'chunks',
      chunkFileExt: '.jsonl',
      maxMessagesPerChunk: MAX_MESSAGES_PER_CHUNK,
      maxBytesPerChunk: MAX_BYTES_PER_CHUNK,
      chunks
    }
  };
  writeJson(path.join(outDir, 'manifest.json'), manifest);
  if (task) pushTaskLog(task, `chunked JSONL 写入完成：messages=${stats.totalMessages} chunks=${chunks.length} manifest=${path.join(outDir, 'manifest.json')}`);
  return { file: path.join(outDir, 'manifest.json'), stats, messageCount: stats.totalMessages, resources };
}

async function writeChunkedJsonl(outDir, peer, messages, task = null) {
  const res = await writeChunkedJsonlFromMessages(outDir, peer, messages, task);
  return res.file;
}

function loadHistory() {
  try {
    const data = JSON.parse(fs.readFileSync(state.historyFile, 'utf8'));
    return Array.isArray(data) ? data : [];
  } catch { return []; }
}

function saveHistory(list) {
  writeJson(state.historyFile, list.slice(-500));
}

function registerExportHistory(record) {
  const list = loadHistory();
  list.push(record);
  saveHistory(list);
}

function parseTimeRange(opts, baseHistory) {
  const range = opts.timePreset || 'all';
  let fromMs = 0, toMs = 0, fromSeq = 0;
  const now = nowMs();
  if (opts.incremental && (baseHistory?.latestTimestamp || opts.incrementalStartTimestamp || baseHistory?.latestSeq || opts.incrementalStartSeq)) {
    const latestTimestamp = Number(baseHistory?.latestTimestamp || opts.incrementalStartTimestamp || 0) || 0;
    const latestSeq = Number(baseHistory?.latestSeq || opts.incrementalStartSeq || 0) || 0;
    const overlapMs = Math.max(0, Number(opts.incrementalOverlapMs ?? DEFAULT_INCREMENTAL_OVERLAP_MS));
    const overlapSeq = Math.max(0, Number(opts.incrementalOverlapSeq ?? DEFAULT_INCREMENTAL_OVERLAP_SEQ));
    // Deliberately overlap the previous export. Using latestTimestamp + 1 can lose
    // messages that share a timestamp or arrive late; overlap + merge is idempotent.
    if (latestTimestamp) fromMs = Math.max(0, latestTimestamp - overlapMs);
    if (latestSeq) fromSeq = Math.max(1, latestSeq - overlapSeq);
  } else if (range === '1d') fromMs = now - 86400000;
  else if (range === '1w') fromMs = now - 7 * 86400000;
  else if (range === '1m') fromMs = now - 30 * 86400000;
  else if (range === '1y') fromMs = now - 365 * 86400000;
  else if (range === 'manual') {
    if (opts.from) fromMs = new Date(opts.from).getTime();
    if (opts.to) toMs = new Date(opts.to).getTime();
  }
  return {
    fromMs: Number.isFinite(fromMs) ? fromMs : 0,
    toMs: Number.isFinite(toMs) ? toMs : 0,
    fromSeq: Number.isFinite(fromSeq) ? fromSeq : 0
  };
}

function canonicalSessionType(value) {
  return String(value || '').toLowerCase() === 'group' ? 'group' : 'private';
}

function historySessionId(h) {
  const peer = h?.peer || {};
  return String(h?.sessionId || peer.uin || peer.peerUid || peer.uid || '');
}

function sameConversationRecord(h, peer) {
  if (!h || !peer) return false;
  const ht = canonicalSessionType(h.sessionType || h.peer?.typeName);
  const pt = canonicalSessionType(peer.typeName);
  return ht === pt && historySessionId(h) === String(peer.uin || peer.peerUid || '');
}

function findLatestHistoryForPeer(history, peer) {
  return history
    .filter(h => sameConversationRecord(h, peer) && h.outputDir && fs.existsSync(h.outputDir))
    .sort((a, b) => String(b.completedAt || b.createdAt || '').localeCompare(String(a.completedAt || a.createdAt || '')))[0] || null;
}

function cleanupAfterExport(task) {
  try {
    task.runtimeCache = null;
    if (Array.isArray(task.logs) && task.logs.length > 1200) task.logs = task.logs.slice(-1200);
    if (global.gc) global.gc();
  } catch {}
}

async function runExport(task) {
  try {
    updateTask(task, { status: 'running', message: '准备导出', progress: 1 });
    const opts = task.options;
    const peer = await resolvePeer(opts);
    task.peer = peer;
    const history = loadHistory();
    let baseHistory = opts.incrementalBaseId ? history.find(h => h.id === opts.incrementalBaseId) : null;
    if (opts.incremental && !baseHistory) baseHistory = findLatestHistoryForPeer(history, peer);
    if (baseHistory && !sameConversationRecord(baseHistory, peer)) throw new Error('增量基准与目标会话不一致');
    const range = parseTimeRange(opts, baseHistory);
    opts.fromMs = range.fromMs;
    opts.toMs = range.toMs;
    opts.fromSeq = range.fromSeq;
    const stamp = new Date().toISOString().replace(/[-:T.Z]/g, '').slice(0, 14);
    const suffix = opts.incremental ? '_incremental_merged' : '';
    const dirName = `${peer.typeName}_${safeName(peer.name)}_${safeName(peer.uin || peer.peerUid)}_${stamp}${suffix}`;
    const outDir = opts.exactOutputDir || (opts.outputDir ? path.join(opts.outputDir, dirName) : path.join(state.exportsDir, dirName));
    ensureDir(outDir);
    task.outputDir = outDir;
    pushTaskLog(task, `输出目录：${outDir}`);
    pushTaskLog(task, `任务参数：session=${peer.typeName}/${peer.name}/${peer.uin || peer.peerUid} format=${opts.format} time=${range.fromMs ? new Date(range.fromMs).toISOString() : 'all'}..${range.toMs ? new Date(range.toMs).toISOString() : 'now'} seqFrom=${range.fromSeq || '-'} batch=${opts.batchCount} retry=${opts.historyRetryCount} parallel=${!!opts.parallelHistory} workers=${opts.historyWorkers} seqWindow=${opts.seqWindow} resources=${!!opts.exportResources}`);
    if (baseHistory) pushTaskLog(task, `增量基准：${baseHistory.id} ${baseHistory.outputDir}；将读取重叠区并与旧记录去重合并`);
    const useParallel = !!opts.parallelHistory && !opts.incremental;
    pushTaskLog(task, useParallel ? '直接 QQNT IPC 并行拉取历史' : '直接 QQNT IPC 顺序拉取历史');
    updateTask(task, { message: '读取历史消息', progress: 5 });

    const raw = useParallel ? await fetchParallel(peer, opts, task) : await fetchSequential(peer, opts, task);
    let sortedRaw = dedupeAndSort(raw);
    if (opts.maxMessages && sortedRaw.length > opts.maxMessages) sortedRaw = sortedRaw.slice(-opts.maxMessages);
    const duplicateCount = raw.length - sortedRaw.length;
    const seqRangeAfter = msgSeqRange(sortedRaw);
    pushTaskLog(task, `历史读取完成：原始 ${raw.length} 条，去重后 ${sortedRaw.length} 条，重复 ${duplicateCount} 条，seqRange=${seqRangeAfter.min ? seqRangeAfter.min + '-' + seqRangeAfter.max : '-'}`);
    updateTask(task, { progress: 75, message: '合并并写入 JSON/JSONL', messageCount: sortedRaw.length });

    const newMessages = sortedRaw.map(m => normalizeMessage(m, peer));
    let messages = newMessages;
    let baseMessages = [];
    if (opts.incremental && opts.autoMergeIncremental && baseHistory?.outputDir) {
      baseMessages = await loadMessagesFromOutputDir(baseHistory.outputDir);
      messages = mergeNormalizedMessages(baseMessages, newMessages);
      pushTaskLog(task, `增量合并：基准=${baseMessages.length} 新抓取=${newMessages.length} 合并后=${messages.length} 去重=${baseMessages.length + newMessages.length - messages.length}`);
    }

    const outputs = [];
    let stats = null;
    let messageCount = 0;
    let resourcesForExport = [];

    if (opts.format === 'jsonl') {
      const jsonl = await writeChunkedJsonlFromMessages(outDir, peer, messages, task);
      outputs.push(jsonl.file);
      stats = jsonl.stats;
      messageCount = jsonl.messageCount;
      resourcesForExport = jsonl.resources;
    } else {
      outputs.push(await writeSingleJson(outDir, peer, messages, opts, task));
      stats = computeStats(messages);
      messageCount = messages.length;
      resourcesForExport = summarizeResourcesFromMessages(messages, opts);
    }

    if (opts.exportResources) {
      updateTask(task, { progress: 82, message: '导出资源' });
      await exportResourcesFromCandidates(resourcesForExport, outDir, task, opts.resourceWorkers || 3);
    }

    const seqStats = normalizedSeqRange(messages);
    const record = {
      id: task.id,
      createdAt: task.createdAt,
      completedAt: new Date().toISOString(),
      outputDir: outDir,
      peer,
      options: opts,
      messageCount,
      latestTimestamp: stats.timeRange.end ? new Date(stats.timeRange.end).getTime() : 0,
      earliestTimestamp: stats.timeRange.start ? new Date(stats.timeRange.start).getTime() : 0,
      latestSeq: seqStats.max,
      earliestSeq: seqStats.min,
      baseHistoryId: baseHistory?.id || null,
      mergedBaseCount: baseMessages.length,
      fetchedCount: newMessages.length,
      stoppedEarly: !!task.stopRequested,
      incompleteWindows: task.incompleteWindows,
      outputs,
      sessionType: peer.typeName === 'group' ? 'group' : 'private',
      sessionId: peer.uin || peer.peerUid,
      sessionName: peer.name,
      formats: [opts.format],
      latestTime: stats.timeRange.end || '',
      earliestTime: stats.timeRange.start || ''
    };
    registerExportHistory(record);
    updateTask(task, { status: task.stopRequested ? 'stopped' : 'completed', progress: 100, message: task.stopRequested ? '已提前停止并保存' : '导出完成', result: { ...record, paths: outputs }, messageCount });
    pushTaskLog(task, task.stopRequested ? '已提前停止并保存部分结果' : '导出完成');
  } catch (err) {
    if (err?.message === 'STOP_REQUESTED') {
      updateTask(task, { status: 'stopped', message: '已停止' });
    } else {
      updateTask(task, { status: 'failed', message: err?.message || String(err), error: err?.stack || String(err) });
      pushTaskLog(task, `导出失败：${err?.message || err}`);
    }
  } finally {
    cleanupAfterExport(task);
  }
}


function normalizeExportOptions(input = {}) {
  const formats = Array.isArray(input.formats) ? input.formats : [];
  const rawFormat = input.format || formats[0] || 'json';
  const format = rawFormat === 'both' ? 'json' : rawFormat;
  const timePreset = input.timePreset || ({ incremental: 'all', manual: 'manual' }[input.timeMode] || input.timeMode) || 'manual';
  return {
    format,
    timePreset,
    from: input.from || input.startTime || '',
    to: input.to || input.endTime || '',
    chatType: input.chatType || input.sessionType || 'private',
    id: input.id || input.sessionId || input.uin || input.peerUid || '',
    sessionId: input.sessionId || input.id || input.uin || '',
    peerUid: input.peerUid || input.uid || '',
    uin: input.uin || input.sessionId || input.id || '',
    name: input.name || input.sessionName || '',
    sessionName: input.sessionName || input.name || '',
    includeSystem: input.includeSystem ?? input.includeSystemMessages ?? true,
    includeRecalled: input.includeRecalled ?? input.includeRecalledMessages ?? false,
    exportResources: input.exportResources ?? input.includeResources ?? input.exportMedia ?? input.exportFiles ?? input.exportStickers ?? false,
    parallelHistory: input.parallelHistory ?? true,
    historyWorkers: Number(input.historyWorkers || 4),
    batchCount: Number(input.batchCount || input.batchSize || 1000),
    seqWindow: Number(input.seqWindow || input.seqWindowSize || 50000),
    resourceWorkers: Number(input.resourceWorkers || 3),
    historyRetryCount: Math.max(5, Number(input.historyRetryCount || input.retries || 5)),
    maxMessages: Math.max(0, Number(input.maxMessages || 0)),
    incremental: !!(input.incremental || input.timeMode === 'incremental'),
    incrementalBaseId: input.incrementalBaseId || input.incrementalOf || null,
    incrementalStartTimestamp: Number(input.incrementalStartTimestamp || 0) || 0,
    incrementalStartSeq: Number(input.incrementalStartSeq || 0) || 0,
    incrementalOverlapMs: Math.max(0, Number(input.incrementalOverlapMs ?? DEFAULT_INCREMENTAL_OVERLAP_MS)),
    incrementalOverlapSeq: Math.max(0, Number(input.incrementalOverlapSeq ?? DEFAULT_INCREMENTAL_OVERLAP_SEQ)),
    autoMergeIncremental: input.autoMergeIncremental ?? true,
    outputDir: input.outputDir || '',
    exactOutputDir: input.exactOutputDir || input.outputPath || '',
    verboseLog: input.verboseLog ?? true
  };
}

function createTask(options) {
  const id = crypto.randomUUID();
  const normalized = normalizeExportOptions(options);
  const task = {
    id,
    status: 'queued',
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    progress: 0,
    progressPercent: '0%',
    message: '排队中',
    messageCount: 0,
    options: normalized,
    logs: [],
    stopRequested: false,
    incompleteWindows: [],
    result: null
  };
  state.tasks.set(id, task);
  setImmediate(() => runExport(task));
  return task;
}


function createMergeTask(options) {
  const id = crypto.randomUUID();
  const task = {
    id,
    status: 'queued',
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    progress: 0,
    progressPercent: '0%',
    message: '排队中',
    messageCount: 0,
    options: { operation: 'merge', ...(options || {}) },
    logs: [],
    stopRequested: false,
    incompleteWindows: [],
    result: null
  };
  state.tasks.set(id, task);
  setImmediate(() => runMergeTask(task));
  return task;
}

async function runMergeTask(task) {
  try {
    updateTask(task, { status: 'running', message: '读取待合并记录', progress: 5 });
    const body = task.options || {};
    const history = loadHistory();
    const ids = Array.isArray(body.historyIds) ? body.historyIds.map(String) : [];
    const selected = ids.map(id => history.find(h => String(h.id) === id)).filter(Boolean);
    const sourceDirs = [
      ...selected.map(h => h.outputDir),
      ...(Array.isArray(body.sourceDirs) ? body.sourceDirs : [])
    ].filter(Boolean);
    if (sourceDirs.length < 1) throw new Error('至少需要一个 historyIds 或 sourceDirs');

    const declaredType = body.chatType || body.sessionType || selected[0]?.sessionType || selected[0]?.peer?.typeName || '';
    const declaredId = String(body.id || body.sessionId || selected[0]?.sessionId || selected[0]?.peer?.uin || selected[0]?.peer?.peerUid || '');
    for (const rec of selected) {
      if (declaredType && canonicalSessionType(rec.sessionType || rec.peer?.typeName) !== canonicalSessionType(declaredType)) throw new Error('拒绝合并不同类型的会话');
      if (declaredId && historySessionId(rec) !== declaredId) throw new Error('拒绝合并不同会话 ID 的记录');
    }

    const lists = [];
    for (let i = 0; i < sourceDirs.length; i++) {
      if (task.stopRequested) throw new Error('STOP_REQUESTED');
      const arr = await loadMessagesFromOutputDir(sourceDirs[i]);
      lists.push(arr);
      updateTask(task, { progress: Math.min(60, 10 + Math.round((i + 1) / sourceDirs.length * 50)), message: `已读取 ${i + 1}/${sourceDirs.length}`, messageCount: lists.reduce((n, x) => n + x.length, 0) });
      pushTaskLog(task, `读取合并源 ${sourceDirs[i]}：${arr.length} 条`);
    }
    const merged = mergeNormalizedMessages(...lists);
    const peer = {
      typeName: canonicalSessionType(declaredType),
      name: String(body.name || body.sessionName || selected[0]?.sessionName || selected[0]?.peer?.name || declaredId || 'merged'),
      uin: declaredId,
      peerUid: String(body.peerUid || selected[0]?.peer?.peerUid || declaredId)
    };
    const stamp = new Date().toISOString().replace(/[-:T.Z]/g, '').slice(0, 14);
    const outDir = body.exactOutputDir || body.outputPath || (body.outputDir ? path.join(body.outputDir, `${peer.typeName}_${safeName(peer.name)}_${safeName(declaredId)}_${stamp}_merged`) : path.join(state.exportsDir, `${peer.typeName}_${safeName(peer.name)}_${safeName(declaredId)}_${stamp}_merged`));
    ensureDir(outDir);
    updateTask(task, { progress: 70, message: '写入合并结果', messageCount: merged.length });
    const jsonl = await writeChunkedJsonlFromMessages(outDir, peer, merged, task);
    const stats = jsonl.stats;
    const seqStats = normalizedSeqRange(merged);
    const record = {
      id: task.id,
      createdAt: task.createdAt,
      completedAt: new Date().toISOString(),
      outputDir: outDir,
      peer,
      options: task.options,
      messageCount: merged.length,
      latestTimestamp: stats.timeRange.end ? new Date(stats.timeRange.end).getTime() : 0,
      earliestTimestamp: stats.timeRange.start ? new Date(stats.timeRange.start).getTime() : 0,
      latestSeq: seqStats.max,
      earliestSeq: seqStats.min,
      sourceHistoryIds: ids,
      sourceDirs,
      outputs: [jsonl.file],
      sessionType: peer.typeName,
      sessionId: declaredId,
      sessionName: peer.name,
      formats: ['jsonl'],
      latestTime: stats.timeRange.end || '',
      earliestTime: stats.timeRange.start || ''
    };
    registerExportHistory(record);
    updateTask(task, { status: 'completed', progress: 100, message: '合并完成', messageCount: merged.length, result: { ...record, paths: record.outputs } });
    pushTaskLog(task, `合并完成：输入=${lists.reduce((n, x) => n + x.length, 0)} 输出=${merged.length} 去重=${lists.reduce((n, x) => n + x.length, 0) - merged.length}`);
  } catch (err) {
    if (err?.message === 'STOP_REQUESTED') updateTask(task, { status: 'stopped', message: '已停止' });
    else {
      updateTask(task, { status: 'failed', message: err?.message || String(err), error: err?.stack || String(err) });
      pushTaskLog(task, `合并失败：${err?.message || err}`);
    }
  } finally {
    cleanupAfterExport(task);
  }
}

async function findRecordOrTaskOutput(id) {
  const task = state.tasks.get(String(id));
  if (task?.result?.outputDir) return task.result;
  return loadHistory().find(h => String(h.id) === String(id)) || null;
}

function sendJson(res, obj, code = 200) {
  const body = JSON.stringify(obj, jsonReplacer);
  res.writeHead(code, {
    'content-type': 'application/json; charset=utf-8',
    'access-control-allow-origin': '*',
    'access-control-allow-methods': 'GET,POST,OPTIONS',
    'access-control-allow-headers': 'content-type'
  });
  res.end(body);
}

function sendText(res, text, type = 'text/html; charset=utf-8') {
  res.writeHead(200, { 'content-type': type, 'access-control-allow-origin': '*' });
  res.end(text);
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    let data = '';
    req.on('data', chunk => { data += chunk; if (data.length > 20 * 1024 * 1024) req.destroy(new Error('body too large')); });
    req.on('end', () => {
      try { resolve(data ? JSON.parse(data) : {}); } catch (err) { reject(err); }
    });
    req.on('error', reject);
  });
}


function compatHistoryRecord(h) {
  const peer = h.peer || {};
  return {
    ...h,
    sessionType: h.sessionType || (peer.typeName === 'group' ? 'group' : 'private'),
    sessionId: h.sessionId || peer.uin || peer.peerUid || peer.uid || '',
    sessionName: h.sessionName || peer.name || '',
    formats: h.formats || [h.options?.format || 'json'],
    latestTime: h.latestTime || (h.latestTimestamp ? new Date(h.latestTimestamp).toISOString() : ''),
    earliestTime: h.earliestTime || (h.earliestTimestamp ? new Date(h.earliestTimestamp).toISOString() : '')
  };
}

async function handleApi(req, res, urlObj) {
  try {
    if (req.method === 'OPTIONS') return sendJson(res, {});
    if (urlObj.pathname === '/api/config') {
      return sendJson(res, { ok: true, defaultExportDir: state.exportsDir, defaultBaseUrl: `http://${LISTEN_HOST}:${state.port}`, version: VERSION, dataDir: state.dataDir, incrementalOverlapMs: DEFAULT_INCREMENTAL_OVERLAP_MS, incrementalOverlapSeq: DEFAULT_INCREMENTAL_OVERLAP_SEQ, chunkedFileApi: true });
    }
    if (urlObj.pathname === '/api/status') {
      return sendJson(res, { ok: true, version: VERSION, host: LISTEN_HOST, port: state.port, dataDir: state.dataDir, exportsDir: state.exportsDir, channel: getChannel(), buildVersion: getBuildVersion(), self: getSelfInfo(), chunkedFileApi: true });
    }
    if (urlObj.pathname === '/api/logs') {
      return sendJson(res, { logs: state.logs });
    }
    if (urlObj.pathname === '/api/sessions') {
      return sendJson(res, { ok: true, sessions: await getSessions(urlObj.searchParams.get('q') || '') });
    }
    if (urlObj.pathname === '/api/history') {
      const q = String(urlObj.searchParams.get('q') || '').trim().toLowerCase();
      let history = loadHistory().map(compatHistoryRecord).sort((a, b) => String(b.completedAt).localeCompare(String(a.completedAt)));
      if (q) history = history.filter(h => [h.sessionType, h.sessionId, h.sessionName, h.outputDir].some(v => String(v || '').toLowerCase().includes(q)));
      return sendJson(res, { ok: true, history });
    }
    if (urlObj.pathname === '/api/history/latest') {
      const sessionType = canonicalSessionType(urlObj.searchParams.get('sessionType') || urlObj.searchParams.get('chatType'));
      const sessionId = String(urlObj.searchParams.get('sessionId') || urlObj.searchParams.get('id') || '');
      const record = loadHistory().map(compatHistoryRecord)
        .filter(h => canonicalSessionType(h.sessionType) === sessionType && (!sessionId || String(h.sessionId) === sessionId))
        .sort((a, b) => String(b.completedAt).localeCompare(String(a.completedAt)))[0] || null;
      return sendJson(res, { ok: true, history: record });
    }
    const historyManifestMatch = urlObj.pathname.match(/^\/api\/history\/([^/]+)\/manifest$/);
    if (historyManifestMatch && req.method === 'GET') {
      const record = await findRecordOrTaskOutput(decodeURIComponent(historyManifestMatch[1]));
      if (!record?.outputDir) return sendJson(res, { ok: false, error: 'history not found' }, 404);
      return await sendChunkedOutputManifest(record, res);
    }
    const historyFileMatch = urlObj.pathname.match(/^\/api\/history\/([^/]+)\/files\/(.+)$/);
    if (historyFileMatch && req.method === 'GET') {
      const record = await findRecordOrTaskOutput(decodeURIComponent(historyFileMatch[1]));
      if (!record?.outputDir) return sendJson(res, { ok: false, error: 'history not found' }, 404);
      return await streamDeclaredOutputFile(record, decodeURIComponent(historyFileMatch[2]), res);
    }
    const historyMessagesMatch = urlObj.pathname.match(/^\/api\/history\/([^/]+)\/messages$/);
    if (historyMessagesMatch && req.method === 'GET') {
      const record = await findRecordOrTaskOutput(decodeURIComponent(historyMessagesMatch[1]));
      if (!record?.outputDir) return sendJson(res, { ok: false, error: 'history not found' }, 404);
      return await streamMessagesFromOutputDir(record.outputDir, res);
    }
    if (urlObj.pathname === '/api/tasks') {
      return sendJson(res, { tasks: Array.from(state.tasks.values()).map(t => ({ ...t, logs: t.logs.slice(-80) })) });
    }
    if (urlObj.pathname === '/api/task') {
      const id = urlObj.searchParams.get('id');
      const t = state.tasks.get(id);
      return sendJson(res, t ? { task: t } : { error: 'task not found' }, t ? 200 : 404);
    }
    const jobManifestMatch = urlObj.pathname.match(/^\/api\/jobs\/([^/]+)\/manifest$/);
    if (jobManifestMatch && req.method === 'GET') {
      const record = await findRecordOrTaskOutput(decodeURIComponent(jobManifestMatch[1]));
      if (!record?.outputDir) return sendJson(res, { ok: false, error: 'job result not ready' }, 409);
      return await sendChunkedOutputManifest(record, res);
    }
    const jobFileMatch = urlObj.pathname.match(/^\/api\/jobs\/([^/]+)\/files\/(.+)$/);
    if (jobFileMatch && req.method === 'GET') {
      const record = await findRecordOrTaskOutput(decodeURIComponent(jobFileMatch[1]));
      if (!record?.outputDir) return sendJson(res, { ok: false, error: 'job result not ready' }, 409);
      return await streamDeclaredOutputFile(record, decodeURIComponent(jobFileMatch[2]), res);
    }
    const jobMessagesMatch = urlObj.pathname.match(/^\/api\/jobs\/([^/]+)\/messages$/);
    if (jobMessagesMatch && req.method === 'GET') {
      const record = await findRecordOrTaskOutput(decodeURIComponent(jobMessagesMatch[1]));
      if (!record?.outputDir) return sendJson(res, { ok: false, error: 'job result not ready' }, 409);
      return await streamMessagesFromOutputDir(record.outputDir, res);
    }
    const jobMatch = urlObj.pathname.match(/^\/api\/jobs\/([^/]+)$/);
    if (jobMatch && req.method === 'GET') {
      const t = state.tasks.get(decodeURIComponent(jobMatch[1]));
      return sendJson(res, t ? { ok: true, job: t } : { ok: false, error: 'task not found' }, t ? 200 : 404);
    }
    const jobStopMatch = urlObj.pathname.match(/^\/api\/jobs\/([^/]+)\/stop$/);
    if (jobStopMatch && req.method === 'POST') {
      const t = state.tasks.get(decodeURIComponent(jobStopMatch[1]));
      if (!t) return sendJson(res, { ok: false, error: 'task not found' }, 404);
      t.stopRequested = true;
      pushTaskLog(t, '已请求提前停止；会在当前接口请求/资源下载批次结束后保存已获取内容');
      return sendJson(res, { ok: true });
    }
    if ((urlObj.pathname === '/api/export' || urlObj.pathname === '/api/sync') && req.method === 'POST') {
      const body = await readBody(req);
      if (urlObj.pathname === '/api/sync') {
        body.incremental = body.incremental ?? true;
        body.autoMergeIncremental = body.autoMergeIncremental ?? true;
        body.format = body.format || 'jsonl';
      }
      const task = createTask(body);
      return sendJson(res, { ok: true, taskId: task.id, jobId: task.id, task });
    }
    if (urlObj.pathname === '/api/merge' && req.method === 'POST') {
      const body = await readBody(req);
      const task = createMergeTask(body);
      return sendJson(res, { ok: true, taskId: task.id, jobId: task.id, task });
    }
    if (urlObj.pathname === '/api/stop' && req.method === 'POST') {
      const body = await readBody(req);
      const t = state.tasks.get(body.taskId || body.jobId);
      if (!t) return sendJson(res, { error: 'task not found' }, 404);
      t.stopRequested = true;
      pushTaskLog(t, '已请求提前停止；会在当前接口请求/资源下载批次结束后保存已获取内容');
      return sendJson(res, { ok: true });
    }

    if (urlObj.pathname === '/api/choose-folder' && req.method === 'POST') {
      const win = electron.BrowserWindow.getFocusedWindow();
      const ret = await electron.dialog.showOpenDialog(win || undefined, { properties: ['openDirectory', 'createDirectory'] });
      if (ret.canceled || !ret.filePaths || !ret.filePaths[0]) return sendJson(res, { ok: false, canceled: true });
      return sendJson(res, { ok: true, path: ret.filePaths[0] });
    }
    if (urlObj.pathname === '/api/open-folder' && req.method === 'POST') {
      const body = await readBody(req);
      const p = body.path || state.exportsDir;
      electron.shell.openPath(p);
      return sendJson(res, { ok: true });
    }
    if (urlObj.pathname === '/api/open-web' && req.method === 'POST') {
      openWebUi();
      return sendJson(res, { ok: true });
    }
    return sendJson(res, { error: 'not found' }, 404);
  } catch (err) {
    return sendJson(res, { ok: false, error: err?.message || String(err), stack: err?.stack }, 500);
  }
}

const HTML = Buffer.from('PCFkb2N0eXBlIGh0bWw+CjxodG1sIGxhbmc9InpoLUNOIj48aGVhZD48bWV0YSBjaGFyc2V0PSJ1dGYtOCI+PG1ldGEgbmFtZT0idmlld3BvcnQiIGNvbnRlbnQ9IndpZHRoPWRldmljZS13aWR0aCwgaW5pdGlhbC1zY2FsZT0xIj48dGl0bGU+cXFfZXhwb3J0PC90aXRsZT48c3R5bGU+Cjpyb290e2NvbG9yLXNjaGVtZTpsaWdodCBkYXJrfWJvZHl7Zm9udC1mYW1pbHk6LWFwcGxlLXN5c3RlbSxCbGlua01hY1N5c3RlbUZvbnQsIlNlZ29lIFVJIixzYW5zLXNlcmlmO21hcmdpbjowO2JhY2tncm91bmQ6Q2FudmFzO2NvbG9yOkNhbnZhc1RleHR9bWFpbnttYXgtd2lkdGg6MTE2MHB4O21hcmdpbjoyOHB4IGF1dG87cGFkZGluZzowIDE4cHggNDJweH1oMXtmb250LXNpemU6MjhweDttYXJnaW46MCAwIDE4cHh9aDJ7Zm9udC1zaXplOjE4cHg7bWFyZ2luOjAgMCAxNHB4fXNlY3Rpb257Ym9yZGVyOjFweCBzb2xpZCBjb2xvci1taXgoaW4gc3JnYixDYW52YXNUZXh0IDE4JSx0cmFuc3BhcmVudCk7Ym9yZGVyLXJhZGl1czoxNHB4O3BhZGRpbmc6MThweDttYXJnaW46MTZweCAwO2JhY2tncm91bmQ6Y29sb3ItbWl4KGluIHNyZ2IsQ2FudmFzIDk2JSxDYW52YXNUZXh0IDQlKX0udGFic3tkaXNwbGF5OmZsZXg7Z2FwOjhweDttYXJnaW46MCAwIDE2cHh9LnRhYnt3aWR0aDphdXRvO3BhZGRpbmc6MTBweCAxNnB4O2JvcmRlci1yYWRpdXM6OTk5cHh9LnRhYi5hY3RpdmV7YmFja2dyb3VuZDpIaWdobGlnaHQ7Y29sb3I6SGlnaGxpZ2h0VGV4dDtib3JkZXItY29sb3I6SGlnaGxpZ2h0O2ZvbnQtd2VpZ2h0OjYwMH0ucGFuZWx7ZGlzcGxheTpub25lfS5wYW5lbC5hY3RpdmV7ZGlzcGxheTpibG9ja30uZ3JpZHtkaXNwbGF5OmdyaWQ7Z3JpZC10ZW1wbGF0ZS1jb2x1bW5zOnJlcGVhdCgyLG1pbm1heCgwLDFmcikpO2dhcDoxMnB4fS5ncmlkM3tkaXNwbGF5OmdyaWQ7Z3JpZC10ZW1wbGF0ZS1jb2x1bW5zOnJlcGVhdCgzLG1pbm1heCgwLDFmcikpO2dhcDoxMnB4fS5ncmlkNHtkaXNwbGF5OmdyaWQ7Z3JpZC10ZW1wbGF0ZS1jb2x1bW5zOnJlcGVhdCg0LG1pbm1heCgwLDFmcikpO2dhcDoxMnB4fWxhYmVse2Rpc3BsYXk6YmxvY2s7Zm9udC1zaXplOjEzcHg7bWFyZ2luLWJvdHRvbTo1cHg7Y29sb3I6Y29sb3ItbWl4KGluIHNyZ2IsQ2FudmFzVGV4dCA3MiUsdHJhbnNwYXJlbnQpfWlucHV0LHNlbGVjdCxidXR0b257Ym94LXNpemluZzpib3JkZXItYm94O3dpZHRoOjEwMCU7Ym9yZGVyLXJhZGl1czoxMHB4O2JvcmRlcjoxcHggc29saWQgY29sb3ItbWl4KGluIHNyZ2IsQ2FudmFzVGV4dCAyMiUsdHJhbnNwYXJlbnQpO3BhZGRpbmc6MTBweCAxMXB4O2ZvbnQtc2l6ZToxNHB4O2JhY2tncm91bmQ6Q2FudmFzO2NvbG9yOkNhbnZhc1RleHR9YnV0dG9ue2N1cnNvcjpwb2ludGVyO2JhY2tncm91bmQ6Y29sb3ItbWl4KGluIHNyZ2IsSGlnaGxpZ2h0IDEyJSxDYW52YXMpO2JvcmRlci1jb2xvcjpjb2xvci1taXgoaW4gc3JnYixIaWdobGlnaHQgNTUlLENhbnZhc1RleHQgMTUlKX1idXR0b24ucHJpbWFyeXtiYWNrZ3JvdW5kOkhpZ2hsaWdodDtjb2xvcjpIaWdobGlnaHRUZXh0O2JvcmRlci1jb2xvcjpIaWdobGlnaHQ7Zm9udC13ZWlnaHQ6NjAwfWJ1dHRvbi5kYW5nZXJ7YmFja2dyb3VuZDpjb2xvci1taXgoaW4gc3JnYixyZWQgMTglLENhbnZhcyk7Ym9yZGVyLWNvbG9yOmNvbG9yLW1peChpbiBzcmdiLHJlZCA1NSUsQ2FudmFzVGV4dCAxNSUpfWJ1dHRvbjpkaXNhYmxlZHtvcGFjaXR5Oi41NTtjdXJzb3I6bm90LWFsbG93ZWR9LnJvd3tkaXNwbGF5OmZsZXg7Z2FwOjEwcHg7YWxpZ24taXRlbXM6ZW5kfS5yb3c+KntmbGV4OjF9LmNoZWNrc3tkaXNwbGF5OmZsZXg7ZmxleC13cmFwOndyYXA7Z2FwOjE0cHg7YWxpZ24taXRlbXM6Y2VudGVyO21hcmdpbi10b3A6MTBweH0uY2hlY2tzIGxhYmVse2Rpc3BsYXk6ZmxleDthbGlnbi1pdGVtczpjZW50ZXI7Z2FwOjZweDttYXJnaW46MH1pbnB1dFt0eXBlPWNoZWNrYm94XXt3aWR0aDphdXRvfXRhYmxle3dpZHRoOjEwMCU7Ym9yZGVyLWNvbGxhcHNlOmNvbGxhcHNlO2ZvbnQtc2l6ZToxM3B4fXRoLHRke2JvcmRlci1ib3R0b206MXB4IHNvbGlkIGNvbG9yLW1peChpbiBzcmdiLENhbnZhc1RleHQgMTIlLHRyYW5zcGFyZW50KTtwYWRkaW5nOjlweCA3cHg7dGV4dC1hbGlnbjpsZWZ0O3ZlcnRpY2FsLWFsaWduOnRvcH10Ym9keSB0ci5zZWxlY3RhYmxle2N1cnNvcjpwb2ludGVyfXRib2R5IHRyLnNlbGVjdGFibGU6aG92ZXJ7YmFja2dyb3VuZDpjb2xvci1taXgoaW4gc3JnYixIaWdobGlnaHQgMTAlLHRyYW5zcGFyZW50KX10Ym9keSB0ci5zZWxlY3RlZHtiYWNrZ3JvdW5kOmNvbG9yLW1peChpbiBzcmdiLEhpZ2hsaWdodCAyMiUsdHJhbnNwYXJlbnQpfWNvZGUscHJle2ZvbnQtZmFtaWx5OnVpLW1vbm9zcGFjZSxTRk1vbm8tUmVndWxhcixNZW5sbyxDb25zb2xhcyxtb25vc3BhY2V9cHJle2JhY2tncm91bmQ6Y29sb3ItbWl4KGluIHNyZ2IsQ2FudmFzVGV4dCA4JSx0cmFuc3BhcmVudCk7cGFkZGluZzoxMnB4O2JvcmRlci1yYWRpdXM6MTJweDtvdmVyZmxvdzphdXRvO21heC1oZWlnaHQ6MzIwcHh9cHJvZ3Jlc3N7d2lkdGg6MTAwJTtoZWlnaHQ6MThweH0uc3RhdHVze2Rpc3BsYXk6Z3JpZDtncmlkLXRlbXBsYXRlLWNvbHVtbnM6MWZyIGF1dG87Z2FwOjEwcHg7YWxpZ24taXRlbXM6Y2VudGVyfS5zbWFsbHtmb250LXNpemU6MTJweDtjb2xvcjpjb2xvci1taXgoaW4gc3JnYixDYW52YXNUZXh0IDY1JSx0cmFuc3BhcmVudCl9LnBpbGx7ZGlzcGxheTppbmxpbmUtYmxvY2s7cGFkZGluZzoycHggOHB4O2JvcmRlci1yYWRpdXM6OTk5cHg7YmFja2dyb3VuZDpjb2xvci1taXgoaW4gc3JnYixIaWdobGlnaHQgMTIlLHRyYW5zcGFyZW50KTttYXJnaW4tcmlnaHQ6NnB4O2ZvbnQtc2l6ZToxMnB4fS5hY3Rpb25ze2Rpc3BsYXk6ZmxleDtnYXA6OHB4O2ZsZXgtd3JhcDp3cmFwfS5hY3Rpb25zIGJ1dHRvbnt3aWR0aDphdXRvO3BhZGRpbmc6N3B4IDEwcHh9Lmhpc3RvcnktdG9vbHN7ZGlzcGxheTpncmlkO2dyaWQtdGVtcGxhdGUtY29sdW1uczoxZnIgYXV0bztnYXA6MTBweDthbGlnbi1pdGVtczplbmR9Lmhpc3RvcnktbGlzdHtkaXNwbGF5OmdyaWQ7Z2FwOjEwcHh9Lmhpc3RvcnktY2FyZHtib3JkZXI6MXB4IHNvbGlkIGNvbG9yLW1peChpbiBzcmdiLENhbnZhc1RleHQgMTQlLHRyYW5zcGFyZW50KTtib3JkZXItcmFkaXVzOjEycHg7cGFkZGluZzoxMnB4O2Rpc3BsYXk6Z3JpZDtncmlkLXRlbXBsYXRlLWNvbHVtbnM6bWlubWF4KDAsMS4yZnIpIDExMHB4IG1pbm1heCgwLDFmcikgYXV0bztnYXA6MTJweDthbGlnbi1pdGVtczpjZW50ZXJ9Lmhpc3RvcnktY2FyZCBzdHJvbmd7ZGlzcGxheTpibG9jazttYXJnaW4tYm90dG9tOjRweH0uaGlzdG9yeS1jYXJkIGNvZGV7d29yZC1icmVhazpicmVhay1hbGx9QG1lZGlhKG1heC13aWR0aDo4NjBweCl7LmdyaWQsLmdyaWQzLC5ncmlkNCwuaGlzdG9yeS1jYXJke2dyaWQtdGVtcGxhdGUtY29sdW1uczoxZnJ9LnJvd3tmbGV4LWRpcmVjdGlvbjpjb2x1bW47YWxpZ24taXRlbXM6c3RyZXRjaH0uc3RhdHVze2dyaWQtdGVtcGxhdGUtY29sdW1uczoxZnJ9fQo8L3N0eWxlPjwvaGVhZD48Ym9keT48bWFpbj48aDE+cXFfZXhwb3J0PC9oMT48ZGl2IGNsYXNzPSJ0YWJzIj48YnV0dG9uIGNsYXNzPSJ0YWIgYWN0aXZlIiBkYXRhLXRhYj0iZXhwb3J0UGFuZWwiPuaWsOW7uuWvvOWHujwvYnV0dG9uPjxidXR0b24gY2xhc3M9InRhYiIgZGF0YS10YWI9Imhpc3RvcnlQYW5lbCI+5Y6G5Y+y5a+85Ye6PC9idXR0b24+PC9kaXY+PGRpdiBpZD0iZXhwb3J0UGFuZWwiIGNsYXNzPSJwYW5lbCBhY3RpdmUiPjxzZWN0aW9uPjxoMj7kvJror508L2gyPjxkaXYgY2xhc3M9InJvdyI+PGRpdj48bGFiZWw+5pCc57SiIFFRL+e+pOWPt+OAgeeUqOaIty/nvqTlkI08L2xhYmVsPjxpbnB1dCBpZD0icXVlcnkiIHBsYWNlaG9sZGVyPSLnlZnnqbrlj6/miYvliqjovpPlhaUiPjwvZGl2PjxidXR0b24gaWQ9ImxvYWRTZXNzaW9ucyIgc3R5bGU9Im1heC13aWR0aDoxODBweCI+6K+75Y+W5Lya6K+d5YiX6KGoPC9idXR0b24+PC9kaXY+PGRpdiBjbGFzcz0iZ3JpZDMiIHN0eWxlPSJtYXJnaW4tdG9wOjEycHgiPjxkaXY+PGxhYmVsPuexu+WeizwvbGFiZWw+PHNlbGVjdCBpZD0ic2Vzc2lvblR5cGUiPjxvcHRpb24gdmFsdWU9InByaXZhdGUiPuengeiBijwvb3B0aW9uPjxvcHRpb24gdmFsdWU9Imdyb3VwIj7nvqTogYo8L29wdGlvbj48L3NlbGVjdD48L2Rpdj48ZGl2PjxsYWJlbD5RUS/nvqTlj7cg5oiWIFVJRDwvbGFiZWw+PGlucHV0IGlkPSJzZXNzaW9uSWQiIHBsYWNlaG9sZGVyPSLmiYvliqjovpPlhaUiPjwvZGl2PjxkaXY+PGxhYmVsPuaYvuekuuWQjeensDwvbGFiZWw+PGlucHV0IGlkPSJzZXNzaW9uTmFtZSIgcGxhY2Vob2xkZXI9IuWPr+mAiSI+PC9kaXY+PC9kaXY+PGRpdiBpZD0ic2Vzc2lvbkJveCIgc3R5bGU9Im1hcmdpbi10b3A6MTJweDtkaXNwbGF5Om5vbmUiPjx0YWJsZT48dGhlYWQ+PHRyPjx0aD7nsbvlnos8L3RoPjx0aD7lj7fnoIE8L3RoPjx0aD5VSUQ8L3RoPjx0aD7lkI3np7A8L3RoPjwvdHI+PC90aGVhZD48dGJvZHkgaWQ9InNlc3Npb25zIj48L3Rib2R5PjwvdGFibGU+PC9kaXY+PC9zZWN0aW9uPjxzZWN0aW9uPjxoMj7lr7zlh7rpgInpobk8L2gyPjxkaXYgY2xhc3M9ImdyaWQ0Ij48ZGl2PjxsYWJlbD7moLzlvI88L2xhYmVsPjxzZWxlY3QgaWQ9ImZvcm1hdCI+PG9wdGlvbiB2YWx1ZT0ianNvbiI+SlNPTjwvb3B0aW9uPjxvcHRpb24gdmFsdWU9Impzb25sIj5KU09OTDwvb3B0aW9uPjwvc2VsZWN0PjwvZGl2PjxkaXY+PGxhYmVsPuaXtumXtOiMg+WbtDwvbGFiZWw+PHNlbGVjdCBpZD0idGltZU1vZGUiPjxvcHRpb24gdmFsdWU9Im1hbnVhbCI+5omL5Yqo6L6T5YWlPC9vcHRpb24+PG9wdGlvbiB2YWx1ZT0iYWxsIj7lhajpg6g8L29wdGlvbj48b3B0aW9uIHZhbHVlPSIxZCI+5pyA6L+RIDEg5aSpPC9vcHRpb24+PG9wdGlvbiB2YWx1ZT0iMXciPuacgOi/kSAxIOWRqDwvb3B0aW9uPjxvcHRpb24gdmFsdWU9IjFtIj7mnIDov5EgMSDmnIg8L29wdGlvbj48b3B0aW9uIHZhbHVlPSIxeSI+5pyA6L+RIDEg5bm0PC9vcHRpb24+PC9zZWxlY3Q+PC9kaXY+PGRpdj48bGFiZWw+5q+P5om55raI5oGv5p2h5pWwPC9sYWJlbD48aW5wdXQgaWQ9ImJhdGNoU2l6ZSIgdHlwZT0ibnVtYmVyIiBtaW49IjIwIiBtYXg9IjIwMDAiIHZhbHVlPSIxMDAwIj48L2Rpdj48ZGl2PjxsYWJlbD7mnIDlpKfmtojmga/mlbA8L2xhYmVsPjxpbnB1dCBpZD0ibWF4TWVzc2FnZXMiIHR5cGU9Im51bWJlciIgbWluPSIwIiB2YWx1ZT0iMCI+PC9kaXY+PC9kaXY+PGRpdiBpZD0ibWFudWFsVGltZVJvdyIgY2xhc3M9ImdyaWQiIHN0eWxlPSJtYXJnaW4tdG9wOjEycHgiPjxkaXY+PGxhYmVsPuW8gOWni+aXtumXtDwvbGFiZWw+PGlucHV0IGlkPSJzdGFydFRpbWUiIHR5cGU9ImRhdGV0aW1lLWxvY2FsIj48L2Rpdj48ZGl2PjxsYWJlbD7nu5PmnZ/ml7bpl7Q8L2xhYmVsPjxpbnB1dCBpZD0iZW5kVGltZSIgdHlwZT0iZGF0ZXRpbWUtbG9jYWwiPjwvZGl2PjwvZGl2PjxkaXYgY2xhc3M9ImdyaWQzIiBzdHlsZT0ibWFyZ2luLXRvcDoxMnB4Ij48ZGl2PjxsYWJlbD7ljoblj7Llubblj5Hnur/nqIvmlbA8L2xhYmVsPjxpbnB1dCBpZD0iaGlzdG9yeVdvcmtlcnMiIHR5cGU9Im51bWJlciIgbWluPSIxIiBtYXg9IjQiIHZhbHVlPSI0Ij48L2Rpdj48ZGl2PjxsYWJlbD5TZXEg56qX5Y+j5aSn5bCPPC9sYWJlbD48aW5wdXQgaWQ9InNlcVdpbmRvd1NpemUiIHR5cGU9Im51bWJlciIgbWluPSIxMDAwIiB2YWx1ZT0iNTAwMDAiPjwvZGl2PjxkaXY+PGxhYmVsPuWksei0pemHjeivleasoeaVsDwvbGFiZWw+PGlucHV0IGlkPSJoaXN0b3J5UmV0cnlDb3VudCIgdHlwZT0ibnVtYmVyIiBtaW49IjUiIG1heD0iMjAiIHZhbHVlPSI1Ij48L2Rpdj48L2Rpdj48ZGl2IGNsYXNzPSJyb3ciIHN0eWxlPSJtYXJnaW4tdG9wOjEycHgiPjxkaXY+PGxhYmVsPuWvvOWHuuaWh+S7tuWkuTwvbGFiZWw+PGlucHV0IGlkPSJvdXRwdXREaXIiIHBsYWNlaG9sZGVyPSLnlZnnqbrkvb/nlKjpu5jorqTnm67lvZUiPjwvZGl2PjxidXR0b24gaWQ9ImNob29zZU91dHB1dERpciIgc3R5bGU9Im1heC13aWR0aDoxNTBweCI+6YCJ5oup5paH5Lu25aS5PC9idXR0b24+PC9kaXY+PGRpdiBjbGFzcz0iY2hlY2tzIj48bGFiZWw+PGlucHV0IGlkPSJpbmNsdWRlU3lzdGVtTWVzc2FnZXMiIHR5cGU9ImNoZWNrYm94IiBjaGVja2VkPiDlr7zlh7rns7vnu5/mtojmga88L2xhYmVsPjxsYWJlbD48aW5wdXQgaWQ9ImluY2x1ZGVSZWNhbGxlZE1lc3NhZ2VzIiB0eXBlPSJjaGVja2JveCI+IOWvvOWHuuW3suaSpOWbnueahOa2iOaBrzwvbGFiZWw+PGxhYmVsPjxpbnB1dCBpZD0icGFyYWxsZWxIaXN0b3J5IiB0eXBlPSJjaGVja2JveCIgY2hlY2tlZD4g5bm26KGM6K+75Y+W5Y6G5Y+yPC9sYWJlbD48bGFiZWw+PGlucHV0IGlkPSJpbmNsdWRlUmVzb3VyY2VzIiB0eXBlPSJjaGVja2JveCI+IOi1hOa6kOaWh+S7tjwvbGFiZWw+PC9kaXY+PGRpdiBjbGFzcz0iYWN0aW9ucyIgc3R5bGU9Im1hcmdpbi10b3A6MTZweCI+PGJ1dHRvbiBpZD0ic3RhcnRFeHBvcnQiIGNsYXNzPSJwcmltYXJ5Ij7lvIDlp4vlr7zlh7o8L2J1dHRvbj48YnV0dG9uIGlkPSJzdG9wRXhwb3J0IiBjbGFzcz0iZGFuZ2VyIiBkaXNhYmxlZD7mj5DliY3lgZzmraLlubbkv53lrZg8L2J1dHRvbj48L2Rpdj48L3NlY3Rpb24+PHNlY3Rpb24+PGgyPueKtuaAgTwvaDI+PGRpdiBjbGFzcz0ic3RhdHVzIj48cHJvZ3Jlc3MgaWQ9InByb2dyZXNzIiB2YWx1ZT0iMCIgbWF4PSIxMDAiPjwvcHJvZ3Jlc3M+PHN0cm9uZyBpZD0ic3RhdHVzVGV4dCI+aWRsZSAwJTwvc3Ryb25nPjwvZGl2PjxwIGlkPSJtZXNzYWdlIiBjbGFzcz0ic21hbGwiPjwvcD48cHJlIGlkPSJsb2dzIj48L3ByZT48ZGl2IGlkPSJyZXN1bHQiPjwvZGl2Pjwvc2VjdGlvbj48L2Rpdj48ZGl2IGlkPSJoaXN0b3J5UGFuZWwiIGNsYXNzPSJwYW5lbCI+PHNlY3Rpb24+PGgyPuWOhuWPsuWvvOWHujwvaDI+PGRpdiBjbGFzcz0iaGlzdG9yeS10b29scyI+PGRpdj48bGFiZWw+562b6YCJPC9sYWJlbD48aW5wdXQgaWQ9Imhpc3RvcnlRdWVyeSIgcGxhY2Vob2xkZXI9IuS8muivneWQjeOAgeWPt+eggeaIlui3r+W+hCI+PC9kaXY+PGJ1dHRvbiBpZD0ibG9hZEhpc3RvcnkiPuWIt+aWsDwvYnV0dG9uPjwvZGl2PjxkaXYgaWQ9Imhpc3RvcnlSb3dzIiBjbGFzcz0iaGlzdG9yeS1saXN0IiBzdHlsZT0ibWFyZ2luLXRvcDoxMnB4Ij48L2Rpdj48L3NlY3Rpb24+PC9kaXY+PC9tYWluPjxzY3JpcHQ+CmNvbnN0ICQ9aWQ9PmRvY3VtZW50LmdldEVsZW1lbnRCeUlkKGlkKTtsZXQgc2VsZWN0ZWRTZXNzaW9uPW51bGwsc2VsZWN0ZWRIaXN0b3J5PW51bGwsY3VycmVudEpvYj1udWxsLHRpbWVyPW51bGw7YXN5bmMgZnVuY3Rpb24gYXBpKHBhdGgsb3B0cz17fSl7Y29uc3Qgcj1hd2FpdCBmZXRjaChwYXRoLG9wdHMpO2NvbnN0IHQ9YXdhaXQgci50ZXh0KCk7bGV0IGo9e307dHJ5e2o9dD9KU09OLnBhcnNlKHQpOnt9fWNhdGNoe3Rocm93IG5ldyBFcnJvcih0KX1pZighci5va3x8ai5lcnJvcnx8ai5vaz09PWZhbHNlKXRocm93IG5ldyBFcnJvcihqLmVycm9yfHxqLm1lc3NhZ2V8fHIuc3RhdHVzVGV4dCk7cmV0dXJuIGp9ZG9jdW1lbnQucXVlcnlTZWxlY3RvckFsbCgnLnRhYicpLmZvckVhY2goYj0+Yi5vbmNsaWNrPSgpPT5zZXRUYWIoYi5kYXRhc2V0LnRhYikpO2Z1bmN0aW9uIHNldFRhYihpZCl7ZG9jdW1lbnQucXVlcnlTZWxlY3RvckFsbCgnLnRhYicpLmZvckVhY2goeD0+eC5jbGFzc0xpc3QudG9nZ2xlKCdhY3RpdmUnLHguZGF0YXNldC50YWI9PT1pZCkpO2RvY3VtZW50LnF1ZXJ5U2VsZWN0b3JBbGwoJy5wYW5lbCcpLmZvckVhY2goeD0+eC5jbGFzc0xpc3QudG9nZ2xlKCdhY3RpdmUnLHguaWQ9PT1pZCkpO2lmKGlkPT09J2hpc3RvcnlQYW5lbCcpbG9hZEhpc3RvcnkoKX0kKCd0aW1lTW9kZScpLm9uY2hhbmdlPSgpPT57JCgnbWFudWFsVGltZVJvdycpLnN0eWxlLmRpc3BsYXk9JCgndGltZU1vZGUnKS52YWx1ZT09PSdtYW51YWwnPydncmlkJzonbm9uZSd9OyQoJ2Nob29zZU91dHB1dERpcicpLm9uY2xpY2s9YXN5bmMoKT0+e3RyeXtjb25zdCByPWF3YWl0IGFwaSgnL2FwaS9jaG9vc2UtZm9sZGVyJyx7bWV0aG9kOidQT1NUJ30pO2lmKHIucGF0aCkkKCdvdXRwdXREaXInKS52YWx1ZT1yLnBhdGh9Y2F0Y2goZSl7YWxlcnQoZS5tZXNzYWdlKX19OyQoJ2xvYWRTZXNzaW9ucycpLm9uY2xpY2s9YXN5bmMoKT0+eyQoJ3Nlc3Npb25zJykuaW5uZXJIVE1MPSc8dHI+PHRkIGNvbHNwYW49IjQiPuivu+WPluS4rS4uLjwvdGQ+PC90cj4nOyQoJ3Nlc3Npb25Cb3gnKS5zdHlsZS5kaXNwbGF5PSdibG9jayc7dHJ5e2NvbnN0IGRhdGE9YXdhaXQgYXBpKCcvYXBpL3Nlc3Npb25zP3E9JytlbmNvZGVVUklDb21wb25lbnQoJCgncXVlcnknKS52YWx1ZXx8JycpKTskKCdzZXNzaW9ucycpLmlubmVySFRNTD0nJztmb3IoY29uc3QgcyBvZiBkYXRhLnNlc3Npb25zKXtjb25zdCB0cj1kb2N1bWVudC5jcmVhdGVFbGVtZW50KCd0cicpO3RyLmNsYXNzTmFtZT0nc2VsZWN0YWJsZSc7dHIuaW5uZXJIVE1MPWA8dGQ+JHtlc2NhcGVIdG1sKHMudHlwZSl9PC90ZD48dGQ+PGNvZGU+JHtlc2NhcGVIdG1sKHMudWlufHxzLmlkfHwnJyl9PC9jb2RlPjwvdGQ+PHRkPjxjb2RlPiR7ZXNjYXBlSHRtbChzLnVpZHx8JycpfTwvY29kZT48L3RkPjx0ZD4ke2VzY2FwZUh0bWwocy5uYW1lfHwnJyl9PC90ZD5gO3RyLm9uY2xpY2s9KCk9Pntkb2N1bWVudC5xdWVyeVNlbGVjdG9yQWxsKCcjc2Vzc2lvbnMgdHInKS5mb3JFYWNoKHg9PnguY2xhc3NMaXN0LnJlbW92ZSgnc2VsZWN0ZWQnKSk7dHIuY2xhc3NMaXN0LmFkZCgnc2VsZWN0ZWQnKTtmaWxsU2Vzc2lvbihzKX07JCgnc2Vzc2lvbnMnKS5hcHBlbmRDaGlsZCh0cil9aWYoIWRhdGEuc2Vzc2lvbnMubGVuZ3RoKSQoJ3Nlc3Npb25zJykuaW5uZXJIVE1MPSc8dHI+PHRkIGNvbHNwYW49IjQiPuayoeacieWMuemFjee7k+aenO+8jOWPr+ebtOaOpeaJi+WKqOi+k+WFpeOAgjwvdGQ+PC90cj4nfWNhdGNoKGUpeyQoJ3Nlc3Npb25zJykuaW5uZXJIVE1MPWA8dHI+PHRkIGNvbHNwYW49IjQiPiR7ZXNjYXBlSHRtbChlLm1lc3NhZ2UpfTwvdGQ+PC90cj5gfX07ZnVuY3Rpb24gZmlsbFNlc3Npb24ocyl7c2VsZWN0ZWRTZXNzaW9uPXM7JCgnc2Vzc2lvblR5cGUnKS52YWx1ZT1zLnR5cGU9PT0nZ3JvdXAnPydncm91cCc6J3ByaXZhdGUnOyQoJ3Nlc3Npb25JZCcpLnZhbHVlPXMudWlufHxzLmlkfHxzLnVpZHx8Jyc7JCgnc2Vzc2lvbk5hbWUnKS52YWx1ZT1zLm5hbWV8fHMuaWR8fCcnfSQoJ2xvYWRIaXN0b3J5Jykub25jbGljaz1sb2FkSGlzdG9yeTthc3luYyBmdW5jdGlvbiBsb2FkSGlzdG9yeSgpeyQoJ2hpc3RvcnlSb3dzJykuaW5uZXJIVE1MPSc8ZGl2IGNsYXNzPSJoaXN0b3J5LWNhcmQiPuivu+WPluS4rS4uLjwvZGl2Pic7dHJ5e2NvbnN0IHE9ZW5jb2RlVVJJQ29tcG9uZW50KCQoJ2hpc3RvcnlRdWVyeScpLnZhbHVlfHwnJyk7Y29uc3QgZGF0YT1hd2FpdCBhcGkoJy9hcGkvaGlzdG9yeT9xPScrcSk7JCgnaGlzdG9yeVJvd3MnKS5pbm5lckhUTUw9Jyc7Zm9yKGNvbnN0IHJlYyBvZiBkYXRhLmhpc3RvcnkpYWRkSGlzdG9yeUNhcmQocmVjKTtpZighZGF0YS5oaXN0b3J5Lmxlbmd0aCkkKCdoaXN0b3J5Um93cycpLmlubmVySFRNTD0nPGRpdiBjbGFzcz0iaGlzdG9yeS1jYXJkIj7msqHmnInljoblj7LorrDlvZXjgII8L2Rpdj4nfWNhdGNoKGUpeyQoJ2hpc3RvcnlSb3dzJykuaW5uZXJIVE1MPWA8ZGl2IGNsYXNzPSJoaXN0b3J5LWNhcmQiPiR7ZXNjYXBlSHRtbChlLm1lc3NhZ2UpfTwvZGl2PmB9fWZ1bmN0aW9uIGFkZEhpc3RvcnlDYXJkKHJlYyl7Y29uc3QgZGl2PWRvY3VtZW50LmNyZWF0ZUVsZW1lbnQoJ2RpdicpO2Rpdi5jbGFzc05hbWU9J2hpc3RvcnktY2FyZCc7Y29uc3QgdGFncz1bcmVjLnNlc3Npb25UeXBlLC4uLihyZWMuZm9ybWF0c3x8W10pXS5maWx0ZXIoQm9vbGVhbikubWFwKHg9PmA8c3BhbiBjbGFzcz0icGlsbCI+JHtlc2NhcGVIdG1sKHgpfTwvc3Bhbj5gKS5qb2luKCcnKTtkaXYuaW5uZXJIVE1MPWA8ZGl2PiR7dGFnc308c3Ryb25nPiR7ZXNjYXBlSHRtbChyZWMuc2Vzc2lvbk5hbWV8fCco5pyq5ZG95ZCNKScpfTwvc3Ryb25nPjxjb2RlPiR7ZXNjYXBlSHRtbChyZWMuc2Vzc2lvbklkfHwnJyl9PC9jb2RlPjwvZGl2PjxkaXY+JHtOdW1iZXIocmVjLm1lc3NhZ2VDb3VudHx8MCkudG9Mb2NhbGVTdHJpbmcoKX0g5p2hPC9kaXY+PGRpdj48Y29kZT4ke2VzY2FwZUh0bWwocmVjLmxhdGVzdFRpbWV8fCcnKX08L2NvZGU+PGJyPjxjb2RlPiR7ZXNjYXBlSHRtbChyZWMub3V0cHV0RGlyfHwnJyl9PC9jb2RlPjwvZGl2PjxkaXYgY2xhc3M9ImFjdGlvbnMiPjxidXR0b24gZGF0YS1hY3Q9ImZpbGwiPuWhq+WFpTwvYnV0dG9uPjxidXR0b24gZGF0YS1hY3Q9ImluYyI+5aKe6YeP5pu05pawPC9idXR0b24+PGJ1dHRvbiBkYXRhLWFjdD0ib3BlbiI+5omT5byA55uu5b2VPC9idXR0b24+PC9kaXY+YDtkaXYucXVlcnlTZWxlY3RvcignW2RhdGEtYWN0PWZpbGxdJykub25jbGljaz0oKT0+e3NlbGVjdGVkSGlzdG9yeT1yZWM7c2VsZWN0ZWRTZXNzaW9uPW51bGw7JCgnc2Vzc2lvblR5cGUnKS52YWx1ZT1yZWMuc2Vzc2lvblR5cGU9PT0nZ3JvdXAnPydncm91cCc6J3ByaXZhdGUnOyQoJ3Nlc3Npb25JZCcpLnZhbHVlPXJlYy5zZXNzaW9uSWR8fCcnOyQoJ3Nlc3Npb25OYW1lJykudmFsdWU9cmVjLnNlc3Npb25OYW1lfHwnJztzZXRUYWIoJ2V4cG9ydFBhbmVsJyl9O2Rpdi5xdWVyeVNlbGVjdG9yKCdbZGF0YS1hY3Q9aW5jXScpLm9uY2xpY2s9KCk9PntzZWxlY3RlZEhpc3Rvcnk9cmVjO3NlbGVjdGVkU2Vzc2lvbj1udWxsOyQoJ3Nlc3Npb25UeXBlJykudmFsdWU9cmVjLnNlc3Npb25UeXBlPT09J2dyb3VwJz8nZ3JvdXAnOidwcml2YXRlJzskKCdzZXNzaW9uSWQnKS52YWx1ZT1yZWMuc2Vzc2lvbklkfHwnJzskKCdzZXNzaW9uTmFtZScpLnZhbHVlPXJlYy5zZXNzaW9uTmFtZXx8Jyc7c2V0VGFiKCdleHBvcnRQYW5lbCcpO3N0YXJ0RXhwb3J0KHRydWUpfTtkaXYucXVlcnlTZWxlY3RvcignW2RhdGEtYWN0PW9wZW5dJykub25jbGljaz0oKT0+YXBpKCcvYXBpL29wZW4tZm9sZGVyJyx7bWV0aG9kOidQT1NUJyxoZWFkZXJzOnsnQ29udGVudC1UeXBlJzonYXBwbGljYXRpb24vanNvbid9LGJvZHk6SlNPTi5zdHJpbmdpZnkoe3BhdGg6cmVjLm91dHB1dERpcn0pfSkuY2F0Y2goZT0+YWxlcnQoZS5tZXNzYWdlKSk7JCgnaGlzdG9yeVJvd3MnKS5hcHBlbmRDaGlsZChkaXYpfSQoJ3N0YXJ0RXhwb3J0Jykub25jbGljaz0oKT0+c3RhcnRFeHBvcnQoZmFsc2UpO2FzeW5jIGZ1bmN0aW9uIHN0YXJ0RXhwb3J0KGluY3JlbWVudGFsKXtjb25zdCB1c2VJbmM9ISFpbmNyZW1lbnRhbDtpZih1c2VJbmMmJighc2VsZWN0ZWRIaXN0b3J5fHwhc2VsZWN0ZWRIaXN0b3J5LmxhdGVzdFRpbWVzdGFtcCkpe2FsZXJ0KCfmsqHmnInlj6/nlKjnmoTlop7ph4/ln7rlh4bjgIInKTtyZXR1cm59Y29uc3QgcGF5bG9hZD17Y2hhdFR5cGU6JCgnc2Vzc2lvblR5cGUnKS52YWx1ZSxpZDokKCdzZXNzaW9uSWQnKS52YWx1ZSxzZXNzaW9uSWQ6JCgnc2Vzc2lvbklkJykudmFsdWUscGVlclVpZDpzZWxlY3RlZFNlc3Npb24/LnVpZHx8JycsdWluOnNlbGVjdGVkU2Vzc2lvbj8udWlufHwkKCdzZXNzaW9uSWQnKS52YWx1ZSxuYW1lOiQoJ3Nlc3Npb25OYW1lJykudmFsdWUsc2Vzc2lvbk5hbWU6JCgnc2Vzc2lvbk5hbWUnKS52YWx1ZSxmb3JtYXQ6JCgnZm9ybWF0JykudmFsdWUsdGltZVByZXNldDp1c2VJbmM/J2FsbCc6JCgndGltZU1vZGUnKS52YWx1ZSxmcm9tOiQoJ3N0YXJ0VGltZScpLnZhbHVlLHRvOiQoJ2VuZFRpbWUnKS52YWx1ZSxpbmNyZW1lbnRhbDp1c2VJbmMsaW5jcmVtZW50YWxCYXNlSWQ6dXNlSW5jP3NlbGVjdGVkSGlzdG9yeS5pZDpudWxsLGluY3JlbWVudGFsU3RhcnRUaW1lc3RhbXA6dXNlSW5jP3NlbGVjdGVkSGlzdG9yeS5sYXRlc3RUaW1lc3RhbXA6bnVsbCxiYXRjaENvdW50Ok51bWJlcigkKCdiYXRjaFNpemUnKS52YWx1ZXx8MTAwMCksbWF4TWVzc2FnZXM6TnVtYmVyKCQoJ21heE1lc3NhZ2VzJykudmFsdWV8fDApLHJlc291cmNlV29ya2VyczozLHBhcmFsbGVsSGlzdG9yeTokKCdwYXJhbGxlbEhpc3RvcnknKS5jaGVja2VkLGhpc3RvcnlXb3JrZXJzOk51bWJlcigkKCdoaXN0b3J5V29ya2VycycpLnZhbHVlfHw0KSxzZXFXaW5kb3c6TnVtYmVyKCQoJ3NlcVdpbmRvd1NpemUnKS52YWx1ZXx8NTAwMDApLG91dHB1dERpcjokKCdvdXRwdXREaXInKS52YWx1ZSxpbmNsdWRlU3lzdGVtOiQoJ2luY2x1ZGVTeXN0ZW1NZXNzYWdlcycpLmNoZWNrZWQsaW5jbHVkZVJlY2FsbGVkOiQoJ2luY2x1ZGVSZWNhbGxlZE1lc3NhZ2VzJykuY2hlY2tlZCxleHBvcnRSZXNvdXJjZXM6JCgnaW5jbHVkZVJlc291cmNlcycpLmNoZWNrZWQsaGlzdG9yeVJldHJ5Q291bnQ6TnVtYmVyKCQoJ2hpc3RvcnlSZXRyeUNvdW50JykudmFsdWV8fDUpfTtpZighU3RyaW5nKHBheWxvYWQuaWR8fHBheWxvYWQucGVlclVpZCkudHJpbSgpKXthbGVydCgn6K+35omL5Yqo6L6T5YWl5oiW6YCJ5oupIFFRL+e+pOWPt+OAgicpO3JldHVybn0kKCdyZXN1bHQnKS5pbm5lckhUTUw9Jyc7JCgnbG9ncycpLnRleHRDb250ZW50PScnOyQoJ3Byb2dyZXNzJykudmFsdWU9MDskKCdzdGF0dXNUZXh0JykudGV4dENvbnRlbnQ9J3BlbmRpbmcgMCUnOyQoJ3N0YXJ0RXhwb3J0JykuZGlzYWJsZWQ9dHJ1ZTskKCdzdG9wRXhwb3J0JykuZGlzYWJsZWQ9ZmFsc2U7dHJ5e2NvbnN0IHI9YXdhaXQgYXBpKCcvYXBpL2V4cG9ydCcse21ldGhvZDonUE9TVCcsaGVhZGVyczp7J0NvbnRlbnQtVHlwZSc6J2FwcGxpY2F0aW9uL2pzb24nfSxib2R5OkpTT04uc3RyaW5naWZ5KHBheWxvYWQpfSk7Y3VycmVudEpvYj1yLmpvYklkfHxyLnRhc2tJZDtwb2xsKCk7dGltZXI9c2V0SW50ZXJ2YWwocG9sbCw1MDApfWNhdGNoKGUpe2FsZXJ0KGUubWVzc2FnZSk7JCgnc3RhcnRFeHBvcnQnKS5kaXNhYmxlZD1mYWxzZTskKCdzdG9wRXhwb3J0JykuZGlzYWJsZWQ9dHJ1ZX19JCgnc3RvcEV4cG9ydCcpLm9uY2xpY2s9YXN5bmMoKT0+e2lmKCFjdXJyZW50Sm9iKXJldHVybjskKCdzdG9wRXhwb3J0JykuZGlzYWJsZWQ9dHJ1ZTt0cnl7YXdhaXQgYXBpKCcvYXBpL2pvYnMvJytjdXJyZW50Sm9iKycvc3RvcCcse21ldGhvZDonUE9TVCd9KX1jYXRjaChlKXthbGVydChlLm1lc3NhZ2UpfX07YXN5bmMgZnVuY3Rpb24gcG9sbCgpe2lmKCFjdXJyZW50Sm9iKXJldHVybjt0cnl7Y29uc3Qgcj1hd2FpdCBhcGkoJy9hcGkvam9icy8nK2N1cnJlbnRKb2IpO2NvbnN0IGo9ci5qb2I7JCgncHJvZ3Jlc3MnKS52YWx1ZT1qLnByb2dyZXNzfHwwOyQoJ3N0YXR1c1RleHQnKS50ZXh0Q29udGVudD0oai5zdGF0dXN8fCcnKSsnICcrKGoucHJvZ3Jlc3NQZXJjZW50fHwoKGoucHJvZ3Jlc3N8fDApKyclJykpOyQoJ21lc3NhZ2UnKS50ZXh0Q29udGVudD1qLm1lc3NhZ2V8fCcnOyQoJ2xvZ3MnKS50ZXh0Q29udGVudD0oai5sb2dzfHxbXSkuam9pbignXG4nKTskKCdsb2dzJykuc2Nyb2xsVG9wPSQoJ2xvZ3MnKS5zY3JvbGxIZWlnaHQ7aWYoWydjb21wbGV0ZWQnLCdzdG9wcGVkJ10uaW5jbHVkZXMoai5zdGF0dXMpKXtjbGVhckludGVydmFsKHRpbWVyKTt0aW1lcj1udWxsOyQoJ3N0YXJ0RXhwb3J0JykuZGlzYWJsZWQ9ZmFsc2U7JCgnc3RvcEV4cG9ydCcpLmRpc2FibGVkPXRydWU7Y29uc3QgcGF0aHM9KChqLnJlc3VsdCYmai5yZXN1bHQucGF0aHMpfHxbXSkubWFwKHA9PmA8bGk+PGNvZGU+JHtlc2NhcGVIdG1sKHApfTwvY29kZT48L2xpPmApLmpvaW4oJycpOyQoJ3Jlc3VsdCcpLmlubmVySFRNTD1gPHA+PHN0cm9uZz4ke2ouc3RhdHVzPT09J3N0b3BwZWQnPyflt7Lkv53lrZjpg6jliIbnu5PmnpwnOiflrozmiJAnfTwvc3Ryb25nPjwvcD48cD7nm67lvZXvvJo8Y29kZT4ke2VzY2FwZUh0bWwoai5yZXN1bHQ/Lm91dHB1dERpcnx8JycpfTwvY29kZT48L3A+PHVsPiR7cGF0aHN9PC91bD5gO2xvYWRIaXN0b3J5KCkuY2F0Y2goKCk9Pnt9KX1lbHNlIGlmKGouc3RhdHVzPT09J2ZhaWxlZCcpe2NsZWFySW50ZXJ2YWwodGltZXIpO3RpbWVyPW51bGw7JCgnc3RhcnRFeHBvcnQnKS5kaXNhYmxlZD1mYWxzZTskKCdzdG9wRXhwb3J0JykuZGlzYWJsZWQ9dHJ1ZTskKCdyZXN1bHQnKS5pbm5lckhUTUw9YDxwPjxzdHJvbmc+5aSx6LSl77yaPC9zdHJvbmc+JHtlc2NhcGVIdG1sKGouZXJyb3J8fGoubWVzc2FnZXx8J3Vua25vd24nKX08L3A+YH19Y2F0Y2goZSl7JCgnbWVzc2FnZScpLnRleHRDb250ZW50PWUubWVzc2FnZX19ZnVuY3Rpb24gZXNjYXBlSHRtbChzKXtyZXR1cm4gU3RyaW5nKHM/PycnKS5yZXBsYWNlKC9bJjw+IiddL2csYz0+KHsnJic6JyZhbXA7JywnPCc6JyZsdDsnLCc+JzonJmd0OycsJyInOicmcXVvdDsnLCInIjonJiMwMzk7J31bY10pKX0KPC9zY3JpcHQ+PC9ib2R5PjwvaHRtbD4=', 'base64').toString('utf8');

async function requestHandler(req, res) {
  const urlObj = new URL(req.url, `http://${LISTEN_HOST}:${state.port || DEFAULT_PORT}`);
  if (urlObj.pathname.startsWith('/api/')) return handleApi(req, res, urlObj);
  if (urlObj.pathname === '/' || urlObj.pathname === '/index.html') return sendText(res, HTML);
  return sendJson(res, { error: 'not found' }, 404);
}

function listenOnPort(startPort) {
  return new Promise((resolve, reject) => {
    const tryPort = port => {
      const server = http.createServer(requestHandler);
      server.on('error', err => {
        if (err.code === 'EADDRINUSE' && port < startPort + 50) tryPort(port + 1);
        else reject(err);
      });
      server.listen(port, LISTEN_HOST, () => resolve({ server, port }));
    };
    tryPort(startPort);
  });
}

function openWebUi() {
  if (!state.port) return;
  electron.shell.openExternal(`http://${LISTEN_HOST}:${state.port}/`);
}



function setupPluginIpcHandlers() {
  try { electron.ipcMain.removeHandler('qq_export_open_web'); } catch {}
  try { electron.ipcMain.removeHandler('qq_export_status'); } catch {}
  electron.ipcMain.handle('qq_export_open_web', async () => {
    if (!state.started || !state.port) await start();
    openWebUi();
    return { ok: true, url: `http://${LISTEN_HOST}:${state.port}/` };
  });
  electron.ipcMain.handle('qq_export_status', async () => ({ ok: true, host: LISTEN_HOST, port: state.port, url: state.port ? `http://${LISTEN_HOST}:${state.port}/` : '' }));
}

async function start() {
  if (state.started) return;
  state.started = true;
  initDirs();
  setupPluginIpcHandlers();
  installIpcHook();
  const { server, port } = await listenOnPort(DEFAULT_PORT);
  state.server = server;
  state.port = port;
  log(`web ui listening on http://${LISTEN_HOST}:${port}/`);
}

start().catch(err => log('startup failed', err?.stack || err));

exports.onBrowserWindowCreated = function onBrowserWindowCreated() {
  // Kept for LiteLoader compatibility. The web UI is opened from the plugin settings panel.
};

exports.onUnload = function onUnload() {
  try { state.server?.close(); } catch {}
};
