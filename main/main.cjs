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
const { URL } = require('node:url');
const electron = require('electron');

const PLUGIN_NAME = 'qq_export';
const VERSION = '1.0.0';
const DEFAULT_PORT = 18765;
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
  Object.assign(task, patch, { updatedAt: new Date().toISOString() });
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
  const p = n => String(n).padStart(2, '0');
  return `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())} ${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}`;
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

function qceMessageType(raw, elements) {
  const msgType = Number(raw?.msgType ?? raw?.messageType ?? raw?.msgTypeEnum ?? 0);
  switch (msgType) {
    case 1:
    case 2:
      return 'type_1';
    case 3:
      return 'type_8';
    case 4:
    case 11:
      return 'type_7';
    case 5:
      return 'type_1';
    case 6:
      return 'type_6';
    case 7:
      return 'type_9';
    case 8:
      return 'type_11';
    case 9:
      return 'type_3';
    case 25:
      return 'type_17';
    default:
      if (elements.some(e => e.type === 'file')) return 'type_8';
      if (elements.some(e => e.type === 'video')) return 'type_9';
      if (elements.some(e => e.type === 'audio')) return 'type_6';
      if (elements.some(e => e.type === 'reply')) return 'type_3';
      if (elements.some(e => e.type === 'forward')) return 'type_11';
      if (elements.some(e => e.type === 'json')) return 'type_7';
      if (elements.some(e => e.type === 'location')) return 'type_17';
      return msgType ? `type_${msgType}` : 'type_1';
  }
}

function qceResourceFromElement(el) {
  if (!['image', 'file', 'video', 'audio', 'market_face'].includes(el.type)) return null;
  const d = el.data || {};
  const resourceType = el.type === 'market_face' ? 'image' : el.type;
  return stripUndefined({
    type: resourceType,
    filename: d.filename || d.fileName || d.name || '未知',
    size: Number(d.size || d.fileSize || 0) || 0,
    url: d.url,
    localPath: d.localPath || d.path,
    width: d.width ? Number(d.width) || 0 : undefined,
    height: d.height ? Number(d.height) || 0 : undefined,
    duration: d.duration ? Number(d.duration) || 0 : undefined,
    md5: d.md5 || d.fileMd5 || d.md5HexStr || d.key || d.emojiId,
    key: d.key || d.emojiId
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
    return { type: 'market_face', data: { name, tabName: pick(m, ['tabName'], ''), key: pick(m, ['key'], ''), emojiId: pick(m, ['emojiId'], ''), emojiPackageId: pick(m, ['emojiPackageId'], ''), filename: name, ...(url ? { url } : {}), ...(localPath ? { localPath } : {}) } };
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
  return stripUndefined({
    id: msgId,
    seq,
    timestamp,
    time: formatQceTime(timestamp),
    sender,
    type: qceMessageType(raw, parsedElements),
    content: { text, html: '', elements, resources, mentions },
    recalled,
    system
  });
}

function filterByOptions(raw, opts) {
  const ts = getRawMsgTimestamp(raw);
  if (opts.fromMs && ts < opts.fromMs) return false;
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

async function fetchSequential(peer, opts, task) {
  const out = [];
  const started = Date.now();
  pushTaskLog(task, `顺序历史拉取开始：batch=${Number(opts.batchCount || 200)} retry=${Number(opts.historyRetryCount || 5)} timeRange=${opts.fromMs ? new Date(opts.fromMs).toISOString() : '-'}..${opts.toMs ? new Date(opts.toMs).toISOString() : '-'}`);
  let latest = await fetchLatestPage(peer, Number(opts.batchCount || 200), task, opts.includeRecalled, Number(opts.historyRetryCount || 5), { tag: 'sequential-initial' });
  if (!latest.length) {
    pushTaskLog(task, '顺序历史拉取结束：最新页为空');
    return out;
  }
  let cursor = Math.min(...latest.map(getRawMsgSeq).filter(Boolean));
  let accepted = 0;
  for (const msg of latest) { if (filterByOptions(msg, opts)) { out.push(msg); accepted++; } }
  pushTaskLog(task, `顺序页 #1：raw=${latest.length} accepted=${accepted} cursor=${cursor} total=${out.length}`);
  let pages = 1;
  while (!task.stopRequested) {
    const minTime = Math.min(...latest.map(getRawMsgTimestamp).filter(Boolean));
    if (opts.fromMs && out.length && minTime && minTime < opts.fromMs) { pushTaskLog(task, `顺序停止：已越过开始时间 minPageTime=${new Date(minTime).toISOString()}`); break; }
    if (!cursor || cursor <= 1) { pushTaskLog(task, `顺序停止：游标到边界 cursor=${cursor}`); break; }
    const requestSeq = cursor - 1;
    const batch = await fetchBySeq(peer, requestSeq, Number(opts.batchCount || 200), task, opts.includeRecalled, Number(opts.historyRetryCount || 5), { page: pages + 1 });
    if (!batch.length) { pushTaskLog(task, `顺序停止：接口返回空页 requestSeq=${requestSeq}`); break; }
    latest = batch;
    cursor = Math.min(...batch.map(getRawMsgSeq).filter(Boolean));
    accepted = 0;
    for (const msg of batch) { if (filterByOptions(msg, opts)) { out.push(msg); accepted++; } }
    pages++;
    updateTask(task, { messageCount: out.length, progress: Math.min(70, 5 + pages) });
    pushTaskLog(task, `顺序页 #${pages}：raw=${batch.length} accepted=${accepted} cursor=${cursor} total=${out.length}`);
  }
  pushTaskLog(task, `顺序历史拉取完成：pages=${pages} accepted=${out.length} cost=${fmtDuration(Date.now() - started)} stopped=${!!task.stopRequested}`);
  return out;
}

function makeWindows(maxSeq, windowSize) {
  const wins = [];
  let hi = maxSeq;
  while (hi > 0) {
    const lo = Math.max(1, hi - windowSize + 1);
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
      const minSeq = Math.min(...seqs);
      const maxSeq = Math.max(...seqs);
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
    if (opts.fromMs && inWin.length) {
      const times = inWin.map(getRawMsgTimestamp).filter(Boolean);
      const minTs = times.length ? Math.min(...times) : 0;
      if (minTs && minTs < opts.fromMs) { reason = `达到开始时间 ${new Date(minTs).toISOString()}`; break; }
    }
  }
  if (task.stopRequested) reason = '提前停止';
  pushTaskLog(task, `[W${workerId}] 窗口结束 ${rangeLabel(win)} reason=${reason} pages=${pages} raw=${rawTotal} inWindow=${inWindowTotal} accepted=${out.length} cost=${fmtDuration(Date.now() - started)}`);
  return out;
}

async function fetchParallel(peer, opts, task) {
  const started = Date.now();
  const first = await fetchLatestPage(peer, Math.min(Number(opts.batchCount || 150), 200), task, opts.includeRecalled, Number(opts.historyRetryCount || 5), { tag: 'parallel-probe' });
  const maxSeq = Math.max(...first.map(getRawMsgSeq).filter(Boolean), 0);
  if (!maxSeq) {
    pushTaskLog(task, '并行历史拉取停止：无法从最新页确定 seq 上界');
    return [];
  }
  const firstAccepted = first.filter(m => filterByOptions(m, opts));
  const windowSize = Number(opts.seqWindow || 0) || Math.max(25000, Math.ceil(maxSeq / 24));
  const windows = makeWindows(maxSeq, windowSize);
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
        results.push(...arr);
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
        results.push(...arr);
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


function computeStats(messages) {
  const st = createStatsTracker();
  for (const m of messages) updateStatsTracker(st, m);
  return finalizeStats(st);
}

function makeChatInfo(peer, messagesOrCount) {
  const self = getSelfInfo();
  const result = {
    name: peer.name,
    type: peer.typeName
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
  return `fallback:${resource?.type || 'file'}:${resource?.filename || resource?.fileName || ''}:${resource?.size || resource?.fileSize || 0}`;
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
    if (el.type === 'market_face') add({ type: 'image', filename: d.filename || d.name || d.emojiId || 'sticker', url: d.url, localPath: d.localPath, path: d.localPath, md5: d.key || d.emojiId || d.md5, messageId: msg.id });
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
  let sub = 'files';
  if (resource.type === 'image') sub = 'images';
  else if (resource.type === 'audio') sub = 'audios';
  else if (resource.type === 'video') sub = 'videos';
  const dir = path.join(baseDir, 'resources', sub);
  ensureDir(dir);
  let ext = path.extname((resource.filename || resource.fileName) || '');
  if (!ext) {
    if (resource.type === 'image') ext = '.jpg';
    else if (resource.type === 'audio') ext = '.silk';
    else if (resource.type === 'video') ext = '.mp4';
    else ext = '.bin';
  }
  let name = safeName(path.basename((resource.filename || resource.fileName) || `${resource.type || 'file'}${ext}`));
  if (!path.extname(name)) name += ext;
  const base = name.replace(new RegExp(`${ext.replace('.', '\.')}$`), '');
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
  ensureDir(chunksDir);
  const chunks = [];
  let stream = null;
  let chunkIndex = 0;
  let chunkCount = 0;
  let chunkBytes = 0;
  let chunkStart = 0;
  let chunkEnd = 0;
  const statsTracker = createStatsTracker();
  const resources = [];
  const resourceSeen = new Set();

  async function finishChunk() {
    if (!stream) return;
    await endStream(stream);
    if (chunkCount > 0) {
      const fileName = `chunk_${String(chunkIndex).padStart(4, '0')}.jsonl`;
      const info = { index: chunkIndex, fileName, relativePath: `chunks/${fileName}`, start: chunkStart ? new Date(chunkStart).toISOString() : '', end: chunkEnd ? new Date(chunkEnd).toISOString() : '', count: chunkCount, bytes: chunkBytes };
      chunks.push(info);
      if (task) pushTaskLog(task, `JSONL chunk 完成：${fileName} count=${chunkCount} bytes=${chunkBytes}`);
    }
    stream = null;
  }

  async function newChunk() {
    await finishChunk();
    chunkIndex++;
    chunkCount = 0;
    chunkBytes = 0;
    chunkStart = 0;
    chunkEnd = 0;
    const fileName = `chunk_${String(chunkIndex).padStart(4, '0')}.jsonl`;
    stream = fs.createWriteStream(path.join(chunksDir, fileName), { encoding: 'utf8' });
  }

  const total = Array.isArray(sourceMessages) ? sourceMessages.length : 0;
  if (task) pushTaskLog(task, `开始流式写入 QCE chunked JSONL：source=${total || '?'} maxMessagesPerChunk=${MAX_MESSAGES_PER_CHUNK} maxBytesPerChunk=${MAX_BYTES_PER_CHUNK}`);
  await newChunk();
  let written = 0;

  for (const src of sourceMessages) {
    const msg = options.alreadyNormalized ? src : normalizeMessage(src, peer);
    const line = JSON.stringify(msg, jsonReplacer) + '\n';
    const bytes = Buffer.byteLength(line, 'utf8');
    if (chunkCount >= MAX_MESSAGES_PER_CHUNK || chunkBytes + bytes > MAX_BYTES_PER_CHUNK) await newChunk();
    await writeLine(stream, line);
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
    if (task && written % 50000 === 0) pushTaskLog(task, `JSONL 流式写入进度：${written}/${total || '?'}`);
  }
  await finishChunk();

  const stats = finalizeStats(statsTracker);
  const metadata = { ...qceMetadata(), exportTime: new Date().toISOString(), format: 'chunked-jsonl' };
  const manifest = { metadata, chatInfo: makeChatInfo(peer, stats.totalMessages), statistics: stats, chunked: { format: 'jsonl', chunksDir: 'chunks', chunkFileExt: '.jsonl', maxMessagesPerChunk: MAX_MESSAGES_PER_CHUNK, maxBytesPerChunk: MAX_BYTES_PER_CHUNK, chunks } };
  writeJson(path.join(outDir, 'manifest.json'), manifest);
  if (task) pushTaskLog(task, `chunked JSONL 流式写入完成：messages=${stats.totalMessages} chunks=${chunks.length} manifest=${path.join(outDir, 'manifest.json')}`);
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
  let fromMs = 0, toMs = 0;
  const now = nowMs();
  if (opts.incremental && (baseHistory?.latestTimestamp || opts.incrementalStartTimestamp)) {
    fromMs = Number(baseHistory?.latestTimestamp || opts.incrementalStartTimestamp) + 1;
  } else if (range === '1d') fromMs = now - 86400000;
  else if (range === '1w') fromMs = now - 7 * 86400000;
  else if (range === '1m') fromMs = now - 30 * 86400000;
  else if (range === '1y') fromMs = now - 365 * 86400000;
  else if (range === 'manual') {
    if (opts.from) fromMs = new Date(opts.from).getTime();
    if (opts.to) toMs = new Date(opts.to).getTime();
  }
  return { fromMs: Number.isFinite(fromMs) ? fromMs : 0, toMs: Number.isFinite(toMs) ? toMs : 0 };
}

async function runExport(task) {
  try {
    updateTask(task, { status: 'running', message: '准备导出', progress: 1 });
    const opts = task.options;
    const history = loadHistory();
    const baseHistory = opts.incrementalBaseId ? history.find(h => h.id === opts.incrementalBaseId) : null;
    const range = parseTimeRange(opts, baseHistory);
    opts.fromMs = range.fromMs;
    opts.toMs = range.toMs;
    const peer = await resolvePeer(opts);
    task.peer = peer;
    const stamp = new Date().toISOString().replace(/[-:T.Z]/g, '').slice(0, 14);
    const dirName = `${peer.typeName}_${safeName(peer.name)}_${safeName(peer.uin || peer.peerUid)}_${stamp}${opts.incremental ? '_incremental' : ''}`;
    const outDir = opts.outputDir ? path.join(opts.outputDir, dirName) : path.join(state.exportsDir, dirName);
    ensureDir(outDir);
    task.outputDir = outDir;
    pushTaskLog(task, `输出目录：${outDir}`);
    pushTaskLog(task, `任务参数：session=${peer.typeName}/${peer.name}/${peer.uin || peer.peerUid} format=${opts.format} time=${range.fromMs ? new Date(range.fromMs).toISOString() : 'all'}..${range.toMs ? new Date(range.toMs).toISOString() : 'now'} batch=${opts.batchCount} retry=${opts.historyRetryCount} parallel=${!!opts.parallelHistory} workers=${opts.historyWorkers} seqWindow=${opts.seqWindow} resources=${!!opts.exportResources}`);
    pushTaskLog(task, opts.parallelHistory ? '直接 QQNT IPC 并行拉取历史' : '直接 QQNT IPC 顺序拉取历史');
    updateTask(task, { message: '读取历史消息', progress: 5 });

    const raw = opts.parallelHistory ? await fetchParallel(peer, opts, task) : await fetchSequential(peer, opts, task);
    let sortedRaw = dedupeAndSort(raw);
    if (opts.maxMessages && sortedRaw.length > opts.maxMessages) sortedRaw = sortedRaw.slice(-opts.maxMessages);
    const duplicateCount = raw.length - sortedRaw.length;
    const seqsAfter = sortedRaw.map(getRawMsgSeq).filter(Boolean);
    pushTaskLog(task, `历史读取完成：原始 ${raw.length} 条，去重后 ${sortedRaw.length} 条，重复 ${duplicateCount} 条，seqRange=${seqsAfter.length ? Math.min(...seqsAfter) + '-' + Math.max(...seqsAfter) : '-'}`);
    updateTask(task, { progress: 75, message: '写入 JSON/JSONL', messageCount: sortedRaw.length });

    const outputs = [];
    let stats = null;
    let messageCount = 0;
    let resourcesForExport = [];

    if (opts.format === 'jsonl') {
      pushTaskLog(task, '已选择 JSONL：直接从 raw 消息流式归一化并写入 chunk，不构造完整 messages/exportData 大对象');
      const jsonl = await writeChunkedJsonlFromRaw(outDir, peer, sortedRaw, task);
      outputs.push(jsonl.file);
      stats = jsonl.stats;
      messageCount = jsonl.messageCount;
      resourcesForExport = jsonl.resources;
    } else {
      pushTaskLog(task, `开始归一化消息：${sortedRaw.length} 条（format=${opts.format} 需要完整 messages 数组）`);
      const messages = sortedRaw.map(m => normalizeMessage(m, peer));
      const typeStats = messages.reduce((m, x) => { m[x.type || 'unknown'] = (m[x.type || 'unknown'] || 0) + 1; return m; }, {});
      pushTaskLog(task, `归一化完成：typeStats=${JSON.stringify(typeStats)}`);
      if (opts.format === 'json') outputs.push(await writeSingleJson(outDir, peer, messages, opts, task));
      stats = computeStats(messages);
      messageCount = messages.length;
      resourcesForExport = summarizeResourcesFromMessages(messages, opts);
    }

    if (opts.exportResources) {
      updateTask(task, { progress: 82, message: '导出资源' });
      await exportResourcesFromCandidates(resourcesForExport, outDir, task, opts.resourceWorkers || 3);
    }

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
    batchCount: Number(input.batchCount || input.batchSize || 500),
    seqWindow: Number(input.seqWindow || input.seqWindowSize || 50000),
    resourceWorkers: Number(input.resourceWorkers || 3),
    historyRetryCount: Math.max(5, Number(input.historyRetryCount || input.retries || 5)),
    maxMessages: Math.max(0, Number(input.maxMessages || 0)),
    incremental: !!(input.incremental || input.timeMode === 'incremental'),
    incrementalBaseId: input.incrementalBaseId || input.incrementalOf || null,
    incrementalStartTimestamp: Number(input.incrementalStartTimestamp || 0) || 0,
    outputDir: input.outputDir || '',
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
      return sendJson(res, { ok: true, defaultExportDir: state.exportsDir, defaultBaseUrl: '', version: VERSION, dataDir: state.dataDir });
    }
    if (urlObj.pathname === '/api/status') {
      return sendJson(res, { ok: true, version: VERSION, port: state.port, dataDir: state.dataDir, exportsDir: state.exportsDir, channel: getChannel(), buildVersion: getBuildVersion(), self: getSelfInfo() });
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
    if (urlObj.pathname === '/api/tasks') {
      return sendJson(res, { tasks: Array.from(state.tasks.values()).map(t => ({ ...t, logs: t.logs.slice(-80) })) });
    }
    if (urlObj.pathname === '/api/task') {
      const id = urlObj.searchParams.get('id');
      const t = state.tasks.get(id);
      return sendJson(res, t ? { task: t } : { error: 'task not found' }, t ? 200 : 404);
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
    if (urlObj.pathname === '/api/export' && req.method === 'POST') {
      const body = await readBody(req);
      const task = createTask(body);
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

const HTML = Buffer.from('PCFkb2N0eXBlIGh0bWw+CjxodG1sIGxhbmc9InpoLUNOIj48aGVhZD48bWV0YSBjaGFyc2V0PSJ1dGYtOCI+PG1ldGEgbmFtZT0idmlld3BvcnQiIGNvbnRlbnQ9IndpZHRoPWRldmljZS13aWR0aCwgaW5pdGlhbC1zY2FsZT0xIj48dGl0bGU+cXFfZXhwb3J0PC90aXRsZT48c3R5bGU+Cjpyb290e2NvbG9yLXNjaGVtZTpsaWdodCBkYXJrfWJvZHl7Zm9udC1mYW1pbHk6LWFwcGxlLXN5c3RlbSxCbGlua01hY1N5c3RlbUZvbnQsIlNlZ29lIFVJIixzYW5zLXNlcmlmO21hcmdpbjowO2JhY2tncm91bmQ6Q2FudmFzO2NvbG9yOkNhbnZhc1RleHR9bWFpbnttYXgtd2lkdGg6MTE2MHB4O21hcmdpbjoyOHB4IGF1dG87cGFkZGluZzowIDE4cHggNDJweH1oMXtmb250LXNpemU6MjhweDttYXJnaW46MCAwIDE4cHh9aDJ7Zm9udC1zaXplOjE4cHg7bWFyZ2luOjAgMCAxNHB4fXNlY3Rpb257Ym9yZGVyOjFweCBzb2xpZCBjb2xvci1taXgoaW4gc3JnYixDYW52YXNUZXh0IDE4JSx0cmFuc3BhcmVudCk7Ym9yZGVyLXJhZGl1czoxNHB4O3BhZGRpbmc6MThweDttYXJnaW46MTZweCAwO2JhY2tncm91bmQ6Y29sb3ItbWl4KGluIHNyZ2IsQ2FudmFzIDk2JSxDYW52YXNUZXh0IDQlKX0udGFic3tkaXNwbGF5OmZsZXg7Z2FwOjhweDttYXJnaW46MCAwIDE2cHh9LnRhYnt3aWR0aDphdXRvO3BhZGRpbmc6MTBweCAxNnB4O2JvcmRlci1yYWRpdXM6OTk5cHh9LnRhYi5hY3RpdmV7YmFja2dyb3VuZDpIaWdobGlnaHQ7Y29sb3I6SGlnaGxpZ2h0VGV4dDtib3JkZXItY29sb3I6SGlnaGxpZ2h0O2ZvbnQtd2VpZ2h0OjYwMH0ucGFuZWx7ZGlzcGxheTpub25lfS5wYW5lbC5hY3RpdmV7ZGlzcGxheTpibG9ja30uZ3JpZHtkaXNwbGF5OmdyaWQ7Z3JpZC10ZW1wbGF0ZS1jb2x1bW5zOnJlcGVhdCgyLG1pbm1heCgwLDFmcikpO2dhcDoxMnB4fS5ncmlkM3tkaXNwbGF5OmdyaWQ7Z3JpZC10ZW1wbGF0ZS1jb2x1bW5zOnJlcGVhdCgzLG1pbm1heCgwLDFmcikpO2dhcDoxMnB4fS5ncmlkNHtkaXNwbGF5OmdyaWQ7Z3JpZC10ZW1wbGF0ZS1jb2x1bW5zOnJlcGVhdCg0LG1pbm1heCgwLDFmcikpO2dhcDoxMnB4fWxhYmVse2Rpc3BsYXk6YmxvY2s7Zm9udC1zaXplOjEzcHg7bWFyZ2luLWJvdHRvbTo1cHg7Y29sb3I6Y29sb3ItbWl4KGluIHNyZ2IsQ2FudmFzVGV4dCA3MiUsdHJhbnNwYXJlbnQpfWlucHV0LHNlbGVjdCxidXR0b257Ym94LXNpemluZzpib3JkZXItYm94O3dpZHRoOjEwMCU7Ym9yZGVyLXJhZGl1czoxMHB4O2JvcmRlcjoxcHggc29saWQgY29sb3ItbWl4KGluIHNyZ2IsQ2FudmFzVGV4dCAyMiUsdHJhbnNwYXJlbnQpO3BhZGRpbmc6MTBweCAxMXB4O2ZvbnQtc2l6ZToxNHB4O2JhY2tncm91bmQ6Q2FudmFzO2NvbG9yOkNhbnZhc1RleHR9YnV0dG9ue2N1cnNvcjpwb2ludGVyO2JhY2tncm91bmQ6Y29sb3ItbWl4KGluIHNyZ2IsSGlnaGxpZ2h0IDEyJSxDYW52YXMpO2JvcmRlci1jb2xvcjpjb2xvci1taXgoaW4gc3JnYixIaWdobGlnaHQgNTUlLENhbnZhc1RleHQgMTUlKX1idXR0b24ucHJpbWFyeXtiYWNrZ3JvdW5kOkhpZ2hsaWdodDtjb2xvcjpIaWdobGlnaHRUZXh0O2JvcmRlci1jb2xvcjpIaWdobGlnaHQ7Zm9udC13ZWlnaHQ6NjAwfWJ1dHRvbi5kYW5nZXJ7YmFja2dyb3VuZDpjb2xvci1taXgoaW4gc3JnYixyZWQgMTglLENhbnZhcyk7Ym9yZGVyLWNvbG9yOmNvbG9yLW1peChpbiBzcmdiLHJlZCA1NSUsQ2FudmFzVGV4dCAxNSUpfWJ1dHRvbjpkaXNhYmxlZHtvcGFjaXR5Oi41NTtjdXJzb3I6bm90LWFsbG93ZWR9LnJvd3tkaXNwbGF5OmZsZXg7Z2FwOjEwcHg7YWxpZ24taXRlbXM6ZW5kfS5yb3c+KntmbGV4OjF9LmNoZWNrc3tkaXNwbGF5OmZsZXg7ZmxleC13cmFwOndyYXA7Z2FwOjE0cHg7YWxpZ24taXRlbXM6Y2VudGVyO21hcmdpbi10b3A6MTBweH0uY2hlY2tzIGxhYmVse2Rpc3BsYXk6ZmxleDthbGlnbi1pdGVtczpjZW50ZXI7Z2FwOjZweDttYXJnaW46MH1pbnB1dFt0eXBlPWNoZWNrYm94XXt3aWR0aDphdXRvfXRhYmxle3dpZHRoOjEwMCU7Ym9yZGVyLWNvbGxhcHNlOmNvbGxhcHNlO2ZvbnQtc2l6ZToxM3B4fXRoLHRke2JvcmRlci1ib3R0b206MXB4IHNvbGlkIGNvbG9yLW1peChpbiBzcmdiLENhbnZhc1RleHQgMTIlLHRyYW5zcGFyZW50KTtwYWRkaW5nOjlweCA3cHg7dGV4dC1hbGlnbjpsZWZ0O3ZlcnRpY2FsLWFsaWduOnRvcH10Ym9keSB0ci5zZWxlY3RhYmxle2N1cnNvcjpwb2ludGVyfXRib2R5IHRyLnNlbGVjdGFibGU6aG92ZXJ7YmFja2dyb3VuZDpjb2xvci1taXgoaW4gc3JnYixIaWdobGlnaHQgMTAlLHRyYW5zcGFyZW50KX10Ym9keSB0ci5zZWxlY3RlZHtiYWNrZ3JvdW5kOmNvbG9yLW1peChpbiBzcmdiLEhpZ2hsaWdodCAyMiUsdHJhbnNwYXJlbnQpfWNvZGUscHJle2ZvbnQtZmFtaWx5OnVpLW1vbm9zcGFjZSxTRk1vbm8tUmVndWxhcixNZW5sbyxDb25zb2xhcyxtb25vc3BhY2V9cHJle2JhY2tncm91bmQ6Y29sb3ItbWl4KGluIHNyZ2IsQ2FudmFzVGV4dCA4JSx0cmFuc3BhcmVudCk7cGFkZGluZzoxMnB4O2JvcmRlci1yYWRpdXM6MTJweDtvdmVyZmxvdzphdXRvO21heC1oZWlnaHQ6MzIwcHh9cHJvZ3Jlc3N7d2lkdGg6MTAwJTtoZWlnaHQ6MThweH0uc3RhdHVze2Rpc3BsYXk6Z3JpZDtncmlkLXRlbXBsYXRlLWNvbHVtbnM6MWZyIGF1dG87Z2FwOjEwcHg7YWxpZ24taXRlbXM6Y2VudGVyfS5zbWFsbHtmb250LXNpemU6MTJweDtjb2xvcjpjb2xvci1taXgoaW4gc3JnYixDYW52YXNUZXh0IDY1JSx0cmFuc3BhcmVudCl9LnBpbGx7ZGlzcGxheTppbmxpbmUtYmxvY2s7cGFkZGluZzoycHggOHB4O2JvcmRlci1yYWRpdXM6OTk5cHg7YmFja2dyb3VuZDpjb2xvci1taXgoaW4gc3JnYixIaWdobGlnaHQgMTIlLHRyYW5zcGFyZW50KTttYXJnaW4tcmlnaHQ6NnB4O2ZvbnQtc2l6ZToxMnB4fS5hY3Rpb25ze2Rpc3BsYXk6ZmxleDtnYXA6OHB4O2ZsZXgtd3JhcDp3cmFwfS5hY3Rpb25zIGJ1dHRvbnt3aWR0aDphdXRvO3BhZGRpbmc6N3B4IDEwcHh9Lmhpc3RvcnktdG9vbHN7ZGlzcGxheTpncmlkO2dyaWQtdGVtcGxhdGUtY29sdW1uczoxZnIgYXV0bztnYXA6MTBweDthbGlnbi1pdGVtczplbmR9Lmhpc3RvcnktbGlzdHtkaXNwbGF5OmdyaWQ7Z2FwOjEwcHh9Lmhpc3RvcnktY2FyZHtib3JkZXI6MXB4IHNvbGlkIGNvbG9yLW1peChpbiBzcmdiLENhbnZhc1RleHQgMTQlLHRyYW5zcGFyZW50KTtib3JkZXItcmFkaXVzOjEycHg7cGFkZGluZzoxMnB4O2Rpc3BsYXk6Z3JpZDtncmlkLXRlbXBsYXRlLWNvbHVtbnM6bWlubWF4KDAsMS4yZnIpIDExMHB4IG1pbm1heCgwLDFmcikgYXV0bztnYXA6MTJweDthbGlnbi1pdGVtczpjZW50ZXJ9Lmhpc3RvcnktY2FyZCBzdHJvbmd7ZGlzcGxheTpibG9jazttYXJnaW4tYm90dG9tOjRweH0uaGlzdG9yeS1jYXJkIGNvZGV7d29yZC1icmVhazpicmVhay1hbGx9QG1lZGlhKG1heC13aWR0aDo4NjBweCl7LmdyaWQsLmdyaWQzLC5ncmlkNCwuaGlzdG9yeS1jYXJke2dyaWQtdGVtcGxhdGUtY29sdW1uczoxZnJ9LnJvd3tmbGV4LWRpcmVjdGlvbjpjb2x1bW47YWxpZ24taXRlbXM6c3RyZXRjaH0uc3RhdHVze2dyaWQtdGVtcGxhdGUtY29sdW1uczoxZnJ9fQo8L3N0eWxlPjwvaGVhZD48Ym9keT48bWFpbj48aDE+cXFfZXhwb3J0PC9oMT48ZGl2IGNsYXNzPSJ0YWJzIj48YnV0dG9uIGNsYXNzPSJ0YWIgYWN0aXZlIiBkYXRhLXRhYj0iZXhwb3J0UGFuZWwiPuaWsOW7uuWvvOWHujwvYnV0dG9uPjxidXR0b24gY2xhc3M9InRhYiIgZGF0YS10YWI9Imhpc3RvcnlQYW5lbCI+5Y6G5Y+y5a+85Ye6PC9idXR0b24+PC9kaXY+PGRpdiBpZD0iZXhwb3J0UGFuZWwiIGNsYXNzPSJwYW5lbCBhY3RpdmUiPjxzZWN0aW9uPjxoMj7kvJror508L2gyPjxkaXYgY2xhc3M9InJvdyI+PGRpdj48bGFiZWw+5pCc57SiIFFRL+e+pOWPt+OAgeeUqOaIty/nvqTlkI08L2xhYmVsPjxpbnB1dCBpZD0icXVlcnkiIHBsYWNlaG9sZGVyPSLnlZnnqbrlj6/miYvliqjovpPlhaUiPjwvZGl2PjxidXR0b24gaWQ9ImxvYWRTZXNzaW9ucyIgc3R5bGU9Im1heC13aWR0aDoxODBweCI+6K+75Y+W5Lya6K+d5YiX6KGoPC9idXR0b24+PC9kaXY+PGRpdiBjbGFzcz0iZ3JpZDMiIHN0eWxlPSJtYXJnaW4tdG9wOjEycHgiPjxkaXY+PGxhYmVsPuexu+WeizwvbGFiZWw+PHNlbGVjdCBpZD0ic2Vzc2lvblR5cGUiPjxvcHRpb24gdmFsdWU9InByaXZhdGUiPuengeiBijwvb3B0aW9uPjxvcHRpb24gdmFsdWU9Imdyb3VwIj7nvqTogYo8L29wdGlvbj48L3NlbGVjdD48L2Rpdj48ZGl2PjxsYWJlbD5RUS/nvqTlj7cg5oiWIFVJRDwvbGFiZWw+PGlucHV0IGlkPSJzZXNzaW9uSWQiIHBsYWNlaG9sZGVyPSLmiYvliqjovpPlhaUiPjwvZGl2PjxkaXY+PGxhYmVsPuaYvuekuuWQjeensDwvbGFiZWw+PGlucHV0IGlkPSJzZXNzaW9uTmFtZSIgcGxhY2Vob2xkZXI9IuWPr+mAiSI+PC9kaXY+PC9kaXY+PGRpdiBpZD0ic2Vzc2lvbkJveCIgc3R5bGU9Im1hcmdpbi10b3A6MTJweDtkaXNwbGF5Om5vbmUiPjx0YWJsZT48dGhlYWQ+PHRyPjx0aD7nsbvlnos8L3RoPjx0aD7lj7fnoIE8L3RoPjx0aD5VSUQ8L3RoPjx0aD7lkI3np7A8L3RoPjwvdHI+PC90aGVhZD48dGJvZHkgaWQ9InNlc3Npb25zIj48L3Rib2R5PjwvdGFibGU+PC9kaXY+PC9zZWN0aW9uPjxzZWN0aW9uPjxoMj7lr7zlh7rpgInpobk8L2gyPjxkaXYgY2xhc3M9ImdyaWQ0Ij48ZGl2PjxsYWJlbD7moLzlvI88L2xhYmVsPjxzZWxlY3QgaWQ9ImZvcm1hdCI+PG9wdGlvbiB2YWx1ZT0ianNvbiI+SlNPTjwvb3B0aW9uPjxvcHRpb24gdmFsdWU9Impzb25sIj5KU09OTDwvb3B0aW9uPjwvc2VsZWN0PjwvZGl2PjxkaXY+PGxhYmVsPuaXtumXtOiMg+WbtDwvbGFiZWw+PHNlbGVjdCBpZD0idGltZU1vZGUiPjxvcHRpb24gdmFsdWU9Im1hbnVhbCI+5omL5Yqo6L6T5YWlPC9vcHRpb24+PG9wdGlvbiB2YWx1ZT0iYWxsIj7lhajpg6g8L29wdGlvbj48b3B0aW9uIHZhbHVlPSIxZCI+5pyA6L+RIDEg5aSpPC9vcHRpb24+PG9wdGlvbiB2YWx1ZT0iMXciPuacgOi/kSAxIOWRqDwvb3B0aW9uPjxvcHRpb24gdmFsdWU9IjFtIj7mnIDov5EgMSDmnIg8L29wdGlvbj48b3B0aW9uIHZhbHVlPSIxeSI+5pyA6L+RIDEg5bm0PC9vcHRpb24+PC9zZWxlY3Q+PC9kaXY+PGRpdj48bGFiZWw+5q+P5om55raI5oGv5p2h5pWwPC9sYWJlbD48aW5wdXQgaWQ9ImJhdGNoU2l6ZSIgdHlwZT0ibnVtYmVyIiBtaW49IjIwIiBtYXg9IjUwMCIgdmFsdWU9IjUwMCI+PC9kaXY+PGRpdj48bGFiZWw+5pyA5aSn5raI5oGv5pWwPC9sYWJlbD48aW5wdXQgaWQ9Im1heE1lc3NhZ2VzIiB0eXBlPSJudW1iZXIiIG1pbj0iMCIgdmFsdWU9IjAiPjwvZGl2PjwvZGl2PjxkaXYgaWQ9Im1hbnVhbFRpbWVSb3ciIGNsYXNzPSJncmlkIiBzdHlsZT0ibWFyZ2luLXRvcDoxMnB4Ij48ZGl2PjxsYWJlbD7lvIDlp4vml7bpl7Q8L2xhYmVsPjxpbnB1dCBpZD0ic3RhcnRUaW1lIiB0eXBlPSJkYXRldGltZS1sb2NhbCI+PC9kaXY+PGRpdj48bGFiZWw+57uT5p2f5pe26Ze0PC9sYWJlbD48aW5wdXQgaWQ9ImVuZFRpbWUiIHR5cGU9ImRhdGV0aW1lLWxvY2FsIj48L2Rpdj48L2Rpdj48ZGl2IGNsYXNzPSJncmlkMyIgc3R5bGU9Im1hcmdpbi10b3A6MTJweCI+PGRpdj48bGFiZWw+5Y6G5Y+y5bm25Y+R57q/56iL5pWwPC9sYWJlbD48aW5wdXQgaWQ9Imhpc3RvcnlXb3JrZXJzIiB0eXBlPSJudW1iZXIiIG1pbj0iMSIgbWF4PSI0IiB2YWx1ZT0iNCI+PC9kaXY+PGRpdj48bGFiZWw+U2VxIOeql+WPo+Wkp+WwjzwvbGFiZWw+PGlucHV0IGlkPSJzZXFXaW5kb3dTaXplIiB0eXBlPSJudW1iZXIiIG1pbj0iMTAwMCIgdmFsdWU9IjUwMDAwIj48L2Rpdj48ZGl2PjxsYWJlbD7lpLHotKXph43or5XmrKHmlbA8L2xhYmVsPjxpbnB1dCBpZD0iaGlzdG9yeVJldHJ5Q291bnQiIHR5cGU9Im51bWJlciIgbWluPSI1IiBtYXg9IjIwIiB2YWx1ZT0iNSI+PC9kaXY+PC9kaXY+PGRpdiBjbGFzcz0icm93IiBzdHlsZT0ibWFyZ2luLXRvcDoxMnB4Ij48ZGl2PjxsYWJlbD7lr7zlh7rmlofku7blpLk8L2xhYmVsPjxpbnB1dCBpZD0ib3V0cHV0RGlyIiBwbGFjZWhvbGRlcj0i55WZ56m65L2/55So6buY6K6k55uu5b2VIj48L2Rpdj48YnV0dG9uIGlkPSJjaG9vc2VPdXRwdXREaXIiIHN0eWxlPSJtYXgtd2lkdGg6MTUwcHgiPumAieaLqeaWh+S7tuWkuTwvYnV0dG9uPjwvZGl2PjxkaXYgY2xhc3M9ImNoZWNrcyI+PGxhYmVsPjxpbnB1dCBpZD0iaW5jbHVkZVN5c3RlbU1lc3NhZ2VzIiB0eXBlPSJjaGVja2JveCIgY2hlY2tlZD4g5a+85Ye657O757uf5raI5oGvPC9sYWJlbD48bGFiZWw+PGlucHV0IGlkPSJpbmNsdWRlUmVjYWxsZWRNZXNzYWdlcyIgdHlwZT0iY2hlY2tib3giPiDlr7zlh7rlt7LmkqTlm57nmoTmtojmga88L2xhYmVsPjxsYWJlbD48aW5wdXQgaWQ9InBhcmFsbGVsSGlzdG9yeSIgdHlwZT0iY2hlY2tib3giIGNoZWNrZWQ+IOW5tuihjOivu+WPluWOhuWPsjwvbGFiZWw+PGxhYmVsPjxpbnB1dCBpZD0iaW5jbHVkZVJlc291cmNlcyIgdHlwZT0iY2hlY2tib3giPiDotYTmupDmlofku7Y8L2xhYmVsPjwvZGl2PjxkaXYgY2xhc3M9ImFjdGlvbnMiIHN0eWxlPSJtYXJnaW4tdG9wOjE2cHgiPjxidXR0b24gaWQ9InN0YXJ0RXhwb3J0IiBjbGFzcz0icHJpbWFyeSI+5byA5aeL5a+85Ye6PC9idXR0b24+PGJ1dHRvbiBpZD0ic3RvcEV4cG9ydCIgY2xhc3M9ImRhbmdlciIgZGlzYWJsZWQ+5o+Q5YmN5YGc5q2i5bm25L+d5a2YPC9idXR0b24+PC9kaXY+PC9zZWN0aW9uPjxzZWN0aW9uPjxoMj7nirbmgIE8L2gyPjxkaXYgY2xhc3M9InN0YXR1cyI+PHByb2dyZXNzIGlkPSJwcm9ncmVzcyIgdmFsdWU9IjAiIG1heD0iMTAwIj48L3Byb2dyZXNzPjxzdHJvbmcgaWQ9InN0YXR1c1RleHQiPmlkbGU8L3N0cm9uZz48L2Rpdj48cCBpZD0ibWVzc2FnZSIgY2xhc3M9InNtYWxsIj48L3A+PHByZSBpZD0ibG9ncyI+PC9wcmU+PGRpdiBpZD0icmVzdWx0Ij48L2Rpdj48L3NlY3Rpb24+PC9kaXY+PGRpdiBpZD0iaGlzdG9yeVBhbmVsIiBjbGFzcz0icGFuZWwiPjxzZWN0aW9uPjxoMj7ljoblj7Llr7zlh7o8L2gyPjxkaXYgY2xhc3M9Imhpc3RvcnktdG9vbHMiPjxkaXY+PGxhYmVsPuetm+mAiTwvbGFiZWw+PGlucHV0IGlkPSJoaXN0b3J5UXVlcnkiIHBsYWNlaG9sZGVyPSLkvJror53lkI3jgIHlj7fnoIHmiJbot6/lvoQiPjwvZGl2PjxidXR0b24gaWQ9ImxvYWRIaXN0b3J5Ij7liLfmlrA8L2J1dHRvbj48L2Rpdj48ZGl2IGlkPSJoaXN0b3J5Um93cyIgY2xhc3M9Imhpc3RvcnktbGlzdCIgc3R5bGU9Im1hcmdpbi10b3A6MTJweCI+PC9kaXY+PC9zZWN0aW9uPjwvZGl2PjwvbWFpbj48c2NyaXB0Pgpjb25zdCAkPWlkPT5kb2N1bWVudC5nZXRFbGVtZW50QnlJZChpZCk7bGV0IHNlbGVjdGVkU2Vzc2lvbj1udWxsLHNlbGVjdGVkSGlzdG9yeT1udWxsLGN1cnJlbnRKb2I9bnVsbCx0aW1lcj1udWxsO2FzeW5jIGZ1bmN0aW9uIGFwaShwYXRoLG9wdHM9e30pe2NvbnN0IHI9YXdhaXQgZmV0Y2gocGF0aCxvcHRzKTtjb25zdCB0PWF3YWl0IHIudGV4dCgpO2xldCBqPXt9O3RyeXtqPXQ/SlNPTi5wYXJzZSh0KTp7fX1jYXRjaHt0aHJvdyBuZXcgRXJyb3IodCl9aWYoIXIub2t8fGouZXJyb3J8fGoub2s9PT1mYWxzZSl0aHJvdyBuZXcgRXJyb3Ioai5lcnJvcnx8ai5tZXNzYWdlfHxyLnN0YXR1c1RleHQpO3JldHVybiBqfWRvY3VtZW50LnF1ZXJ5U2VsZWN0b3JBbGwoJy50YWInKS5mb3JFYWNoKGI9PmIub25jbGljaz0oKT0+c2V0VGFiKGIuZGF0YXNldC50YWIpKTtmdW5jdGlvbiBzZXRUYWIoaWQpe2RvY3VtZW50LnF1ZXJ5U2VsZWN0b3JBbGwoJy50YWInKS5mb3JFYWNoKHg9PnguY2xhc3NMaXN0LnRvZ2dsZSgnYWN0aXZlJyx4LmRhdGFzZXQudGFiPT09aWQpKTtkb2N1bWVudC5xdWVyeVNlbGVjdG9yQWxsKCcucGFuZWwnKS5mb3JFYWNoKHg9PnguY2xhc3NMaXN0LnRvZ2dsZSgnYWN0aXZlJyx4LmlkPT09aWQpKTtpZihpZD09PSdoaXN0b3J5UGFuZWwnKWxvYWRIaXN0b3J5KCl9JCgndGltZU1vZGUnKS5vbmNoYW5nZT0oKT0+eyQoJ21hbnVhbFRpbWVSb3cnKS5zdHlsZS5kaXNwbGF5PSQoJ3RpbWVNb2RlJykudmFsdWU9PT0nbWFudWFsJz8nZ3JpZCc6J25vbmUnfTskKCdjaG9vc2VPdXRwdXREaXInKS5vbmNsaWNrPWFzeW5jKCk9Pnt0cnl7Y29uc3Qgcj1hd2FpdCBhcGkoJy9hcGkvY2hvb3NlLWZvbGRlcicse21ldGhvZDonUE9TVCd9KTtpZihyLnBhdGgpJCgnb3V0cHV0RGlyJykudmFsdWU9ci5wYXRofWNhdGNoKGUpe2FsZXJ0KGUubWVzc2FnZSl9fTskKCdsb2FkU2Vzc2lvbnMnKS5vbmNsaWNrPWFzeW5jKCk9PnskKCdzZXNzaW9ucycpLmlubmVySFRNTD0nPHRyPjx0ZCBjb2xzcGFuPSI0Ij7or7vlj5bkuK0uLi48L3RkPjwvdHI+JzskKCdzZXNzaW9uQm94Jykuc3R5bGUuZGlzcGxheT0nYmxvY2snO3RyeXtjb25zdCBkYXRhPWF3YWl0IGFwaSgnL2FwaS9zZXNzaW9ucz9xPScrZW5jb2RlVVJJQ29tcG9uZW50KCQoJ3F1ZXJ5JykudmFsdWV8fCcnKSk7JCgnc2Vzc2lvbnMnKS5pbm5lckhUTUw9Jyc7Zm9yKGNvbnN0IHMgb2YgZGF0YS5zZXNzaW9ucyl7Y29uc3QgdHI9ZG9jdW1lbnQuY3JlYXRlRWxlbWVudCgndHInKTt0ci5jbGFzc05hbWU9J3NlbGVjdGFibGUnO3RyLmlubmVySFRNTD1gPHRkPiR7ZXNjYXBlSHRtbChzLnR5cGUpfTwvdGQ+PHRkPjxjb2RlPiR7ZXNjYXBlSHRtbChzLnVpbnx8cy5pZHx8JycpfTwvY29kZT48L3RkPjx0ZD48Y29kZT4ke2VzY2FwZUh0bWwocy51aWR8fCcnKX08L2NvZGU+PC90ZD48dGQ+JHtlc2NhcGVIdG1sKHMubmFtZXx8JycpfTwvdGQ+YDt0ci5vbmNsaWNrPSgpPT57ZG9jdW1lbnQucXVlcnlTZWxlY3RvckFsbCgnI3Nlc3Npb25zIHRyJykuZm9yRWFjaCh4PT54LmNsYXNzTGlzdC5yZW1vdmUoJ3NlbGVjdGVkJykpO3RyLmNsYXNzTGlzdC5hZGQoJ3NlbGVjdGVkJyk7ZmlsbFNlc3Npb24ocyl9OyQoJ3Nlc3Npb25zJykuYXBwZW5kQ2hpbGQodHIpfWlmKCFkYXRhLnNlc3Npb25zLmxlbmd0aCkkKCdzZXNzaW9ucycpLmlubmVySFRNTD0nPHRyPjx0ZCBjb2xzcGFuPSI0Ij7msqHmnInljLnphY3nu5PmnpzvvIzlj6/nm7TmjqXmiYvliqjovpPlhaXjgII8L3RkPjwvdHI+J31jYXRjaChlKXskKCdzZXNzaW9ucycpLmlubmVySFRNTD1gPHRyPjx0ZCBjb2xzcGFuPSI0Ij4ke2VzY2FwZUh0bWwoZS5tZXNzYWdlKX08L3RkPjwvdHI+YH19O2Z1bmN0aW9uIGZpbGxTZXNzaW9uKHMpe3NlbGVjdGVkU2Vzc2lvbj1zOyQoJ3Nlc3Npb25UeXBlJykudmFsdWU9cy50eXBlPT09J2dyb3VwJz8nZ3JvdXAnOidwcml2YXRlJzskKCdzZXNzaW9uSWQnKS52YWx1ZT1zLnVpbnx8cy5pZHx8cy51aWR8fCcnOyQoJ3Nlc3Npb25OYW1lJykudmFsdWU9cy5uYW1lfHxzLmlkfHwnJ30kKCdsb2FkSGlzdG9yeScpLm9uY2xpY2s9bG9hZEhpc3Rvcnk7YXN5bmMgZnVuY3Rpb24gbG9hZEhpc3RvcnkoKXskKCdoaXN0b3J5Um93cycpLmlubmVySFRNTD0nPGRpdiBjbGFzcz0iaGlzdG9yeS1jYXJkIj7or7vlj5bkuK0uLi48L2Rpdj4nO3RyeXtjb25zdCBxPWVuY29kZVVSSUNvbXBvbmVudCgkKCdoaXN0b3J5UXVlcnknKS52YWx1ZXx8JycpO2NvbnN0IGRhdGE9YXdhaXQgYXBpKCcvYXBpL2hpc3Rvcnk/cT0nK3EpOyQoJ2hpc3RvcnlSb3dzJykuaW5uZXJIVE1MPScnO2Zvcihjb25zdCByZWMgb2YgZGF0YS5oaXN0b3J5KWFkZEhpc3RvcnlDYXJkKHJlYyk7aWYoIWRhdGEuaGlzdG9yeS5sZW5ndGgpJCgnaGlzdG9yeVJvd3MnKS5pbm5lckhUTUw9JzxkaXYgY2xhc3M9Imhpc3RvcnktY2FyZCI+5rKh5pyJ5Y6G5Y+y6K6w5b2V44CCPC9kaXY+J31jYXRjaChlKXskKCdoaXN0b3J5Um93cycpLmlubmVySFRNTD1gPGRpdiBjbGFzcz0iaGlzdG9yeS1jYXJkIj4ke2VzY2FwZUh0bWwoZS5tZXNzYWdlKX08L2Rpdj5gfX1mdW5jdGlvbiBhZGRIaXN0b3J5Q2FyZChyZWMpe2NvbnN0IGRpdj1kb2N1bWVudC5jcmVhdGVFbGVtZW50KCdkaXYnKTtkaXYuY2xhc3NOYW1lPSdoaXN0b3J5LWNhcmQnO2NvbnN0IHRhZ3M9W3JlYy5zZXNzaW9uVHlwZSwuLi4ocmVjLmZvcm1hdHN8fFtdKV0uZmlsdGVyKEJvb2xlYW4pLm1hcCh4PT5gPHNwYW4gY2xhc3M9InBpbGwiPiR7ZXNjYXBlSHRtbCh4KX08L3NwYW4+YCkuam9pbignJyk7ZGl2LmlubmVySFRNTD1gPGRpdj4ke3RhZ3N9PHN0cm9uZz4ke2VzY2FwZUh0bWwocmVjLnNlc3Npb25OYW1lfHwnKOacquWRveWQjSknKX08L3N0cm9uZz48Y29kZT4ke2VzY2FwZUh0bWwocmVjLnNlc3Npb25JZHx8JycpfTwvY29kZT48L2Rpdj48ZGl2PiR7TnVtYmVyKHJlYy5tZXNzYWdlQ291bnR8fDApLnRvTG9jYWxlU3RyaW5nKCl9IOadoTwvZGl2PjxkaXY+PGNvZGU+JHtlc2NhcGVIdG1sKHJlYy5sYXRlc3RUaW1lfHwnJyl9PC9jb2RlPjxicj48Y29kZT4ke2VzY2FwZUh0bWwocmVjLm91dHB1dERpcnx8JycpfTwvY29kZT48L2Rpdj48ZGl2IGNsYXNzPSJhY3Rpb25zIj48YnV0dG9uIGRhdGEtYWN0PSJmaWxsIj7loavlhaU8L2J1dHRvbj48YnV0dG9uIGRhdGEtYWN0PSJpbmMiPuWinumHj+abtOaWsDwvYnV0dG9uPjxidXR0b24gZGF0YS1hY3Q9Im9wZW4iPuaJk+W8gOebruW9lTwvYnV0dG9uPjwvZGl2PmA7ZGl2LnF1ZXJ5U2VsZWN0b3IoJ1tkYXRhLWFjdD1maWxsXScpLm9uY2xpY2s9KCk9PntzZWxlY3RlZEhpc3Rvcnk9cmVjO3NlbGVjdGVkU2Vzc2lvbj1udWxsOyQoJ3Nlc3Npb25UeXBlJykudmFsdWU9cmVjLnNlc3Npb25UeXBlPT09J2dyb3VwJz8nZ3JvdXAnOidwcml2YXRlJzskKCdzZXNzaW9uSWQnKS52YWx1ZT1yZWMuc2Vzc2lvbklkfHwnJzskKCdzZXNzaW9uTmFtZScpLnZhbHVlPXJlYy5zZXNzaW9uTmFtZXx8Jyc7c2V0VGFiKCdleHBvcnRQYW5lbCcpfTtkaXYucXVlcnlTZWxlY3RvcignW2RhdGEtYWN0PWluY10nKS5vbmNsaWNrPSgpPT57c2VsZWN0ZWRIaXN0b3J5PXJlYztzZWxlY3RlZFNlc3Npb249bnVsbDskKCdzZXNzaW9uVHlwZScpLnZhbHVlPXJlYy5zZXNzaW9uVHlwZT09PSdncm91cCc/J2dyb3VwJzoncHJpdmF0ZSc7JCgnc2Vzc2lvbklkJykudmFsdWU9cmVjLnNlc3Npb25JZHx8Jyc7JCgnc2Vzc2lvbk5hbWUnKS52YWx1ZT1yZWMuc2Vzc2lvbk5hbWV8fCcnO3NldFRhYignZXhwb3J0UGFuZWwnKTtzdGFydEV4cG9ydCh0cnVlKX07ZGl2LnF1ZXJ5U2VsZWN0b3IoJ1tkYXRhLWFjdD1vcGVuXScpLm9uY2xpY2s9KCk9PmFwaSgnL2FwaS9vcGVuLWZvbGRlcicse21ldGhvZDonUE9TVCcsaGVhZGVyczp7J0NvbnRlbnQtVHlwZSc6J2FwcGxpY2F0aW9uL2pzb24nfSxib2R5OkpTT04uc3RyaW5naWZ5KHtwYXRoOnJlYy5vdXRwdXREaXJ9KX0pLmNhdGNoKGU9PmFsZXJ0KGUubWVzc2FnZSkpOyQoJ2hpc3RvcnlSb3dzJykuYXBwZW5kQ2hpbGQoZGl2KX0kKCdzdGFydEV4cG9ydCcpLm9uY2xpY2s9KCk9PnN0YXJ0RXhwb3J0KGZhbHNlKTthc3luYyBmdW5jdGlvbiBzdGFydEV4cG9ydChpbmNyZW1lbnRhbCl7Y29uc3QgdXNlSW5jPSEhaW5jcmVtZW50YWw7aWYodXNlSW5jJiYoIXNlbGVjdGVkSGlzdG9yeXx8IXNlbGVjdGVkSGlzdG9yeS5sYXRlc3RUaW1lc3RhbXApKXthbGVydCgn5rKh5pyJ5Y+v55So55qE5aKe6YeP5Z+65YeG44CCJyk7cmV0dXJufWNvbnN0IHBheWxvYWQ9e2NoYXRUeXBlOiQoJ3Nlc3Npb25UeXBlJykudmFsdWUsaWQ6JCgnc2Vzc2lvbklkJykudmFsdWUsc2Vzc2lvbklkOiQoJ3Nlc3Npb25JZCcpLnZhbHVlLHBlZXJVaWQ6c2VsZWN0ZWRTZXNzaW9uPy51aWR8fCcnLHVpbjpzZWxlY3RlZFNlc3Npb24/LnVpbnx8JCgnc2Vzc2lvbklkJykudmFsdWUsbmFtZTokKCdzZXNzaW9uTmFtZScpLnZhbHVlLHNlc3Npb25OYW1lOiQoJ3Nlc3Npb25OYW1lJykudmFsdWUsZm9ybWF0OiQoJ2Zvcm1hdCcpLnZhbHVlLHRpbWVQcmVzZXQ6dXNlSW5jPydhbGwnOiQoJ3RpbWVNb2RlJykudmFsdWUsZnJvbTokKCdzdGFydFRpbWUnKS52YWx1ZSx0bzokKCdlbmRUaW1lJykudmFsdWUsaW5jcmVtZW50YWw6dXNlSW5jLGluY3JlbWVudGFsQmFzZUlkOnVzZUluYz9zZWxlY3RlZEhpc3RvcnkuaWQ6bnVsbCxpbmNyZW1lbnRhbFN0YXJ0VGltZXN0YW1wOnVzZUluYz9zZWxlY3RlZEhpc3RvcnkubGF0ZXN0VGltZXN0YW1wOm51bGwsYmF0Y2hDb3VudDpOdW1iZXIoJCgnYmF0Y2hTaXplJykudmFsdWV8fDUwMCksbWF4TWVzc2FnZXM6TnVtYmVyKCQoJ21heE1lc3NhZ2VzJykudmFsdWV8fDApLHJlc291cmNlV29ya2VyczozLHBhcmFsbGVsSGlzdG9yeTokKCdwYXJhbGxlbEhpc3RvcnknKS5jaGVja2VkLGhpc3RvcnlXb3JrZXJzOk51bWJlcigkKCdoaXN0b3J5V29ya2VycycpLnZhbHVlfHw0KSxzZXFXaW5kb3c6TnVtYmVyKCQoJ3NlcVdpbmRvd1NpemUnKS52YWx1ZXx8NTAwMDApLG91dHB1dERpcjokKCdvdXRwdXREaXInKS52YWx1ZSxpbmNsdWRlU3lzdGVtOiQoJ2luY2x1ZGVTeXN0ZW1NZXNzYWdlcycpLmNoZWNrZWQsaW5jbHVkZVJlY2FsbGVkOiQoJ2luY2x1ZGVSZWNhbGxlZE1lc3NhZ2VzJykuY2hlY2tlZCxleHBvcnRSZXNvdXJjZXM6JCgnaW5jbHVkZVJlc291cmNlcycpLmNoZWNrZWQsaGlzdG9yeVJldHJ5Q291bnQ6TnVtYmVyKCQoJ2hpc3RvcnlSZXRyeUNvdW50JykudmFsdWV8fDUpfTtpZighU3RyaW5nKHBheWxvYWQuaWR8fHBheWxvYWQucGVlclVpZCkudHJpbSgpKXthbGVydCgn6K+35omL5Yqo6L6T5YWl5oiW6YCJ5oupIFFRL+e+pOWPt+OAgicpO3JldHVybn0kKCdyZXN1bHQnKS5pbm5lckhUTUw9Jyc7JCgnbG9ncycpLnRleHRDb250ZW50PScnOyQoJ3Byb2dyZXNzJykudmFsdWU9MDskKCdzdGF0dXNUZXh0JykudGV4dENvbnRlbnQ9J3BlbmRpbmcnOyQoJ3N0YXJ0RXhwb3J0JykuZGlzYWJsZWQ9dHJ1ZTskKCdzdG9wRXhwb3J0JykuZGlzYWJsZWQ9ZmFsc2U7dHJ5e2NvbnN0IHI9YXdhaXQgYXBpKCcvYXBpL2V4cG9ydCcse21ldGhvZDonUE9TVCcsaGVhZGVyczp7J0NvbnRlbnQtVHlwZSc6J2FwcGxpY2F0aW9uL2pzb24nfSxib2R5OkpTT04uc3RyaW5naWZ5KHBheWxvYWQpfSk7Y3VycmVudEpvYj1yLmpvYklkfHxyLnRhc2tJZDtwb2xsKCk7dGltZXI9c2V0SW50ZXJ2YWwocG9sbCwxMjAwKX1jYXRjaChlKXthbGVydChlLm1lc3NhZ2UpOyQoJ3N0YXJ0RXhwb3J0JykuZGlzYWJsZWQ9ZmFsc2U7JCgnc3RvcEV4cG9ydCcpLmRpc2FibGVkPXRydWV9fSQoJ3N0b3BFeHBvcnQnKS5vbmNsaWNrPWFzeW5jKCk9PntpZighY3VycmVudEpvYilyZXR1cm47JCgnc3RvcEV4cG9ydCcpLmRpc2FibGVkPXRydWU7dHJ5e2F3YWl0IGFwaSgnL2FwaS9qb2JzLycrY3VycmVudEpvYisnL3N0b3AnLHttZXRob2Q6J1BPU1QnfSl9Y2F0Y2goZSl7YWxlcnQoZS5tZXNzYWdlKX19O2FzeW5jIGZ1bmN0aW9uIHBvbGwoKXtpZighY3VycmVudEpvYilyZXR1cm47dHJ5e2NvbnN0IHI9YXdhaXQgYXBpKCcvYXBpL2pvYnMvJytjdXJyZW50Sm9iKTtjb25zdCBqPXIuam9iOyQoJ3Byb2dyZXNzJykudmFsdWU9ai5wcm9ncmVzc3x8MDskKCdzdGF0dXNUZXh0JykudGV4dENvbnRlbnQ9ai5zdGF0dXN8fCcnOyQoJ21lc3NhZ2UnKS50ZXh0Q29udGVudD1qLm1lc3NhZ2V8fCcnOyQoJ2xvZ3MnKS50ZXh0Q29udGVudD0oai5sb2dzfHxbXSkuam9pbignXG4nKTskKCdsb2dzJykuc2Nyb2xsVG9wPSQoJ2xvZ3MnKS5zY3JvbGxIZWlnaHQ7aWYoWydjb21wbGV0ZWQnLCdzdG9wcGVkJ10uaW5jbHVkZXMoai5zdGF0dXMpKXtjbGVhckludGVydmFsKHRpbWVyKTt0aW1lcj1udWxsOyQoJ3N0YXJ0RXhwb3J0JykuZGlzYWJsZWQ9ZmFsc2U7JCgnc3RvcEV4cG9ydCcpLmRpc2FibGVkPXRydWU7Y29uc3QgcGF0aHM9KChqLnJlc3VsdCYmai5yZXN1bHQucGF0aHMpfHxbXSkubWFwKHA9PmA8bGk+PGNvZGU+JHtlc2NhcGVIdG1sKHApfTwvY29kZT48L2xpPmApLmpvaW4oJycpOyQoJ3Jlc3VsdCcpLmlubmVySFRNTD1gPHA+PHN0cm9uZz4ke2ouc3RhdHVzPT09J3N0b3BwZWQnPyflt7Lkv53lrZjpg6jliIbnu5PmnpwnOiflrozmiJAnfTwvc3Ryb25nPjwvcD48cD7nm67lvZXvvJo8Y29kZT4ke2VzY2FwZUh0bWwoai5yZXN1bHQ/Lm91dHB1dERpcnx8JycpfTwvY29kZT48L3A+PHVsPiR7cGF0aHN9PC91bD5gO2xvYWRIaXN0b3J5KCkuY2F0Y2goKCk9Pnt9KX1lbHNlIGlmKGouc3RhdHVzPT09J2ZhaWxlZCcpe2NsZWFySW50ZXJ2YWwodGltZXIpO3RpbWVyPW51bGw7JCgnc3RhcnRFeHBvcnQnKS5kaXNhYmxlZD1mYWxzZTskKCdzdG9wRXhwb3J0JykuZGlzYWJsZWQ9dHJ1ZTskKCdyZXN1bHQnKS5pbm5lckhUTUw9YDxwPjxzdHJvbmc+5aSx6LSl77yaPC9zdHJvbmc+JHtlc2NhcGVIdG1sKGouZXJyb3J8fGoubWVzc2FnZXx8J3Vua25vd24nKX08L3A+YH19Y2F0Y2goZSl7JCgnbWVzc2FnZScpLnRleHRDb250ZW50PWUubWVzc2FnZX19ZnVuY3Rpb24gZXNjYXBlSHRtbChzKXtyZXR1cm4gU3RyaW5nKHM/PycnKS5yZXBsYWNlKC9bJjw+IiddL2csYz0+KHsnJic6JyZhbXA7JywnPCc6JyZsdDsnLCc+JzonJmd0OycsJyInOicmcXVvdDsnLCInIjonJiMwMzk7J31bY10pKX0KPC9zY3JpcHQ+PC9ib2R5PjwvaHRtbD4=', 'base64').toString('utf8');

async function requestHandler(req, res) {
  const urlObj = new URL(req.url, `http://127.0.0.1:${state.port || DEFAULT_PORT}`);
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
      server.listen(port, '127.0.0.1', () => resolve({ server, port }));
    };
    tryPort(startPort);
  });
}

function openWebUi() {
  if (!state.port) return;
  electron.shell.openExternal(`http://127.0.0.1:${state.port}/`);
}



function setupPluginIpcHandlers() {
  try { electron.ipcMain.removeHandler('qq_export_open_web'); } catch {}
  try { electron.ipcMain.removeHandler('qq_export_status'); } catch {}
  electron.ipcMain.handle('qq_export_open_web', async () => {
    if (!state.started || !state.port) await start();
    openWebUi();
    return { ok: true, url: `http://127.0.0.1:${state.port}/` };
  });
  electron.ipcMain.handle('qq_export_status', async () => ({ ok: true, port: state.port, url: state.port ? `http://127.0.0.1:${state.port}/` : '' }));
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
  log(`web ui listening on http://127.0.0.1:${port}/`);
}

start().catch(err => log('startup failed', err?.stack || err));

exports.onBrowserWindowCreated = function onBrowserWindowCreated() {
  // Kept for LiteLoader compatibility. The web UI is opened from the plugin settings panel.
};

exports.onUnload = function onUnload() {
  try { state.server?.close(); } catch {}
};
