import { appendFileSync, mkdirSync, writeFileSync } from "node:fs";
import { dirname } from "node:path";
import { gzipSync } from "node:zlib";
import { recordPath } from "./sink.ts";

export type Channel = "orderbook" | "trades";

export type Stream = Channel | "orderbook_snapshot";

export const CHANNELS: readonly Channel[] = ["orderbook", "trades"];

export const DEFAULT_PUBLIC_WS_URL = "wss://ws-api.coincheck.com";

export const DEFAULT_API_URL = "https://coincheck.com";

// pair はファイルのパスに使うので、`../` などで --out の外に書かせない形だけを通す
export const PAIR_PATTERN = /^[a-z0-9_]+$/;

export type Subscription = { channel: Channel; pair: string };

export type RecorderOptions = {
  url: string;
  apiUrl: string;
  subscriptions: Subscription[];
  outDir: string;
  snapshotIntervalMs?: number;
  snapshotTimeoutMs?: number;
  backoffInitialMs?: number;
  backoffMaxMs?: number;
  idleTimeoutMs?: number;
  statsIntervalMs?: number;
  flushIntervalMs?: number;
  now?: () => Date;
  log?: (line: string) => void;
  onFatal?: (err: Error) => void;
};

export type Recorder = {
  stop(): Promise<void>;
};

function isObject(v: unknown): v is object {
  return typeof v === "object" && v !== null && !Array.isArray(v);
}

// ログに出す生テキストは 1 行に収める。改行を残すとログの行が割れて読めなくなる
const head = (text: string) => text.slice(0, 200).replace(/[\r\n]/g, " ");

function isTrade(v: unknown): v is unknown[] {
  return Array.isArray(v);
}

function parseAccepted(text: string): { stream: Channel; pair: string } | undefined {
  let msg: unknown;
  try {
    msg = JSON.parse(text);
  } catch {
    return undefined;
  }
  if (!Array.isArray(msg)) return undefined;

  const [first, second] = msg;
  if (
    msg.length === 2 &&
    typeof first === "string" &&
    PAIR_PATTERN.test(first) &&
    isObject(second)
  ) {
    return { stream: "orderbook", pair: first };
  }

  // trades は約定 1 件が [unix 秒, id, pair, ...] の配列で、1 メッセージは同じ pair の約定の並び
  if (msg.length === 0 || !msg.every(isTrade)) return undefined;
  const pair = msg[0]?.[2];
  if (typeof pair !== "string" || !PAIR_PATTERN.test(pair)) return undefined;
  if (!msg.every((trade) => trade[2] === pair)) return undefined;
  return { stream: "trades", pair };
}

// 板の REST 応答は asks と bids の配列を持つ object。それ以外は差分の基準にならないので捨てる
function isBook(text: string): boolean {
  let body: unknown;
  try {
    body = JSON.parse(text);
  } catch {
    return false;
  }
  if (!isObject(body)) return false;
  const { asks, bids } = body as Record<string, unknown>;
  return Array.isArray(asks) && Array.isArray(bids);
}

export function startRecorder(opts: RecorderOptions): Recorder {
  const {
    url,
    apiUrl,
    outDir,
    snapshotIntervalMs = 60_000,
    snapshotTimeoutMs = 10_000,
    backoffInitialMs = 1000,
    backoffMaxMs = 60_000,
    idleTimeoutMs = 60_000,
    statsIntervalMs = 60_000,
    flushIntervalMs = 5000,
    now = () => new Date(),
    log = (line: string) => console.error(line),
  } = opts;
  const onFatal = opts.onFatal ?? ((err: Error) => log(`fatal: ${err.message}`));

  const subscriptions = [
    ...new Map(opts.subscriptions.map((s) => [`${s.channel}/${s.pair}`, s])).values(),
  ];
  const snapshotPairs = subscriptions.filter((s) => s.channel === "orderbook").map((s) => s.pair);

  // 購読している stream は 0 件でも stats に出すため、起動時に 0 で置いておく
  const counts = new Map<string, number>();
  for (const s of subscriptions) {
    counts.set(`${s.channel}/${s.pair}`, 0);
    if (s.channel === "orderbook") counts.set(`orderbook_snapshot/${s.pair}`, 0);
  }
  const buffers = new Map<string, string[]>();
  // 自分が作ったファイルにしか書き足さないため、(stream, UTC 日付) ごとに最初の受信時刻で決めた path を固定する
  const paths = new Map<string, string>();
  const created = new Set<string>();
  const snapshotInFlight = new Set<string>();
  // 再接続の時点で取得中なら、その応答は切断より前の板かもしれないので、終わったら取り直す
  const snapshotAgain = new Set<string>();

  let ws: WebSocket | undefined;
  let stopped = false;
  let backoff = backoffInitialMs;
  let idleTimer: ReturnType<typeof setTimeout> | undefined;
  let reconnectTimer: ReturnType<typeof setTimeout> | undefined;

  const flushTimer = setInterval(flush, flushIntervalMs);
  const statsTimer = setInterval(() => {
    log(`stats ${[...counts].map(([k, n]) => `${k}=${n}`).join(" ")}`);
  }, statsIntervalMs);
  // WebSocket の状態とは関係なく取る。板の差分が止まっていても REST は返るため
  const snapshotTimer = setInterval(() => snapshotAll(), snapshotIntervalMs);

  function closeSocket(sock: WebSocket) {
    sock.onopen = sock.onmessage = sock.onclose = sock.onerror = null;
    try {
      sock.close();
    } catch {}
  }

  function halt() {
    stopped = true;
    clearInterval(flushTimer);
    clearInterval(statsTimer);
    clearInterval(snapshotTimer);
    clearTimeout(idleTimer);
    clearTimeout(reconnectTimer);
    if (ws) closeSocket(ws);
    ws = undefined;
  }

  function flush() {
    const pending = [...buffers];
    buffers.clear();
    let written = 0;
    try {
      for (const [path, lines] of pending) {
        mkdirSync(dirname(path), { recursive: true });
        const member = gzipSync(lines.join(""));
        if (created.has(path)) appendFileSync(path, member);
        else {
          writeFileSync(path, member, { flag: "wx" });
          created.add(path);
        }
        written++;
      }
    } catch (e) {
      const err = e instanceof Error ? e : new Error(String(e));
      const dropped = pending.slice(written).reduce((n, [, lines]) => n + lines.length, 0);
      halt();
      log(`write failed, dropped ${dropped} line(s): ${err.message}`);
      onFatal(err);
    }
  }

  function armIdle() {
    clearTimeout(idleTimer);
    idleTimer = setTimeout(() => {
      if (ws) drop(ws, `no valid message for ${idleTimeoutMs}ms`);
    }, idleTimeoutMs);
  }

  function drop(sock: WebSocket, reason: string) {
    log(`disconnected: ${reason}`);
    closeSocket(sock);
    ws = undefined;
    clearTimeout(idleTimer);

    const wait = backoff;
    backoff = Math.min(backoff * 2, backoffMaxMs);
    reconnectTimer = setTimeout(connect, wait);
  }

  function append(stream: Stream, pair: string, at: Date, text: string) {
    const key = `${stream}/${pair}`;
    counts.set(key, (counts.get(key) ?? 0) + 1);

    const pathKey = `${key}/${at.toISOString().slice(0, 10)}`;
    let path = paths.get(pathKey);
    if (!path) {
      path = recordPath(outDir, stream, pair, at);
      paths.set(pathKey, path);
    }
    // JSON の文字列の中に生の改行は入れられないので、置き換わるのは構造の空白だけ
    const line = `{"recv_at":"${at.toISOString()}","msg":${text.replace(/[\r\n]/g, " ")}}\n`;
    const lines = buffers.get(path);
    if (lines) lines.push(line);
    else buffers.set(path, [line]);
  }

  function receive(text: string) {
    const at = now();
    const accepted = parseAccepted(text);
    if (!accepted) {
      log(`skipped message: ${head(text)}`);
      return;
    }

    armIdle();
    backoff = backoffInitialMs;
    append(accepted.stream, accepted.pair, at, text);
  }

  function subscribeAll(sock: WebSocket) {
    for (const s of subscriptions) {
      sock.send(JSON.stringify({ type: "subscribe", channel: `${s.pair}-${s.channel}` }));
    }
  }

  function snapshotAll(afterReconnect = false) {
    for (const pair of snapshotPairs) snapshot(pair, afterReconnect);
  }

  function snapshot(pair: string, afterReconnect: boolean) {
    // 前の取得が終わる前に次を始めると、応答が遅いときに取得が積み上がる
    if (snapshotInFlight.has(pair)) {
      if (afterReconnect) snapshotAgain.add(pair);
      return;
    }
    snapshotInFlight.add(pair);
    void fetchSnapshot(pair).finally(() => {
      snapshotInFlight.delete(pair);
      if (snapshotAgain.delete(pair)) snapshot(pair, false);
    });
  }

  async function fetchSnapshot(pair: string) {
    const fail = (reason: string) => log(`snapshot failed for ${pair}: ${reason}`);
    try {
      const res = await fetch(`${apiUrl}/api/order_books?pair=${pair}`, {
        signal: AbortSignal.timeout(snapshotTimeoutMs),
      });
      const text = await res.text();
      if (res.status !== 200) {
        fail(`status ${res.status}: ${head(text)}`);
        return;
      }
      if (!isBook(text)) {
        fail(`unexpected body: ${head(text)}`);
        return;
      }
      // ヘッダーだけ届いた時点では板の全体が手元に無いので、本文を読み切った時刻を受信時刻にする
      append("orderbook_snapshot", pair, now(), text);
    } catch (e) {
      fail(e instanceof Error ? e.message : String(e));
    }
  }

  function connect() {
    if (stopped) return;
    const sock = new WebSocket(url);
    ws = sock;
    armIdle();

    sock.onopen = () => {
      subscribeAll(sock);
      snapshotAll(true);
    };
    sock.onmessage = (ev) => {
      if (typeof ev.data === "string") receive(ev.data);
      else log("skipped message: non-text frame");
    };
    sock.onclose = () => drop(sock, "closed");
    sock.onerror = () => drop(sock, "error");
  }

  connect();

  return {
    async stop() {
      if (stopped) return;
      flush();
      halt();
    },
  };
}
