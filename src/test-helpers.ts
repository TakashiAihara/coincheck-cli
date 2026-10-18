import { existsSync, mkdtempSync, readdirSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { gunzipSync } from "node:zlib";
import { createHash } from "node:crypto";
import type { ServerWebSocket } from "bun";

export type Received = { conn: number; at: number; text: string };

export type FakeServer = {
  url: string;
  // 同じポートで REST も受ける (http://127.0.0.1:<port>)
  apiUrl: string;
  restRequests: string[];
  // REST の応答をテストから差し替える。既定は板のスナップショットを返す
  rest: (path: string) => Response | Promise<Response>;
  received: Received[];
  connections: ServerWebSocket<{ id: number }>[];
  opened: number;
  closed: number;
  send(text: string, conn?: number): void;
  closeConn(conn?: number): void;
  stop(): void;
};

// 偽の Public WebSocket と REST。テストから送信とサーバー側の切断を操作する
export function startFakeServer(): FakeServer {
  const state: FakeServer = {
    url: "",
    apiUrl: "",
    restRequests: [],
    rest: () => Response.json({ asks: [["13000000.0", "0.1"]], bids: [["12990000.0", "0.2"]] }),
    received: [],
    connections: [],
    opened: 0,
    closed: 0,
    send(text, conn) {
      const ws = state.connections[conn ?? state.connections.length - 1];
      if (!ws) throw new Error("no connection");
      ws.send(text);
    },
    closeConn(conn) {
      state.connections[conn ?? state.connections.length - 1]?.close();
    },
    stop() {
      server.stop(true);
    },
  };
  const server = Bun.serve<{ id: number }>({
    hostname: "127.0.0.1",
    port: 0,
    fetch(req, srv) {
      if (req.headers.get("upgrade") === "websocket") {
        if (srv.upgrade(req, { data: { id: state.opened } })) return undefined;
        return new Response("upgrade failed", { status: 400 });
      }
      const url = new URL(req.url);
      const path = url.pathname + url.search;
      state.restRequests.push(path);
      return state.rest(path);
    },
    websocket: {
      open(ws) {
        state.connections.push(ws);
        state.opened++;
      },
      message(ws, msg) {
        state.received.push({ conn: ws.data.id, at: Date.now(), text: String(msg) });
      },
      close() {
        state.closed++;
      },
    },
  });
  state.url = `ws://127.0.0.1:${server.port}`;
  state.apiUrl = `http://127.0.0.1:${server.port}`;
  return state;
}

export async function waitFor(cond: () => boolean, timeoutMs = 3000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!cond()) {
    if (Date.now() > deadline) throw new Error(`waitFor timed out after ${timeoutMs}ms`);
    await Bun.sleep(10);
  }
}

export function tempDir(): { path: string; cleanup(): void } {
  const path = mkdtempSync(join(tmpdir(), "coincheck-test-"));
  return { path, cleanup: () => rmSync(path, { recursive: true, force: true }) };
}

// 受け付けるだけで黙る TCP サーバー。handshake: false なら upgrade を返さず、true なら
// upgrade だけ返してその後は close フレームにも応えない (応答しなくなった相手を作る)
export function startSilentTcpServer(opts: { handshake?: boolean } = {}): {
  url: string;
  accepts: number[];
  closes: number[];
  frames: number;
  stop(): void;
} {
  const accepts: number[] = [];
  const closes: number[] = [];
  let frames = 0;
  let upgraded = false;
  const listener = Bun.listen({
    hostname: "127.0.0.1",
    port: 0,
    socket: {
      open() {
        accepts.push(Date.now());
      },
      data(socket, data) {
        if (!opts.handshake) return;
        if (upgraded) {
          frames++;
          return;
        }
        const key = /Sec-WebSocket-Key: (.+)\r\n/i.exec(data.toString())?.[1];
        if (!key) return;
        const accept = createHash("sha1")
          .update(`${key}258EAFA5-E914-47DA-95CA-C5AB0DC85B11`)
          .digest("base64");
        socket.write(
          "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n" +
            `Sec-WebSocket-Accept: ${accept}\r\n\r\n`,
        );
        upgraded = true;
      },
      close() {
        closes.push(Date.now());
      },
    },
  });
  return {
    url: `ws://127.0.0.1:${listener.port}`,
    accepts,
    closes,
    get frames() {
      return frames;
    },
    stop: () => listener.stop(true),
  };
}

export function readRawLines(path: string): string[] {
  if (!existsSync(path)) return [];
  return gunzipSync(readFileSync(path))
    .toString("utf8")
    .split("\n")
    .filter((l) => l.length > 0);
}

// その日の stream のファイル (<date>T*.jsonl.gz) を名前順にすべて読む
export function dayFiles(root: string, stream: string, date: string): string[] {
  const d = join(root, stream);
  if (!existsSync(d)) return [];
  return readdirSync(d)
    .filter((f) => f.startsWith(`${date}T`) && f.endsWith(".jsonl.gz"))
    .sort()
    .map((f) => join(d, f));
}

export function readDayRaw(root: string, stream: string, date: string): string[] {
  return dayFiles(root, stream, date).flatMap(readRawLines);
}

export function readDay(
  root: string,
  stream: string,
  date: string,
): { recv_at: string; msg: unknown }[] {
  return dayFiles(root, stream, date).flatMap(readRecords);
}

export function readRecords(path: string): { recv_at: string; msg: unknown }[] {
  if (!existsSync(path)) return [];
  const text = gunzipSync(readFileSync(path)).toString("utf8");
  return text
    .split("\n")
    .filter((l) => l.length > 0)
    .map((l) => JSON.parse(l));
}
