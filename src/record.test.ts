import { afterEach, describe, expect, test } from "bun:test";
import { mkdirSync, readdirSync, readFileSync, writeFileSync } from "node:fs";
import { gzipSync } from "node:zlib";
import { join } from "node:path";
import { startRecorder, type Recorder, type RecorderOptions, type Subscription } from "./record.ts";
import { recordPath } from "./sink.ts";
import {
  readDay,
  readDayRaw,
  readRecords,
  startFakeServer,
  startSilentTcpServer,
  tempDir,
  waitFor,
  type FakeServer,
} from "./test-helpers.ts";

// UTC 日付で分けることを、ローカル日付と食い違う TZ で確かめる (CI の既定は UTC)
process.env.TZ = "Asia/Tokyo";

const OB_BTC = [
  "btc_jpy",
  {
    bids: [
      ["12923124.0", "0"],
      ["12924207.0", "0.02302683"],
    ],
    asks: [["12927674.0", "0.03"]],
    last_update_at: "1792298108",
  },
];

function trade(pair: string, price: string, amount = "0.03") {
  return [["1792298110", "289000001", pair, price, amount, "buy", "5001", "5002", null]];
}

const TRADES_BTC = trade("btc_jpy", "12927674.0");
const TRADES_ETH = trade("eth_jpy", "382228.0");
const SNAPSHOT = { asks: [["12927084.0", "0.01"]], bids: [["12926000.0", "0.5"]] };

let server: FakeServer;
let dir: { path: string; cleanup(): void };
let recorder: Recorder | undefined;
let logs: string[];
let fatals: Error[];

function start(over: Partial<RecorderOptions> & { subscriptions: Subscription[] }): Recorder {
  recorder = startRecorder({
    url: server.url,
    apiUrl: server.apiUrl,
    outDir: dir.path,
    backoffInitialMs: 50,
    backoffMaxMs: 400,
    idleTimeoutMs: 10_000,
    statsIntervalMs: 60_000,
    flushIntervalMs: 50,
    snapshotIntervalMs: 60_000,
    snapshotTimeoutMs: 1000,
    log: (l) => logs.push(l),
    onFatal: (e) => fatals.push(e),
    ...over,
  });
  return recorder;
}

function sentOn(conn: number) {
  return server.received.filter((r) => r.conn === conn);
}

function subscribeCommands(subs: Subscription[]) {
  return subs.map((s) => ({ type: "subscribe", channel: `${s.pair}-${s.channel}` }));
}

function listFiles(root: string): string[] {
  return readdirSync(root, { recursive: true, withFileTypes: true })
    .filter((e) => e.isFile())
    .map((e) => join(e.parentPath, e.name).slice(root.length + 1))
    .sort();
}

function setup() {
  server = startFakeServer();
  dir = tempDir();
  logs = [];
  fatals = [];
  recorder = undefined;
}

afterEach(async () => {
  await recorder?.stop();
  // recordPath のテストは setup を呼ばない
  server?.stop();
  dir?.cleanup();
});

describe("recordPath", () => {
  test("names a file stream/pair/<UTC date>T<HHMMSS.mmm>Z.jsonl.gz after its first receive time", () => {
    expect(recordPath("/x", "orderbook", "btc_jpy", new Date("2026-10-12T23:59:59.999Z"))).toBe(
      "/x/orderbook/btc_jpy/2026-10-12T235959.999Z.jsonl.gz",
    );
    expect(
      recordPath("/x", "orderbook_snapshot", "eth_jpy", new Date("2026-10-13T00:00:00.000Z")),
    ).toBe("/x/orderbook_snapshot/eth_jpy/2026-10-13T000000.000Z.jsonl.gz");
  });

  test("uses UTC even where the local date differs", () => {
    // JST では 2026-10-13 05:00
    expect(recordPath("/x", "trades", "btc_jpy", new Date("2026-10-12T20:00:00.000Z"))).toBe(
      "/x/trades/btc_jpy/2026-10-12T200000.000Z.jsonl.gz",
    );
  });
});

describe("subscribing", () => {
  test("subscribes every pair and channel in order once the connection opens", async () => {
    setup();
    const subs: Subscription[] = [
      { channel: "orderbook", pair: "btc_jpy" },
      { channel: "trades", pair: "btc_jpy" },
      { channel: "trades", pair: "eth_jpy" },
    ];
    start({ subscriptions: subs });
    await waitFor(() => server.received.length >= 3);
    await Bun.sleep(200);

    expect(server.received.map((r) => JSON.parse(r.text))).toEqual(subscribeCommands(subs));
  });

  test("subscribes a repeated pair only once", async () => {
    setup();
    start({
      subscriptions: [
        { channel: "trades", pair: "btc_jpy" },
        { channel: "trades", pair: "btc_jpy" },
        { channel: "orderbook", pair: "btc_jpy" },
      ],
    });
    await waitFor(() => server.received.length >= 2);
    await Bun.sleep(300);
    expect(server.received.map((r) => JSON.parse(r.text))).toEqual(
      subscribeCommands([
        { channel: "trades", pair: "btc_jpy" },
        { channel: "orderbook", pair: "btc_jpy" },
      ]),
    );
  });
});

describe("writing", () => {
  test("writes each message verbatim with the receive time to its stream/pair/day file", async () => {
    setup();
    const recvAt = new Date("2026-03-04T05:06:07.890Z");
    start({
      subscriptions: [
        { channel: "orderbook", pair: "btc_jpy" },
        { channel: "trades", pair: "btc_jpy" },
      ],
      now: () => recvAt,
    });
    await waitFor(() => server.opened === 1);
    server.send(JSON.stringify(OB_BTC));
    server.send(JSON.stringify(TRADES_BTC));
    server.send(JSON.stringify(TRADES_ETH));
    await Bun.sleep(200);
    await recorder!.stop();

    expect(readDay(dir.path, "orderbook/btc_jpy", "2026-03-04")).toEqual([
      { recv_at: "2026-03-04T05:06:07.890Z", msg: OB_BTC },
    ]);
    expect(readDay(dir.path, "trades/btc_jpy", "2026-03-04")).toEqual([
      { recv_at: "2026-03-04T05:06:07.890Z", msg: TRADES_BTC },
    ]);
    // 購読していない組でも、届いたものは捨てずに書く
    expect(readDay(dir.path, "trades/eth_jpy", "2026-03-04")).toEqual([
      { recv_at: "2026-03-04T05:06:07.890Z", msg: TRADES_ETH },
    ]);
  });

  test("keeps the received text as is, without re-serializing it", async () => {
    setup();
    const recvAt = new Date("2026-10-12T10:00:00.000Z");
    start({ subscriptions: [{ channel: "trades", pair: "btc_jpy" }], now: () => recvAt });
    await waitFor(() => server.opened === 1);
    const raw = '[["1792298110", "1","btc_jpy",1.0,12345678901234567890,"buy","1","2",null]]';
    server.send(raw);
    await Bun.sleep(150);
    await recorder!.stop();

    expect(readDayRaw(dir.path, "trades/btc_jpy", "2026-10-12")).toEqual([
      `{"recv_at":"2026-10-12T10:00:00.000Z","msg":${raw}}`,
    ]);
  });

  test("turns line breaks inside the received JSON into spaces so a record stays one line", async () => {
    setup();
    const recvAt = new Date("2026-10-12T10:00:00.000Z");
    start({ subscriptions: [{ channel: "orderbook", pair: "btc_jpy" }], now: () => recvAt });
    await waitFor(() => server.opened === 1);
    server.send(JSON.stringify(OB_BTC, null, 2).replaceAll("\n", "\r\n"));
    await Bun.sleep(150);
    await recorder!.stop();

    const lines = readDayRaw(dir.path, "orderbook/btc_jpy", "2026-10-12");
    expect(lines.length).toBe(1);
    expect(JSON.parse(lines[0]!).msg).toEqual(OB_BTC);
    // 消すのではなく空白に置き換える。CR と LF の 2 つに、インデントの 2 つが続く
    expect(lines[0]).toContain('[    "btc_jpy",    {');
  });

  test("starts a new file when the UTC date of the receive time changes", async () => {
    setup();
    let current = new Date("2026-10-12T23:59:59.900Z");
    // flush を挟まない。日付は書き込み時刻ではなく受信時刻で決まる
    start({
      subscriptions: [{ channel: "trades", pair: "btc_jpy" }],
      now: () => current,
      flushIntervalMs: 60_000,
    });
    await waitFor(() => server.opened === 1);
    server.send(JSON.stringify(trade("btc_jpy", "13000001.0")));
    await Bun.sleep(150);
    current = new Date("2026-10-13T00:00:00.100Z");
    server.send(JSON.stringify(trade("btc_jpy", "13300001.0")));
    await Bun.sleep(150);
    await recorder!.stop();

    const day1 = readDay(dir.path, "trades/btc_jpy", "2026-10-12");
    const day2 = readDay(dir.path, "trades/btc_jpy", "2026-10-13");
    expect(day1.map((r) => r.msg)).toEqual([trade("btc_jpy", "13000001.0")]);
    expect(day2.map((r) => r.msg)).toEqual([trade("btc_jpy", "13300001.0")]);
    expect(day2[0]!.recv_at).toBe("2026-10-13T00:00:00.100Z");
    expect(listFiles(dir.path)).toEqual([
      "trades/btc_jpy/2026-10-12T235959.900Z.jsonl.gz",
      "trades/btc_jpy/2026-10-13T000000.100Z.jsonl.gz",
    ]);
  });

  test("makes flushed records readable on disk while still running", async () => {
    setup();
    const recvAt = new Date("2026-10-12T10:00:00.000Z");
    start({ subscriptions: [{ channel: "trades", pair: "btc_jpy" }], now: () => recvAt });
    await waitFor(() => server.opened === 1);
    server.send(JSON.stringify(TRADES_BTC));
    await waitFor(() => {
      try {
        return readDay(dir.path, "trades/btc_jpy", "2026-10-12").length === 1;
      } catch {
        return false;
      }
    }, 1000);
  });

  test("stop writes what is buffered without waiting for the flush interval", async () => {
    setup();
    const recvAt = new Date("2026-10-12T10:00:00.000Z");
    start({
      subscriptions: [{ channel: "trades", pair: "btc_jpy" }],
      now: () => recvAt,
      flushIntervalMs: 60_000,
    });
    await waitFor(() => server.opened === 1);
    server.send(JSON.stringify(TRADES_BTC));
    await Bun.sleep(100);
    await recorder!.stop();
    expect(readDay(dir.path, "trades/btc_jpy", "2026-10-12").length).toBe(1);
  });

  test("keeps one file per stream and day for the life of the process", async () => {
    setup();
    let current = new Date("2026-10-12T10:00:00.000Z");
    start({ subscriptions: [{ channel: "trades", pair: "btc_jpy" }], now: () => current });
    await waitFor(() => server.opened === 1);
    server.send(JSON.stringify(trade("btc_jpy", "13000001.0", "0.0001")));
    await Bun.sleep(150);
    current = new Date("2026-10-12T11:30:00.000Z");
    server.send(JSON.stringify(trade("btc_jpy", "13000001.0", "0.333")));
    await Bun.sleep(150);
    await recorder!.stop();

    expect(listFiles(dir.path)).toEqual(["trades/btc_jpy/2026-10-12T100000.000Z.jsonl.gz"]);
    expect(readDay(dir.path, "trades/btc_jpy", "2026-10-12").map((r) => r.msg)).toEqual([
      trade("btc_jpy", "13000001.0", "0.0001"),
      trade("btc_jpy", "13000001.0", "0.333"),
    ]);
  });

  test("writes to a new file after a restart instead of appending to the earlier one", async () => {
    setup();
    start({
      subscriptions: [{ channel: "trades", pair: "btc_jpy" }],
      now: () => new Date("2026-10-12T10:00:00.000Z"),
    });
    await waitFor(() => server.opened === 1);
    server.send(JSON.stringify(trade("btc_jpy", "13000001.0", "0.0001")));
    await Bun.sleep(150);
    await recorder!.stop();

    start({
      subscriptions: [{ channel: "trades", pair: "btc_jpy" }],
      now: () => new Date("2026-10-12T10:05:00.000Z"),
    });
    await waitFor(() => server.opened === 2);
    server.send(JSON.stringify(trade("btc_jpy", "13000001.0", "0.777")));
    await Bun.sleep(150);
    await recorder!.stop();

    const rows = readDay(dir.path, "trades/btc_jpy", "2026-10-12");
    expect(rows.map((r) => r.msg)).toEqual([
      trade("btc_jpy", "13000001.0", "0.0001"),
      trade("btc_jpy", "13000001.0", "0.777"),
    ]);
    expect(listFiles(dir.path)).toEqual([
      "trades/btc_jpy/2026-10-12T100000.000Z.jsonl.gz",
      "trades/btc_jpy/2026-10-12T100500.000Z.jsonl.gz",
    ]);
  });

  test("leaves a file from an earlier run untouched, even with a torn tail", async () => {
    setup();
    mkdirSync(join(dir.path, "trades/btc_jpy"), { recursive: true });
    const earlier = join(dir.path, "trades/btc_jpy/2026-10-12T090000.000Z.jsonl.gz");
    const member = gzipSync(
      `{"recv_at":"2026-10-12T09:00:00.000Z","msg":${JSON.stringify(TRADES_BTC)}}\n`,
    );
    // 完結したメンバーの後ろに、途中で切れたメンバーが残っている状態
    const torn = Buffer.concat([member, member.subarray(0, member.length - 6)]);
    writeFileSync(earlier, torn);

    start({
      subscriptions: [{ channel: "trades", pair: "btc_jpy" }],
      now: () => new Date("2026-10-12T10:00:00.000Z"),
    });
    await waitFor(() => server.opened === 1);
    server.send(JSON.stringify(trade("btc_jpy", "13000001.0", "0.555")));
    await Bun.sleep(150);
    await recorder!.stop();

    expect(readFileSync(earlier)).toEqual(torn);
    expect(
      readRecords(join(dir.path, "trades/btc_jpy/2026-10-12T100000.000Z.jsonl.gz")).map(
        (r) => r.msg,
      ),
    ).toEqual([trade("btc_jpy", "13000001.0", "0.555")]);
  });

  test("stops instead of appending when the file it would create already exists", async () => {
    setup();
    mkdirSync(join(dir.path, "trades/btc_jpy"), { recursive: true });
    const taken = join(dir.path, "trades/btc_jpy/2026-10-12T100000.000Z.jsonl.gz");
    writeFileSync(taken, "someone else's");
    start({
      subscriptions: [{ channel: "trades", pair: "btc_jpy" }],
      now: () => new Date("2026-10-12T10:00:00.000Z"),
    });
    await waitFor(() => server.opened === 1);
    server.send(JSON.stringify(TRADES_BTC));
    await waitFor(() => fatals.length === 1, 1000);
    expect(readFileSync(taken, "utf8")).toBe("someone else's");
  });

  test("reports a write failure through onFatal and stops recording", async () => {
    setup();
    // trades のディレクトリを作れないように、同じ名前の通常ファイルを置く
    writeFileSync(join(dir.path, "trades"), "");
    // スナップショットが同じ flush に混ざると捨てる行数が変わるので、取得を失敗させる
    server.rest = () => new Response("down", { status: 503 });
    const recvAt = new Date("2026-10-12T10:00:00.000Z");
    start({
      subscriptions: [
        { channel: "orderbook", pair: "btc_jpy" },
        { channel: "trades", pair: "btc_jpy" },
      ],
      now: () => recvAt,
      flushIntervalMs: 300,
    });
    await waitFor(() => server.opened === 1);
    // 同じ flush で、書ける板の後に書けない約定が来る。捨てるのは約定の 2 行だけ
    server.send(JSON.stringify(OB_BTC));
    server.send(JSON.stringify(TRADES_BTC));
    server.send(JSON.stringify(TRADES_BTC));
    await waitFor(() => fatals.length === 1, 1000);
    expect(fatals[0]).toBeInstanceOf(Error);
    expect(logs.some((l) => /dropped 2 line/.test(l))).toBe(true);
    expect(readDay(dir.path, "orderbook/btc_jpy", "2026-10-12").length).toBe(1);
    await waitFor(() => server.closed === 1, 1000);
    await Bun.sleep(300);
    expect(server.opened).toBe(1);
  });
});

describe("accepting messages", () => {
  test("skips non-JSON and messages of another shape, logs them, and keeps recording", async () => {
    setup();
    const recvAt = new Date("2026-10-12T10:00:00.000Z");
    start({ subscriptions: [{ channel: "trades", pair: "btc_jpy" }], now: () => recvAt });
    await waitFor(() => server.opened === 1);
    server.send("not json at all");
    server.send(JSON.stringify({ error: "SOMETHING-WRONG" }));
    server.send(JSON.stringify(TRADES_BTC));
    await Bun.sleep(200);
    await recorder!.stop();

    expect(readDay(dir.path, "trades/btc_jpy", "2026-10-12")).toEqual([
      { recv_at: "2026-10-12T10:00:00.000Z", msg: TRADES_BTC },
    ]);
    expect(logs.some((l) => l.includes("SOMETHING-WRONG"))).toBe(true);
    expect(logs.some((l) => l.includes("not json at all"))).toBe(true);
    expect(server.opened).toBe(1);
  });

  test("logs only the head of a skipped message", async () => {
    setup();
    start({ subscriptions: [{ channel: "trades", pair: "btc_jpy" }] });
    await waitFor(() => server.opened === 1);
    server.send(`HEAD-${"x".repeat(5000)}-TAIL`);
    await waitFor(() => logs.some((l) => l.includes("HEAD-")), 1000);
    const line = logs.find((l) => l.includes("HEAD-"))!;
    expect(line.includes("-TAIL")).toBe(false);
    expect(line.length).toBeLessThan(400);

    server.send("broken\r\nstats orderbook/btc_jpy=999");
    await waitFor(() => logs.some((l) => l.includes("broken")), 1000);
    // 受信した改行でログが割れて、別の行 (stats) に見えないようにする
    expect(logs.find((l) => l.includes("broken"))).not.toMatch(/[\r\n]/);
  });

  test("never writes a message of an unexpected shape or pair, inside or outside the output directory", async () => {
    setup();
    // 抜け出しても一時ディレクトリの中に収まる深さに置き、そこだけを数える
    const out = join(dir.path, "a/b/c/out");
    const recvAt = new Date("2026-10-12T10:00:00.000Z");
    start({
      subscriptions: [
        { channel: "trades", pair: "btc_jpy" },
        { channel: "orderbook", pair: "btc_jpy" },
      ],
      outDir: out,
      now: () => recvAt,
    });
    await waitFor(() => server.opened === 1);
    const ob = OB_BTC[1];
    for (const bad of [
      ["../../escaped", ob],
      ["BTC_JPY", ob],
      [1, ob],
      ["btc_jpy", null],
      ["btc_jpy", []],
      ["btc_jpy", "str"],
      ["btc_jpy", ob, "extra"],
      trade("../../escaped", "1.0"),
      trade("BTC_JPY", "1.0"),
      [...TRADES_BTC, ...trade("eth_jpy", "1.0")],
      [...TRADES_BTC, "not an array"],
      [["1792298110", "1", 5, "1.0", "1.0", "buy", "1", "2", null]],
      [["1792298110", "1"]],
    ]) {
      server.send(JSON.stringify(bad));
    }
    for (const bad of ["null", "[]", "{}", '"str"', "42"]) server.send(bad);
    server.send(JSON.stringify(TRADES_BTC));
    await Bun.sleep(200);
    await recorder!.stop();

    expect(listFiles(dir.path).filter((f) => !f.includes("orderbook_snapshot/"))).toEqual([
      "a/b/c/out/trades/btc_jpy/2026-10-12T100000.000Z.jsonl.gz",
    ]);
    expect(readDay(out, "trades/btc_jpy", "2026-10-12").length).toBe(1);
  });
});

describe("snapshots", () => {
  test("fetches the REST order book right after subscribing and writes the body verbatim", async () => {
    setup();
    const recvAt = new Date("2026-10-12T10:00:00.000Z");
    const body = '{"asks":[["12927084.0","0.01"]],\n"bids":[["12926000.0","0.5"]]}';
    server.rest = () => new Response(body, { headers: { "content-type": "application/json" } });
    start({
      subscriptions: [
        { channel: "orderbook", pair: "btc_jpy" },
        { channel: "trades", pair: "eth_jpy" },
      ],
      now: () => recvAt,
    });
    await waitFor(() => server.restRequests.length === 1, 1000);
    await Bun.sleep(200);
    await recorder!.stop();

    // 板を購読していない pair は取らない
    expect(server.restRequests).toEqual(["/api/order_books?pair=btc_jpy"]);
    expect(readDayRaw(dir.path, "orderbook_snapshot/btc_jpy", "2026-10-12")).toEqual([
      `{"recv_at":"2026-10-12T10:00:00.000Z","msg":${body.replace("\n", " ")}}`,
    ]);
  });

  test("does not fetch anything when no order book is subscribed", async () => {
    setup();
    start({ subscriptions: [{ channel: "trades", pair: "btc_jpy" }], snapshotIntervalMs: 50 });
    await waitFor(() => server.received.length === 1);
    await Bun.sleep(300);
    expect(server.restRequests).toEqual([]);
  });

  test("fetches again every snapshot interval", async () => {
    setup();
    start({ subscriptions: [{ channel: "orderbook", pair: "btc_jpy" }], snapshotIntervalMs: 100 });
    // stop より後に届いた応答は書かないので、待つ要求は期待する件数より 1 件多くする
    await waitFor(() => server.restRequests.length >= 5, 2000);
    await recorder!.stop();
    await Bun.sleep(100);
    expect(readDay(dir.path, "orderbook_snapshot/btc_jpy", todayUtc()).length).toBeGreaterThan(3);
  });

  test("fetches again right after a reconnect", async () => {
    setup();
    start({ subscriptions: [{ channel: "orderbook", pair: "btc_jpy" }] });
    await waitFor(() => server.restRequests.length === 1, 1000);
    server.closeConn(0);
    await waitFor(() => server.opened === 2 && sentOn(1).length === 1, 2000);
    await waitFor(() => server.restRequests.length === 2, 1000);
  });

  test("discards a snapshot whose fetch started before a reconnect and fetches again", async () => {
    setup();
    const stale = { asks: [["1.0", "9"]], bids: [["0.5", "9"]] };
    let release: (() => void) | undefined;
    server.rest = () =>
      new Promise<Response>((resolve) => {
        release = () => resolve(Response.json(stale));
      });
    const recvAt = new Date("2026-10-12T10:00:00.000Z");
    start({ subscriptions: [{ channel: "orderbook", pair: "btc_jpy" }], now: () => recvAt });
    await waitFor(() => server.restRequests.length === 1, 1000);
    server.closeConn(0);
    await waitFor(() => server.opened === 2 && sentOn(1).length === 1, 2000);
    await Bun.sleep(100);
    // 切断前に始めた取得が終わるまでは重ねない
    expect(server.restRequests.length).toBe(1);
    server.rest = () => Response.json(SNAPSHOT);
    release!();
    await waitFor(() => server.restRequests.length === 2, 1000);
    await Bun.sleep(200);
    // 同じ接続の間に終わった取得は取り直さない (interval は既定の 60 秒)
    expect(server.restRequests.length).toBe(2);
    await recorder!.stop();
    expect(readDay(dir.path, "orderbook_snapshot/btc_jpy", "2026-10-12").map((r) => r.msg)).toEqual(
      [SNAPSHOT],
    );
  });

  test("does not fetch again after stop, even for a fetch that started before a reconnect", async () => {
    setup();
    let release: (() => void) | undefined;
    server.rest = () =>
      new Promise<Response>((resolve) => {
        release = () => resolve(Response.json(SNAPSHOT));
      });
    start({ subscriptions: [{ channel: "orderbook", pair: "btc_jpy" }] });
    await waitFor(() => server.restRequests.length === 1, 1000);
    server.closeConn(0);
    await waitFor(() => server.opened === 2, 2000);
    await recorder!.stop();
    release!();
    await Bun.sleep(200);
    expect(server.restRequests.length).toBe(1);
  });

  test("fetches again at once when a fetch started before a reconnect fails", async () => {
    setup();
    server.rest = () => new Promise<Response>(() => {});
    start({ subscriptions: [{ channel: "orderbook", pair: "btc_jpy" }], snapshotTimeoutMs: 300 });
    await waitFor(() => server.restRequests.length === 1, 1000);
    server.closeConn(0);
    await waitFor(() => server.opened === 2, 2000);
    // 打ち切りの直後に取り直す。interval (既定 60 秒) を待たない
    await waitFor(() => server.restRequests.length === 2, 1000);
  });

  test("does not let a snapshot reset the reconnect wait", async () => {
    setup();
    start({ subscriptions: [{ channel: "orderbook", pair: "btc_jpy" }] });
    // 開くたびに取るスナップショットが届いてから切る。待ちは 50 → 100 → 200 と伸び続ける
    const opens: number[] = [];
    let seen = 0;
    while (opens.length < 4) {
      await waitFor(
        () => server.opened > seen && server.restRequests.length >= server.opened,
        3000,
      );
      seen = server.opened;
      opens.push(Date.now());
      await Bun.sleep(20);
      server.closeConn(seen - 1);
    }
    const gaps = opens.slice(1).map((t, i) => t - opens[i]!);
    [50, 100, 200].forEach((ms, i) => expect(gaps[i]!).toBeGreaterThanOrEqual(ms + 20 - 10));
  });

  test("discards a body that lacks either asks or bids", async () => {
    setup();
    let n = 0;
    server.rest = () => {
      n++;
      if (n === 1) return Response.json({ asks: [] });
      if (n === 2) return Response.json({ bids: [] });
      return Response.json(SNAPSHOT);
    };
    const recvAt = new Date("2026-10-12T10:00:00.000Z");
    start({
      subscriptions: [{ channel: "orderbook", pair: "btc_jpy" }],
      now: () => recvAt,
      snapshotIntervalMs: 100,
    });
    await waitFor(() => server.restRequests.length >= 4, 2000);
    await Bun.sleep(100);
    await recorder!.stop();
    const snaps = readDay(dir.path, "orderbook_snapshot/btc_jpy", "2026-10-12").map((r) => r.msg);
    expect(snaps.length).toBeGreaterThan(0);
    for (const s of snaps) expect(s).toEqual(SNAPSHOT);
  });

  test("discards a non-200 response even when its body looks like a book", async () => {
    setup();
    server.rest = () => Response.json(SNAPSHOT, { status: 500 });
    start({ subscriptions: [{ channel: "orderbook", pair: "btc_jpy" }] });
    await waitFor(() => logs.some((l) => l.includes("status 500")), 1000);
    await recorder!.stop();
    expect(listFiles(dir.path)).toEqual([]);
  });

  test("logs a failed fetch, writes nothing for it, and keeps recording", async () => {
    setup();
    let n = 0;
    server.rest = () => {
      n++;
      if (n === 1) return new Response("oops", { status: 500 });
      if (n === 2) return new Response('{"error":"bad pair"}');
      if (n === 3) return new Response("not json");
      return Response.json(SNAPSHOT);
    };
    const recvAt = new Date("2026-10-12T10:00:00.000Z");
    start({
      subscriptions: [{ channel: "orderbook", pair: "btc_jpy" }],
      now: () => recvAt,
      snapshotIntervalMs: 100,
    });
    await waitFor(() => server.restRequests.length >= 4, 2000);
    server.send(JSON.stringify(OB_BTC));
    await Bun.sleep(150);
    await recorder!.stop();

    expect(fatals).toEqual([]);
    expect(server.opened).toBe(1);
    expect(logs.some((l) => l.includes("500"))).toBe(true);
    expect(logs.some((l) => l.includes("bad pair"))).toBe(true);
    const snaps = readDay(dir.path, "orderbook_snapshot/btc_jpy", "2026-10-12").map((r) => r.msg);
    expect(snaps.length).toBeGreaterThan(0);
    for (const s of snaps) expect(s).toEqual(SNAPSHOT);
    expect(readDay(dir.path, "orderbook/btc_jpy", "2026-10-12").length).toBe(1);
  });

  test("gives up on a fetch after the snapshot timeout and does not overlap fetches", async () => {
    setup();
    // 応答しない REST。取得は打ち切られるまで次を始めない
    server.rest = () => new Promise<Response>(() => {});
    start({
      subscriptions: [{ channel: "orderbook", pair: "btc_jpy" }],
      snapshotIntervalMs: 50,
      snapshotTimeoutMs: 400,
    });
    await waitFor(() => server.restRequests.length === 1, 1000);
    const firstAt = Date.now();
    await waitFor(() => server.restRequests.length === 2, 2000);
    expect(Date.now() - firstAt).toBeGreaterThanOrEqual(350);
    expect(logs.some((l) => /snapshot/.test(l))).toBe(true);
    expect(fatals).toEqual([]);
  });

  test("does not let snapshots postpone the idle timeout", async () => {
    setup();
    start({
      subscriptions: [{ channel: "orderbook", pair: "btc_jpy" }],
      idleTimeoutMs: 300,
      snapshotIntervalMs: 50,
    });
    await waitFor(() => server.opened === 1);
    const firstAt = Date.now();
    await waitFor(() => server.opened >= 2, 1500);
    expect(Date.now() - firstAt).toBeGreaterThanOrEqual(250);
  });

  test("does not write a snapshot that arrives after stop", async () => {
    setup();
    let release: (() => void) | undefined;
    server.rest = () =>
      new Promise<Response>((resolve) => {
        release = () => resolve(Response.json(SNAPSHOT));
      });
    start({ subscriptions: [{ channel: "orderbook", pair: "btc_jpy" }] });
    await waitFor(() => release !== undefined, 1000);
    await recorder!.stop();
    release!();
    await Bun.sleep(200);
    expect(listFiles(dir.path)).toEqual([]);
  });

  test("stop cancels the snapshot interval", async () => {
    setup();
    start({ subscriptions: [{ channel: "orderbook", pair: "btc_jpy" }], snapshotIntervalMs: 50 });
    await waitFor(() => server.restRequests.length >= 2, 1000);
    await recorder!.stop();
    await Bun.sleep(50);
    const n = server.restRequests.length;
    await Bun.sleep(300);
    expect(server.restRequests.length).toBe(n);
  });
});

function todayUtc(): string {
  return new Date().toISOString().slice(0, 10);
}

describe("recovering", () => {
  test("reconnects once after the server closes the connection and subscribes again", async () => {
    setup();
    const recvAt = new Date("2026-10-12T10:00:00.000Z");
    const subs: Subscription[] = [
      { channel: "orderbook", pair: "btc_jpy" },
      { channel: "trades", pair: "btc_jpy" },
    ];
    start({ subscriptions: subs, now: () => recvAt });
    await waitFor(() => sentOn(0).length === 2);
    server.closeConn(0);

    await waitFor(() => server.opened === 2 && sentOn(1).length === 2);
    expect(sentOn(1).map((r) => JSON.parse(r.text))).toEqual(subscribeCommands(subs));
    await Bun.sleep(300);
    expect(server.opened).toBe(2);

    server.send(JSON.stringify(TRADES_BTC), 1);
    await Bun.sleep(150);
    await recorder!.stop();
    expect(readDay(dir.path, "trades/btc_jpy", "2026-10-12").length).toBe(1);
  });

  test("doubles the reconnect wait up to the maximum and resets it after a message", async () => {
    setup();
    start({ subscriptions: [{ channel: "trades", pair: "btc_jpy" }] });

    // 開いた直後にサーバーが切る状態を 6 回続ける
    const opens: number[] = [];
    let seen = 0;
    while (opens.length < 6) {
      await waitFor(() => server.opened > seen, 3000);
      seen = server.opened;
      opens.push(Date.now());
      server.closeConn(seen - 1);
    }
    const gaps = opens.slice(1).map((t, i) => t - opens[i]!);
    // 50 → 100 → 200 → 400 → 400 (上限)。下限はタイマーの誤差 10ms を引いた待ち、上限は待ち + 余裕
    const expected = [50, 100, 200, 400, 400];
    expected.forEach((ms, i) => {
      expect(gaps[i]!).toBeGreaterThanOrEqual(ms - 10);
      expect(gaps[i]!).toBeLessThan(ms + Math.min(ms, 150));
    });

    // 次の接続ではメッセージを 1 件届けてから切る。待ちは初期値に戻る
    await waitFor(() => server.opened > seen, 3000);
    seen = server.opened;
    server.send(JSON.stringify(TRADES_BTC), seen - 1);
    await Bun.sleep(50);
    const closedAt = Date.now();
    server.closeConn(seen - 1);
    await waitFor(() => server.opened > seen, 3000);
    expect(Date.now() - closedAt).toBeGreaterThanOrEqual(40);
    // 戻していなければ上限の 400ms を待つ
    expect(Date.now() - closedAt).toBeLessThan(300);
  });

  test("reconnects when nothing arrives within the idle timeout, and closes the idle connection", async () => {
    setup();
    start({ subscriptions: [{ channel: "trades", pair: "btc_jpy" }], idleTimeoutMs: 300 });
    await waitFor(() => server.opened === 1);
    const firstAt = Date.now();
    await waitFor(() => server.opened >= 2, 2000);
    expect(Date.now() - firstAt).toBeGreaterThanOrEqual(250);
    await waitFor(() => server.closed >= 1, 1000);
  });

  test("reconnects only once after an idle timeout", async () => {
    setup();
    start({ subscriptions: [{ channel: "trades", pair: "btc_jpy" }], idleTimeoutMs: 300 });
    await waitFor(() => server.opened === 2, 2000);
    const timer = setInterval(() => {
      try {
        server.send(JSON.stringify(TRADES_BTC), 1);
      } catch {}
    }, 100);
    await Bun.sleep(800);
    clearInterval(timer);
    expect(server.opened).toBe(2);
  });

  test("does not let invalid messages postpone the idle timeout", async () => {
    setup();
    start({ subscriptions: [{ channel: "trades", pair: "btc_jpy" }], idleTimeoutMs: 300 });
    await waitFor(() => server.opened === 1);
    const timer = setInterval(() => {
      try {
        server.send(JSON.stringify({ error: "SOMETHING-WRONG" }), 0);
      } catch {}
    }, 100);
    const firstAt = Date.now();
    try {
      await waitFor(() => server.opened >= 2, 1500);
    } finally {
      clearInterval(timer);
    }
    expect(Date.now() - firstAt).toBeGreaterThanOrEqual(250);
  });

  test("valid messages keep the connection open past the idle timeout", async () => {
    setup();
    start({ subscriptions: [{ channel: "trades", pair: "btc_jpy" }], idleTimeoutMs: 300 });
    await waitFor(() => server.opened === 1);
    const timer = setInterval(() => server.send(JSON.stringify(TRADES_BTC), 0), 100);
    await Bun.sleep(900);
    clearInterval(timer);
    expect(server.opened).toBe(1);
  });

  test("cuts an idle connection whose peer no longer answers, without waiting for the close handshake", async () => {
    setup();
    const silent = startSilentTcpServer({ handshake: true });
    try {
      start({
        subscriptions: [{ channel: "trades", pair: "btc_jpy" }],
        url: silent.url,
        idleTimeoutMs: 300,
        backoffInitialMs: 5000,
      });
      // 購読のフレームが届いた = handshake を終えて開いた接続になっている
      await waitFor(() => silent.frames >= 1, 1000);
      await waitFor(() => silent.closes.length >= 1, 1500);
      const lived = silent.closes[0]! - silent.accepts[0]!;
      expect(lived).toBeGreaterThanOrEqual(250);
      expect(lived).toBeLessThan(800);
    } finally {
      await recorder?.stop();
      recorder = undefined;
      silent.stop();
    }
  });

  test("gives up on a connection that never completes the handshake", async () => {
    setup();
    const silent = startSilentTcpServer();
    try {
      start({
        subscriptions: [{ channel: "trades", pair: "btc_jpy" }],
        url: silent.url,
        idleTimeoutMs: 300,
      });
      await waitFor(() => silent.accepts.length >= 2, 2000);
      expect(silent.accepts[1]! - silent.accepts[0]!).toBeGreaterThanOrEqual(250);
      // 捨てた接続は閉じる (残すと止まった接続が溜まり続ける)
      await waitFor(() => silent.closes.length >= 1, 1000);
    } finally {
      await recorder?.stop();
      recorder = undefined;
      silent.stop();
    }
  });
});

describe("stats", () => {
  test("logs cumulative per-stream counts, including snapshots and subscribed streams with none", async () => {
    setup();
    start({
      subscriptions: [
        { channel: "orderbook", pair: "btc_jpy" },
        { channel: "trades", pair: "btc_jpy" },
      ],
      statsIntervalMs: 100,
    });
    await waitFor(() => server.opened === 1);
    server.send(JSON.stringify(OB_BTC));
    server.send(JSON.stringify(OB_BTC));
    await waitFor(
      () =>
        logs.some(
          (l) =>
            /orderbook\/btc_jpy=2\b/.test(l) &&
            /orderbook_snapshot\/btc_jpy=1\b/.test(l) &&
            /trades\/btc_jpy=0\b/.test(l),
        ),
      1000,
    );
    server.send(JSON.stringify(OB_BTC));
    await waitFor(() => logs.some((l) => /orderbook\/btc_jpy=3\b/.test(l)), 1000);
  });

  test("lists a snapshot stream with zero before any snapshot succeeds", async () => {
    setup();
    server.rest = () => new Response("down", { status: 503 });
    start({ subscriptions: [{ channel: "orderbook", pair: "btc_jpy" }], statsIntervalMs: 100 });
    await waitFor(() => logs.some((l) => /orderbook_snapshot\/btc_jpy=0\b/.test(l)), 1000);
  });
});

describe("stop", () => {
  test("closes the connection and does not reconnect", async () => {
    setup();
    start({ subscriptions: [{ channel: "trades", pair: "btc_jpy" }] });
    await waitFor(() => server.opened === 1);
    await recorder!.stop();
    await waitFor(() => server.closed === 1, 1000);
    await Bun.sleep(300);
    expect(server.opened).toBe(1);
  });

  test("cancels a pending reconnect", async () => {
    setup();
    start({ subscriptions: [{ channel: "trades", pair: "btc_jpy" }], backoffInitialMs: 300 });
    await waitFor(() => server.opened === 1);
    server.closeConn(0);
    await waitFor(() => server.closed === 1, 1000);
    await recorder!.stop();
    await Bun.sleep(600);
    expect(server.opened).toBe(1);
  });
});
