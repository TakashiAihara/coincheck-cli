import { afterEach, expect, test } from "bun:test";
import { existsSync, readdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import type { Subprocess } from "bun";
import { readRecords, startFakeServer, tempDir, waitFor, type FakeServer } from "./test-helpers.ts";

const CLI = join(import.meta.dir, "cli.ts");
const TRADES = [
  ["1792298110", "289000001", "btc_jpy", "12927674.0", "0.03", "buy", "1", "2", null],
];

let server: FakeServer | undefined;
let dir: { path: string; cleanup(): void } | undefined;

let procs: Subprocess[] = [];

afterEach(() => {
  for (const p of procs) p.kill("SIGKILL");
  procs = [];
  server?.stop();
  dir?.cleanup();
  server = undefined;
  dir = undefined;
});

function spawnCli(args: string[], fake?: FakeServer) {
  const env: Record<string, string> = fake
    ? { COINCHECK_PUBLIC_WS_URL: fake.url, COINCHECK_API_URL: fake.apiUrl }
    : {};
  const p = Bun.spawn(["bun", CLI, ...args], {
    env: { ...process.env, ...env },
    stdout: "pipe",
    stderr: "pipe",
  });
  procs.push(p);
  return p;
}

test("--help lists the record command", async () => {
  const p = spawnCli(["--help"]);
  expect(await p.exited).toBe(0);
  expect(await new Response(p.stdout).text()).toContain("record");
});

test("record rejects a channel other than orderbook and trades before connecting", async () => {
  server = startFakeServer();
  dir = tempDir();
  const out = join(dir.path, "out");
  const p = spawnCli(["record", "--channel", "orderbooks", "--out", out], server);
  expect(await p.exited).not.toBe(0);
  expect(await new Response(p.stderr).text()).toContain("orderbooks");
  expect(server.opened).toBe(0);
  expect(server.restRequests).toEqual([]);
  expect(existsSync(out)).toBe(false);
});

test("record rejects a pair that is not lower-case letters, digits and underscores", async () => {
  server = startFakeServer();
  for (const pair of ["../btc_jpy", "BTC_JPY"]) {
    const p = spawnCli(["record", "--pair", pair], server);
    expect(await p.exited).not.toBe(0);
    expect(await new Response(p.stderr).text()).toContain(pair);
  }
  expect(server.opened).toBe(0);
});

test("record subscribes orderbook and trades of btc_jpy by default and fetches its snapshot", async () => {
  server = startFakeServer();
  dir = tempDir();
  const p = spawnCli(["record", "--out", dir.path], server);
  await waitFor(() => server!.received.length >= 2 && server!.restRequests.length >= 1, 5000);
  p.kill("SIGINT");
  expect(await p.exited).toBe(0);
  expect(server.received.map((r) => JSON.parse(r.text))).toEqual([
    { type: "subscribe", channel: "btc_jpy-orderbook" },
    { type: "subscribe", channel: "btc_jpy-trades" },
  ]);
  expect(server.restRequests).toEqual(["/api/order_books?pair=btc_jpy"]);
  expect(readdirSync(join(dir.path, "orderbook_snapshot/btc_jpy")).length).toBe(1);
}, 10_000);

test("record writes what it has buffered and exits 0 on SIGTERM", async () => {
  server = startFakeServer();
  dir = tempDir();
  const p = spawnCli(["record", "--channel", "trades", "--out", dir.path], server);
  await waitFor(() => server!.received.length >= 1, 5000);
  server.send(JSON.stringify(TRADES));
  // 既定の flush 間隔 (5 秒) より前に止める
  await Bun.sleep(300);
  p.kill("SIGTERM");
  expect(await p.exited).toBe(0);

  const files = readdirSync(join(dir.path, "trades/btc_jpy"));
  expect(files.length).toBe(1);
  expect(readRecords(join(dir.path, "trades/btc_jpy", files[0]!)).map((r) => r.msg)).toEqual([
    TRADES,
  ]);
}, 10_000);

test("record exits 1 when it cannot write", async () => {
  server = startFakeServer();
  dir = tempDir();
  writeFileSync(join(dir.path, "trades"), "");
  const p = spawnCli(["record", "--channel", "trades", "--out", dir.path], server);
  await waitFor(() => server!.received.length >= 1, 5000);
  server.send(JSON.stringify(TRADES));
  expect(await p.exited).toBe(1);
  expect(await new Response(p.stderr).text()).toContain("write failed");
}, 15_000);
