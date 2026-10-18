#!/usr/bin/env bun
import { Command, InvalidArgumentError } from "commander";
import {
  CHANNELS,
  type Channel,
  DEFAULT_API_URL,
  DEFAULT_PUBLIC_WS_URL,
  PAIR_PATTERN,
  startRecorder,
} from "./record.ts";

function collect<T>(parse: (v: string) => T) {
  return (value: string, prev: T[] = []) => [...prev, parse(value)];
}

function parsePair(v: string): string {
  if (!PAIR_PATTERN.test(v)) throw new InvalidArgumentError("pair must match ^[a-z0-9_]+$");
  return v;
}

function parseChannel(v: string): Channel {
  const channel = CHANNELS.find((c) => c === v);
  if (!channel) throw new InvalidArgumentError(`channel must be one of ${CHANNELS.join(", ")}`);
  return channel;
}

const program = new Command().name("coincheck").description("CLI for the Coincheck API");

program
  .command("record")
  .description("record Public WebSocket orderbooks and trades as gzip JSONL")
  .option("--pair <pair>", "pair to record, repeatable (default btc_jpy)", collect(parsePair))
  .option(
    "--channel <channel>",
    "channel to record, repeatable (default orderbook and trades)",
    collect(parseChannel),
  )
  .option("--out <dir>", "output directory", "./data")
  .action((opts: { pair?: string[]; channel?: Channel[]; out: string }) => {
    const pairs = opts.pair ?? ["btc_jpy"];
    const channels = opts.channel ?? CHANNELS;
    const recorder = startRecorder({
      url: process.env.COINCHECK_PUBLIC_WS_URL || DEFAULT_PUBLIC_WS_URL,
      apiUrl: process.env.COINCHECK_API_URL || DEFAULT_API_URL,
      outDir: opts.out,
      subscriptions: channels.flatMap((channel) => pairs.map((pair) => ({ channel, pair }))),
      onFatal: () => void exitAfterStop(1),
    });

    async function exitAfterStop(code: number) {
      try {
        await recorder.stop();
      } catch (e) {
        console.error(e);
      }
      process.exit(code);
    }

    process.on("SIGINT", () => void exitAfterStop(0));
    process.on("SIGTERM", () => void exitAfterStop(0));
    const crash = (err: unknown) => {
      console.error(err);
      void exitAfterStop(1);
    };
    process.on("uncaughtException", crash);
    process.on("unhandledRejection", crash);
  });

await program.parseAsync();
