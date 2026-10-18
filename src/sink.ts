import { join } from "node:path";
import type { Stream } from "./record.ts";

export function recordPath(outDir: string, stream: Stream, pair: string, firstAt: Date): string {
  return join(outDir, stream, pair, `${firstAt.toISOString().replace(/:/g, "")}.jsonl.gz`);
}
