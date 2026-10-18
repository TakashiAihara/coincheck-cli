# coincheck-cli

CLI for the Coincheck API (market data recording, trading)

## Install

```bash
git clone https://github.com/TakashiAihara/coincheck-cli && cd coincheck-cli
bun install --production
bun run src/cli.ts --help
```

## Commands

- `coincheck record` — record the Public WebSocket order book (diffs) and trades, plus a REST order book snapshot every 60 seconds, as gzip JSONL. Spec: `docs/design/record.md`. Running it as a service: `docs/recording.md`
