# 記録の常駐

## 結論

- 常駐させたホストで `coincheck record` を systemd で回し続ける。落ちたことは外から気づける形にする (記録が書かれていないと通知が来る外形監視)
- 生存の判定は `orderbook` / `orderbook_snapshot` / `trades` のそれぞれで最新のファイルの更新時刻を見て、全部が新しいときだけ外形監視に生存を送る (どれか 1 つだけ止まっても気づけるように)。新しさの基準は板の 2 つが 2 分、約定が 60 分
- 実際の配置 (trade1) は gmocoin-cli の記録と同じ CT・同じ実行ユーザー・同じ監視の形にしてある

## systemd

```ini
# /etc/systemd/system/coincheck-record.service
[Unit]
Description=coincheck record (btc_jpy orderbook and trades)
Wants=network-online.target
After=network-online.target

[Service]
User=<実行ユーザー>
WorkingDirectory=<作業ディレクトリ>
ExecStart=/usr/local/bin/bun run <clone>/src/cli.ts record --out <data>
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

- `Restart=always` は落ちたプロセスを上げ直すだけで、繋がったまま書かれない状態には効かない。それを見るのが下の生存の送信

## 生存の送信 (Uptime Kuma の push 監視)

```bash
#!/usr/bin/env bash
# /usr/local/bin/coincheck-record-heartbeat
# 記録が動いていて、板の 2 つの stream が 2 分以内、約定が 60 分以内に書かれていれば Kuma に生存を送る。
# 送らないこと自体が異常の合図 (Kuma の push 監視が 180 秒で DOWN にする)
set -u
systemctl is-active -q coincheck-record || exit 1
for st in orderbook:2 orderbook_snapshot:2 trades:60; do
  [ -n "$(find <data>/${st%%:*}/btc_jpy -name '*.jsonl.gz' -mmin -${st##*:} -print -quit)" ] || { echo "no fresh ${st%%:*} file"; exit 1; }
done
curl -fsS -m 15 -o /dev/null "http://uptime.local/api/push/$(cat /etc/coincheck/kuma-push-token)?status=up&msg=OK"
```

```ini
# /etc/systemd/system/coincheck-record-heartbeat.service
[Unit]
Description=Send a Kuma heartbeat while coincheck record is writing
[Service]
Type=oneshot
ExecStart=/usr/local/bin/coincheck-record-heartbeat
```

```ini
# /etc/systemd/system/coincheck-record-heartbeat.timer
[Unit]
Description=coincheck record heartbeat every minute
[Timer]
OnBootSec=2min
OnUnitActiveSec=60s
[Install]
WantedBy=timers.target
```

- 板の差分は約 2.8 件/秒で、flush は 5 秒ごとなので、2 分書かれなければ止まっている。スナップショットは 60 秒ごとに 1 件で、flush と合わせても 2 分以内に書かれる
- 約定は閑散時に 2 分以上空く (2026-10-18 14 時 JST の観測で 90 秒に 7 件の時間帯と、120 秒に 1 件の時間帯があった) ので 60 分で見る。約定の stream だけが止まったときに気づくのは最大で 60 分遅れる
