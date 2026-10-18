# 記録の常駐

## 結論

- 常駐させたホストで `coincheck record` を systemd で回し続ける。落ちたことは外から気づける形にする (記録が書かれていないと通知が来る外形監視)
- 生存の判定は `orderbook` / `orderbook_snapshot` / `trades` のそれぞれで最新のファイルの更新時刻を見て、全部が新しいときだけ外形監視に生存を送る (どれか 1 つだけ止まっても気づけるように)。新しさの基準は板の差分が 2 分、スナップショットが 4 分、約定が 60 分
- `bun` は `.mise.toml` と同じ版を入れる (テストした版と動かす版を揃えるため)
- 記録は 1 pair で約 15 MB/日 (gzip、5 秒ごとのメンバー)。古いファイルの削除はこの CLI ではしない

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
# 記録が動いていて、板の差分が 2 分以内、スナップショットが 4 分以内、約定が 60 分以内に書かれていれば Kuma に生存を送る。
# 送らないこと自体が異常の合図 (Kuma の push 監視が 180 秒で DOWN にする)
set -u
systemctl is-active -q coincheck-record || exit 1
# 起動から 60 分は約定を見ない (閑散時は最初の約定まで数分かかり、ファイルがまだ無い)
started=$(date -d "$(systemctl show coincheck-record -p ActiveEnterTimestamp --value)" +%s)
streams="orderbook:2 orderbook_snapshot:4"
[ $(( $(date +%s) - started )) -lt 3600 ] || streams="$streams trades:60"
for st in $streams; do
  [ -n "$(find <data>/${st%%:*}/btc_jpy -name '*.jsonl.gz' -mmin -${st##*:} -print -quit 2>/dev/null)" ] || { echo "no fresh ${st%%:*} file"; exit 1; }
done
# token を argv に載せないため、curl の設定として stdin から渡す
printf 'url = "<Kuma の URL>/api/push/%s?status=up&msg=OK"\n' "$(cat /etc/coincheck/kuma-push-token)" | curl -fsS -m 15 -o /dev/null -K -
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
# 既定の誤差 (1 分) だと間隔が 2 分近くまで延び、Kuma の 180 秒に近づく
AccuracySec=1s
[Install]
WantedBy=timers.target
```

- 板の差分は約 2.8 件/秒で、flush は 5 秒ごとなので、2 分書かれなければ止まっている
- スナップショットは 60 秒ごとに 1 件。k 回続けて失敗すると最後の書き込みから最大 (k+1)×60+5 秒空くので、4 分の基準では 3 回続けて失敗したら生存を送らなくなる (1 回の失敗では誤報しない)
- 約定は閑散時に 2 分以上空く (2026-10-18 14 時 JST の観測で 90 秒に 7 件の時間帯と、120 秒に 1 件の時間帯があった) ので 60 分で見る。約定の stream だけが止まったときに気づくのは最大で 60 分遅れる
