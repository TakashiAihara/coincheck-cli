# 記録の常駐

## 結論

- 常駐させたホストで `coincheck record` を systemd で回し続ける。落ちたことは外から気づける形にする (記録が書かれていないと通知が来る外形監視)
- 生存の送信は外形監視を 2 つに分ける。板の監視は `orderbook` (2 分) と `orderbook_snapshot` (4 分) の最新ファイルが両方新しいときだけ送り、窓は 180 秒。約定の監視は `trades` の最新ファイルが 2 分以内のときだけ送り、窓は 3 時間
- 約定を板と同じ監視に入れないのは、閑散時やメンテ中に長く途切れうるため (gmocoin の BTC は 2026-10-11〜18 に平常時で最長 19 分、土曜の定期メンテ中に 2 時間 11 分途切れた。gmocoin-cli 側の trade1 の journal の実測)。Coincheck のメンテの有無と長さは確かめていない
- `<data>` は絶対パスで書く。生存の送信は作業ディレクトリを持たないので、相対パスだと別の場所を探す
- 動かす `bun` の版が `.mise.toml` と違うなら、その版で `bun test` を 1 回通してから動かす (ホストの bun を他の記録と共有していて揃えられないことがあるため)
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
# 記録が動いていて、板の差分が 2 分以内、スナップショットが 4 分以内に書かれていれば板の監視に生存を送る。
# 送らないこと自体が異常の合図 (Kuma の push 監視が 180 秒で DOWN にする)
# 約定は閑散時やメンテ中に長く途切れうるので、この監視には入れず、
# 約定のファイルが 2 分以内に書かれているときだけ別の監視 (Kuma の窓 3 時間) に送る
set -u
systemctl is-active -q coincheck-record || { echo "coincheck-record is not active"; exit 1; }
for st in orderbook:2 orderbook_snapshot:4; do
  [ -n "$(find <data>/${st%%:*}/btc_jpy -name '*.jsonl.gz' -mmin -${st##*:} -print -quit 2>/dev/null)" ] || { echo "no fresh ${st%%:*} file"; exit 1; }
done
# token を argv に載せない (同じホストの ps から読めるため)
push() {
  local token
  token=$(cat "$1") || return 1
  printf 'url = "<Kuma の URL>/api/push/%s?status=up&msg=OK"\n' "$token" | curl -fsS -m 15 -o /dev/null -K -
}
push /etc/coincheck/kuma-push-token || exit 1
[ -n "$(find <data>/trades/btc_jpy -name '*.jsonl.gz' -mmin -2 -print -quit 2>/dev/null)" ] || exit 0
push /etc/coincheck/kuma-push-token-trades
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
- スナップショットは 60 秒ごとに 1 件。1〜2 回の失敗では最後の書き込みが 4 分より古くならないので誤報しない。続けて失敗すると 4 分を超え、そこから Kuma が 180 秒 push を受けないと DOWN になる。合わせて 5〜6 回 (6〜7 分) 続けて失敗したあたりで通知が来る
- 約定の stream だけが止まったとき (購読の拒否など) は、約定の監視が最大 3 時間後に DOWN にする。3 時間を超える約定の途切れ (長いメンテ) でも DOWN になる
