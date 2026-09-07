# yolo再点検：問題点と改善方針

## 対象と結論

2026-09-07。HEADは4a3557d8e84242d5082f055a30746ecfe7b7f306。
前回の未コミット修正を含むsrc/main.rsを確認した。
対象ファイルSHA256:
`0d28817e0fae6534391087ad821c12f6877e1a30dd044ef82048b51494c9b583`

セッション同定、監視購読、WebSocket受信、設定適用経路を重点点検。
全機能・全ホストの網羅監査ではない。今回は実装変更・再起動・配布をしていない。
既存の181テスト成功は前回の結果であり、以下の問題を否定するものではない。

|優先度|問題|確度|
|---|---|---|
|P1|途中timeout後の受信再開でフレーム境界を失う|抽出した実関数で再現|
|P1|thread購読のRPC失敗を購読済みとして固定|呼出し・応答経路を静的確認|
|P1|単一cwd候補を時刻・接続の確認なしに確定|条件分岐を静的確認|
|P2|分割JSONの最初の断片だけを返す|抽出した実関数で再現|
|P2|遠い候補の同点が明確な最良候補も保留する|前回修正の分岐を静的確認|
|P2|設定適用の同期RPCがイベント受信を止める|同期呼出し経路を確認、遅延量は未計測|

P1は先行修正推奨、P2は次の改善単位。過去の実障害との因果関係までは
立証していない。特に既報の連続出力がこれらだけで説明できるとは断定しない。

## 1. 途中timeout後に同じストリームを再利用する

根拠: src/main.rs:18858, 19289, 23049, 23108。
read_exact_retryは途中まで消費したバイトをローカルbufferに保持するが、
deadline超過でErrを返すとbufferと読取位置情報は破棄される。
status listenerは文字列にtimed outが含まれると処理を継続し、次の呼出しで
残りのpayloadやheaderを新しいframe headerとして読む。

再現: headerの先頭0x81を1byte受信→TimedOut→残り[2, '{', '}']を受信。
最初はtimeout、次は本来のJSONを復元できずfailed to fill whole bufferになる。
抽出した実関数を使う独立テストでこの不正動作を確認した。

影響: 遅い受信・一時停止で監視切断、状態更新の欠落、再接続が発生し得る。

改善: connectionに紐付く増分decoderにbufferと進行状態を保持する。
暫定策なら「一切受信していない待機timeout」と「部分受信後timeout」を
型で区別し、後者は接続を破棄・再接続する。文字列判定を廃止する。

受入条件: header/拡張長/mask/payloadの各途中で停止させ、再開後に
元messageが1度だけ届くか、安全な再接続になり、不正な解析を継続しない。

## 2. 購読失敗後に再試行できない

根拠: src/main.rs:19191, 18858, 18876, 21958, 20260。
subscribe_running_client_threadsはsend_requestの戻りIDを保存せず、
送信直後にsubscribed_thread_idsへ追加する。
error応答はlistenerのobserve_app_server_messageへ渡るが、
成功thread responseとして解析できず、購読集合からも取り除かれない。
同じ接続が続く限りそのthreadへの再購読が抑止される。

影響: 一時的なthread not foundやRPC失敗後、個別threadの監視が回復しない。
別経路の通知で補完される可能性はあるが、この購読処理自身の復旧はない。

改善: request ID→thread IDのpending表を持ち、ACK成功後だけsubscribedへ
移す。失敗・期限切れは分類し、一時障害だけ上限付きbackoffで再試行する。
thread破棄・世代更新時は購読情報を整理する。

受入条件: 初回error、同じ接続で次回successとなる偽サーバを用い、
再購読と状態通知の復帰を確認。恒久的なnot-foundでは再試行を連発しない。

## 3. 単一候補へのthread同定が依然として推定を確定扱いする

根拠: src/main.rs:18916, 19000, 20154。
candidates.len()==1ではcreatedAtも起動時刻も照合せず、cwdだけで割当てる。
client_uses_managed_proxyもremoteが非空ならtrueを返す。
結果にはapp_server_startedというauthoritativeなsourceが設定される。

例: /projectに未解決clientが1件残る状態で、同cwdの別接続から生成・通知
されたroot threadを受けると、そのclientのthreadとして確定される。
この分岐の存在は確認済み。ただし実環境でその通知が届く頻度は未検証。

改善: proxyが観測したthread/start・resumeのrequest/response対応を優先する。
cwd/時刻しか根拠がない候補には専用のtentative sourceを設け、
設定変更・再開対象・idle判定の確定情報として扱わない。
単一候補にも時刻制限を適用し、remoteは管理対象の実接続と照合する。

受入条件: 同cwdの別接続、古いstarted通知、起動時刻欠落、非管理remoteを
入力し、誤確定しないこと。proxyの確定responseでは正常にbindingを完了する。

## 4. 分割messageを組み立てず途中で返す

根拠: src/main.rs:23049。
FIN bitを参照せず、opcode=1でpayloadを直ちに返す。
continuation frame(opcode=0)はdefault分岐で捨てる。

再現: [0x01,1,'{', 0x80,1,'}']を入力すると、返却値は"{"となり、
後半frameが未読のまま残った。呼出し元のJSON parseは完結した"{}"を得られない。
抽出した実関数で再現済み。現行app-serverが実際に分割送信する頻度は未確認。

改善: FIN/continuationを扱うmessage decoderを共通化する。
ping/pongの割込みを許可し、frame上限に加えて組立てmessage全体にも上限を置く。
1の増分decoderと同じ実装単位で扱うことを推奨する。

受入条件: textを複数frameに分割し、中間ping、途中close、過大messageも検証。

## 5. 曖昧性保留が最良候補以外の同点まで拾う

根拠: src/main.rs:19019、特に19074以降。
前回追加したdistance countは全候補を対象にし、どこかの距離で同点になると
client全体をambiguousにする。

例: clientの起動秒=T、候補threadの生成秒がT、T-5、T+5の場合、
最良候補は距離0で唯一なのに、残る距離5の同点で全候補を保留する。
誤紐付け防止には保守的だが、不要にpendingが長引く仕様になっている。

改善: 確定情報によるbindingを第一とし、推定を残す場合は
最良候補と競合する割当てだけで曖昧性を判定する。
最低でも上のケースと「本当の最良同点」を分け、保留理由を表示・記録する。

受入条件: 最良候補は唯一・遠い候補だけ同点のケースを保留しないこと。
最良候補が同点の場合や複数clientが同じthreadを争う場合は誤確定しないこと。

## 6. イベント受信ループから同期の設定更新を実行する

根拠: src/main.rs:18876, 19128, 20597, 20693。
各message処理中に全clientのpending設定を調べ、configure_clientsを同期実行する。
設定RPCは複数回retryでき、内部でsleepも行う。その間、同じlistenerは
後続のthread status/eventを読めない。失敗後もpending fileが残るため、
次のmessageで再び同じ設定適用を試みる。

影響: 設定更新が詰まるほどidle/active通知処理も遅れ、表示や復旧判断の
鮮度が落ちる可能性。実測負荷・遅延は今回測定していないため性能上の懸念とする。

改善: listenerはenqueueまでとし、client単位で重複をまとめる有界workerへ
設定適用を委譲する。設定世代、接続世代、ACK結果を照合して永続化する。
新しい設定が来た場合の古いACK処理・pending削除も世代で保護する。

受入条件: 設定RPCを意図的に遅延・失敗させても別threadの状態通知が
処理されること。queueは有界で、同一clientへ無制限に仕事が蓄積しないこと。

## 推奨順序と検証記録

1. 購読ACKの管理と受信decoderを修正し、失敗注入テストを追加。
2. proxy由来の確定bindingと推定bindingを分離。
3. 曖昧候補判定を調整し、pendingの理由を可視化。
4. 設定適用を受信ループから分離し、遅延・世代競合を検証。

独立テストはsrc/main.rs:23049-23151をそのまま抽出して実行。
2 passedは「問題が起きることを確認するassert」が成功した意味であり、
実装が正常という意味ではない。再現コードは同ディレクトリの
2026-09-07-transport-review-reproducer.mdに保存。
ソース修正・既存テストの変更・実サービスへの障害注入は行っていない。

## 実装結果

上記6項目を実装し、2026-09-07に再検証した。対象ソースの現在SHA256は
`8b7086d66e89e60fc72dbc2379c97145ee61174bb1fb4142914baaa9a1fe97c7`。

- WebSocket受信をframe単位のdecoderに置換した。FIN/continuation、途中の
  ping/pong、control frame検証、frame/message上限を扱う。
  frame途中のtimeoutはidle扱いにせず接続を破棄して再接続し、未受信の
  idle timeoutだけをlistenerが継続扱いする。
- thread購読はrequest IDとthread IDのpending表で相関し、成功ACK後だけ
  subscribedへ移す。期限切れ・一時エラーは最大5回のbackoff再試行、
  thread not found等の恒久エラーはblockedとして連続再試行を止める。
- proxyの実接続とclient IDを照合し、thread/startedと起動時刻推定には
  createdAtおよび120秒以内の時刻差を要求する。推定結果は
  `app_server_inferred`/`tentative`とし、resume・upgrade・idle判定・設定
  更新・status反映・active-session永続化の権威情報には使わない。
  最近傍候補は各割当て後に再評価し、遠い候補の同点だけで一意な最良候補を
  保留しない一方、最良同点は保留する。
- pending設定適用はstatus listenerから有界channelのworkerへ移した。
  古いACKの後に新しいatomic置換が発生した場合は、file gate付きの比較削除
  で新しい設定を残し、workerを再スケジュールする。
- app-server状態lockがpoisonedな場合は「死亡確定」とせず、復旧の破壊的な
  active-work gateを通さない。短命RPCのDropではWebSocket closeを送る。

## 検証・反映結果

- `cargo test --locked --quiet`: 191 passed / 0 failed。
- `cargo fmt`、`git diff --check`: 成功。
- `cargo build --release --locked`: 成功。
- `tests/bg_restart.py target/release/yolo`: 成功。
- `cargo clippy --locked --all-targets`: 成功。ただし既存箇所にclippy警告が
  26件（test target重複を含め28件）残るため、別タスクとして整理可能。
- ローカル`yolo.service`はpreflightの`waiting=true / working=[]`を確認後に
  更新・systemd再起動した。新PIDは3271442、app-server PIDは3271490、
  `progress_ready=true`、`consecutive_failures=0`。
- release、配置先、稼働中`/proc/3271442/exe`のSHA256は一致し、値は
  `b00387c04e3d4ad9e9b5deb6b828ed842c01bb94ec635bbca40a5baaad35c17c`。
  旧binaryは`/tmp/yolo-review-implementation-4w7JzL/yolo`に退避した。
- versionは0.5.43のまま。別host・別slotへの配布は実施していない。

## 残る限界

現実装は部分frameを接続破棄して再接続する暫定策であり、接続をまたいで
decoder bufferを保持する完全なincremental decoderではない。client proxyの
raw relayはframeを透過転送するため、そこでのthread binding解析は完全な
message decoderとは別経路である。必要なら次段でdecoder共通化と、設定worker
の失敗時retry状態・可観測性を追加する。
