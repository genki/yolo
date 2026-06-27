# yolo recovery loop risk review

Date: 2026-06-27 13:58 JST

## Question

復旧失敗ループなどにハマってシステムが不安定化するリスクを再点検した。

## Conclusion

外部の Codex app-server、OS、ネットワーク、実行中クライアントが絡むため、
「全リスクの完全排除」は断言できない。ただし今回観測された yolo 由来の
自己増殖的な不安定化経路は、短周期ループにならないよう有界化した。

## Remaining risks before this change

- app-server socket が消えて spawn が失敗し続けると、status listener が
  短周期で self-heal を繰り返す余地があった。
- app-server replacement を spawn した直後に listener がまた落ちると、
  再spawn判定が短周期化する余地があった。
- cwd 一致で thread_id を推定した非resumeクライアントが、既存resume
  セッションの thread_id を握り続ける余地があった。
- 推定 thread_id 同士の重複所有が残ると、working 表示や権限更新対象が
  混線する余地があった。

## Changes

- app-server status listener の self-heal に指数バックオフを追加した。
  - 初期値は既存 monitor interval の2秒。
  - 最大60秒で上限固定。
  - listener が60秒以上安定稼働した後の切断ではバックオフをリセットする。
- app-server が既に reachable、または既存PIDを採用できた場合は
  destructive restart を行わずバックオフも進めない。
- `clear_conflicting_inferred_thread_ids` を追加し、snapshot適用前に
  thread所有状態を修復する。
  - 明示 `resume THREAD_ID` の client は args 上の THREAD_ID を正とする。
  - 明示resumeと衝突する非明示推定 thread_id は解除する。
  - 非明示推定同士で同じ thread_id を重複所有した場合も解除する。
  - thread_id を解除・補正した場合は codex_status も一旦 clear する。
- cwd 一致で snapshot を割り当てる場合は、既に他clientがclaimしている
  thread_id を除外する。

## Validation

- `cargo fmt --check`
- `cargo test`
  - 20 tests passed
- `git diff --check`
- `cargo install --path . --force`
- yolo server のみ再起動し、app-server は継続採用。

## Live status after install

- yolo server PID: 2515026
- Codex app-server PID: 2462062
- `yolo refresh-permissions --all`
  - matched: 6
  - updated: 5
  - skipped: 1
- 非resume head client は `thread_id: null` のまま維持され、既存resume
  thread_id を誤claimしていない。

## Assessment

復旧失敗時に無制限・短周期で再spawnを繰り返す経路は抑制済み。
また、復旧後のthread ownership混線によって関係ないclientへ影響が広がる
経路も、明示resume優先と重複推定解除で抑制した。

