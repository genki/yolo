# yolo resume CWD修復の知見

## 結論

`yolo resume`で`--cd`を指定しても、Codex app-serverのlive thread
settingsに古い`cwd`や`runtimeWorkspaceRoots`が残っていると、ターン開始時の
実行コンテキストが古いディレクトリへ戻る。

このためresume時は次の3層をすべて揃える必要がある。

- Codex CLI起動引数の`--cd`
- rollout/state DB上の`cwd`、`workspace_roots`、権限設定
- app-server live thread settingsの`cwd`、`runtimeWorkspaceRoots`

## 観測した症状

`/home/vagrant/websh`で`yolo resume`しているにもかかわらず、モデルに渡る
`environment_context`が`/home/vagrant/jotter`になった。

プロセス側は以下の通り正しかった。

- yolo/codex子プロセスの`/proc/<pid>/cwd`: `/home/vagrant/websh`
- Codex CLI引数: `--cd /home/vagrant/websh`
- Codex state DBの`threads.cwd`: `/home/vagrant/websh`
- rollout内の`turn_context.cwd`: `/home/vagrant/websh`

しかしapp-serverの`thread/resume`応答では、同じレスポンス内で値が分裂して
いた。

```text
thread.cwd=/home/vagrant/websh
cwd=/home/vagrant/jotter
runtimeWorkspaceRoots=[/home/vagrant/jotter]
```

ターン開始側はトップレベルの`cwd`と`runtimeWorkspaceRoots`を使うため、
`thread.cwd`や`--cd`だけを直しても不十分だった。

## 対応方針

resume時にyoloが以下を実行する。

- `resume --last`は現在CWDに一致する停止済みセッションだけに解決する
- resume対象rollout/state DBのCWDと権限設定を修復する
- app-serverへ`thread/settings/update`を送り、live settingsの
  `cwd`と`runtimeWorkspaceRoots`も現在CWDへ揃える
- resume時は`include_environment_context=false`を指定し、古い
  workspace root由来の環境context注入を避ける
- Codexがturn開始時にrolloutへ追記したcontextもwatcherで補正する

## 確認方法

app-server live settingsはUnixソケットへJSON-RPCで確認できる。

```text
method: thread/resume
params:
  threadId: <thread-id>
  excludeTurns: true
```

確認時は`clientInfo`と`experimentalApi` capability付きで`initialize`する。
修復後は以下のように揃っていることを確認する。

```text
cwd=/home/vagrant/websh
runtimeWorkspaceRoots=[/home/vagrant/websh]
thread.cwd=/home/vagrant/websh
```

