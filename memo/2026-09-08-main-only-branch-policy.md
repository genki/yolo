# mainブランチ限定運用の知見

## 事象

前回の改良コミットを、誤って`agent/yolo-new-session-defaults`へpushした。
リポジトリの運用方針は`main`のみを使用するため、作業ブランチが残ると
実装の所在とデプロイ対象を誤認しやすい。

## 是正

- `main`が対象コミットの祖先であることを`git merge-base --is-ancestor`
  で確認した。
- force pushを使わず、`git push origin HEAD:main`でfast-forward反映した。
- ローカルの作業ブランチと、反映済みコミットを保持していたリモート作業
  ブランチを削除した。
- `git branch -vv`、`git ls-remote --heads origin`、`git status --short --branch`
  で`main`のみ、`HEAD == origin/main`、clean状態を確認した。

## 今後の手順

1. 作業開始時に`git branch --show-current`と`git status --short --branch`を確認する。
2. コミット前に対象が`main`であることを確認し、無関係な差分を分離する。
3. push先は`origin/main`に固定し、`git push origin HEAD:main`を使う。
4. push後に`git ls-remote --heads origin main`でリモート先頭を照合する。
5. 作業ブランチが作られた場合は、mainへの反映確認後に残存させない。

## 教訓

コミットの内容が正しくても、ブランチ運用を外すとリリース経路が不明確に
なる。実装レビューと同じ粒度で、コミット前後のブランチ・remote・clean
状態を記録する。
