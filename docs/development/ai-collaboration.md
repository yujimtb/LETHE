# AI Collaboration Guide

LETHE の開発では、人間が意味論・ガバナンス・受け入れ判断を所有し、
エージェントは設計、実装、検証を担当する。

## Required workflow

1. OpenSpec change で目的、非目標、影響する System Law を明示する。
2. 実装前に依存方向と失敗条件を確定する。
3. 実装と同じ変更で unit / integration test を更新する。
4. 実装完了後に README、仕様、運用文書を現在の挙動へ同期する。
5. `cargo fmt --check`、workspace test、依存検査、公開監査を通す。

## Ownership

- 人間: 意味論、consent、公開範囲、最終承認
- Spec Designer: interface、invariant、acceptance criteria
- Implementer: 実装、テスト、migration、文書更新
- Reviewer: law、権限、lineage、replay、secret exposure の検証

## Implementation posture

- 後方互換は要件として明示された場合だけ実装する。
- silent fallback を置かず、安全に継続できない場合は明示的に失敗させる。
- 生成物、credential、runtime data をソースツリーへ混在させない。
- 完了チェックはファイルの存在ではなく、実装と検証結果で判断する。
