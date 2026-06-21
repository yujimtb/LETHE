# Issue R2-2: MVP の Slack Schema と Adapter 設計

**Labels:** [MVP]
**Priority:** High
**Status:** Approved (source adapter policy を先行策定)

---

## 問題

MVP シナリオ（plan.md §11.4）で Slack のデータ取り込みを定義したが、**Slack 用の schema、adapter、capture 戦略**が未定義。Slack は以下の特性を持ち、Google Slides とは異なる設計が必要:

- メッセージは基本的に immutable（edit / delete はあるが主流はappend）
- thread / reply の階層構造がある
- reaction / file attachment がある
- channel / DM / group の scope がある
- Bot message と human message の区別がある

## 影響

- MVP の実装に着手できない
- 名寄せに必要な Slack user identity の取得方法が不明

## 提案

### Schema 設計

```yaml
- id: "schema:slack-message"
  name: "Slack Message"
  version: "1.0.0"
  subject_type: "et:person"          # 送信者
  target_type: "et:slack-channel"    # 送信先 channel
  payload_schema:
    type: object
    properties:
      message_type: { enum: ["message", "reply", "bot-message"] }
      text: { type: string }
      ts: { type: string }           # Slack の message timestamp (unique id)
      thread_ts: { type: string }    # thread parent の ts (reply の場合)
      reactions: { type: array }
      file_refs: { type: array }
      edited: { type: boolean }
    required: ["message_type", "text", "ts"]
```

### Capture 戦略

| 方式 | 説明 | 推奨 |
|---|---|---|
| **初回 full export** | conversations.history API で全メッセージを取得 | MVP 初回 |
| **差分 crawl** | oldest パラメータで前回 cursor 以降のみ取得 | 継続運用 |
| **Event subscription** | Slack Events API で real-time にメッセージを受信 | Growth phase |

### Authority model

`lake-authoritative`（取り込み後は Lake が正史）

### Observer 登録

```yaml
- id: "obs:slack-crawler"
  observer_type: "crawler"
  source_system: "sys:slack"
  schemas: ["schema:slack-message", "schema:slack-channel-snapshot"]
  authority_model: "lake-authoritative"
  capture_model: "event"
  owner: "infra-team"
  trust_level: "automated"
```

### 名寄せ用の identity 情報

- Slack user id (`U...`) をメッセージの actor として記録
- Slack user profile（display name, email, real name）を別途 `schema:slack-user-profile` として取り込む
- Google account と Slack の email を突合キーにして名寄せする

---

## ユーザー回答

この仕様で良いと思いますが、先にsource adapterのポリシーを定めてから開発に進んだ方が混乱が少なくなると思います。
次回のopen_issuesでadapterポリシーを提案してください。

---

## 次のアクション

- source adapter policy を策定（R2-2 / R2-6 共通の前提条件）
- plan.md に schema 追加
- adapter 実装着手
