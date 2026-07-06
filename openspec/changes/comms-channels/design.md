# Design: comms-channels

**Date:** 2026-07-06
**Session:** 仕様決定 grill-me(Q18, Q23)。v2 設計記録の C20–C22(通信ループとモード系)を前提とする。

---

## 決定台帳

### D1. 初期チャネルは Slack / Gmail / Discord の3本、全て ingest+返信 SLO 対象(Q23)

v2 の優先順位(Slack, Gmail の2本開始)を改め、Discord を最初から ingest+返信 SLO 対象に含めた3チャネルで開始する。Discord はカード承認インターフェース(change ② CARD-02)としてどのみち adapter を作るため、取り込み口を同じ機構に載せる増分は小さく、着信の見落とし面を最初から塞ぐ価値が勝る。チャネルレジストリの設計上、以後の追加(Teams 等)は adapter 一枚である。

### D2. チャネルレジストリは ops 宣言・generic 管理

チャネルは「識別子・種別(slack / gmail / discord / …)・接続設定への参照・consent_scope 既定・SLO 値(初期 30 分)・break-glass ホワイトリスト・有効/無効」を持つレコードとして ops 設定で宣言され、起動時にレジストリへ載る。consent_scope の推奨既定は v2 決定の通り: 組織 Slack/Teams = org_federated、DM・メンション・自発言 = personal。break-glass ホワイトリスト(例: 寮インフラ障害系チャネル、GX 納期直撃の特定送信者)はチャネル単位・送信者単位の両方で宣言でき、モード系(change ④)が focus 中でも割り込みを許す判定に使う。LETHE 側の責務はこの宣言の保持と観測への正しい付与までであり、割り込み判定・エスカレーション実行は runtime の仕事。

### D3. LETHE は通信の読み取り側のみを持つ

送信用トークン・送信 API 呼び出しは一切 LETHE に置かない。LETHE が持つのは着信の観測化と、チャネル文脈のメタデータ付与だけである。これにより、公開 MCP 面を持つ LETHE プロセスが侵害されても、対外送信能力は奪えない — 送信能力は LAN 内の runtime プロセスにのみ存在する。change ② D1(LLM を呼ぶものは LETHE に置かない)と対になる境界規則:「世界へ作用するものも LETHE に置かない」。

### D4. 着信観測の SLO 素材

各着信観測には着信時刻・チャネル参照・送信者・スレッド文脈を付与する。返信要否の判定・返信滞留時間の計測は runtime とメトリクス projection(着信→送信 send-record の突合)で行うため、LETHE 側は素材の完全性のみ保証する。Gmail はスレッド構造(Message-ID / References)を保持し、get_thread がメールスレッドを会話として復元できるようにする。

### D5. 冪等性

identity key 規則は既存原則に従う: `slack:{channel}:{ts}:H(canonical)` 系(既存機構踏襲)、`gmail:{message_id}:H(canonical)`、`discord:{channel_id}:{message_id}:H(canonical)`。published は各プラットフォームのメッセージ時刻。再取り込みは全件 duplicate。

## 未決事項

- Gmail の取得方式(API ポーリング間隔)と Discord の gateway 常駐をどちらのプロセスに置くか — 常駐が必要な購読(Discord gateway, Slack socket mode)は runtime supervisor に置き、LETHE の HTTP 取り込み口へ流す形を既定とする(実装時に確定し evidence を記録)
