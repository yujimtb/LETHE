# Person Page Notion Write-back Design Specification

**Version:** 1.2
**Date:** 2026-06-22
**Scope:** `crates/adapters/notion/src/notion/client/` および関連モジュールの Notion個人ページwrite-back
**Status:** Implemented reference adapter

---

## 0. この仕様の位置づけ

### 0.1 意味論上の位置

この仕様は `SaaSWriteAdapter` trait の Notion 実装が出力する **Notion ページの構造** を定義する。

**Notion ページは Projection materialization（派生ビュー）であり、正史（canonical truth）ではない。**

| 項目 | 値 | 根拠 |
|---|---|---|
| **Authority Model** | `SourceAuthoritative` | Google Slides が live authority を保持する。Notion は managed view。 |
| **Write Mode** | `Annotation`（既定）/ `Proposal`（review 必要時） | Notion への書き込みは Lake / source に対する canonical write ではない。 |
| **Read-back 禁止** | Notion properties を Lake に逆流させない | No Direct Mutation Law — Projection materialization を正史として更新しない。 |
| **Filtering-before-Exposure** | write-back 直前に `FilteringGate` を通す | restricted data は表示・配布前に filtering projection を通す。 |

### 0.2 System Law チェックリスト

本仕様の実装が遵守すべき law（`system-overview.md` §0.4 / `domain-algebra.md` §7）：

| Law | 本仕様での保証方法 |
|---|---|
| **Append-Only Law** | Notion write-back は Lake を変更しない。入力は Projection 出力のみ。 |
| **Replay Law** | `normalize_in_place()` + 全削除→再作成で冪等性を確保（§6.2）。 |
| **Effect Isolation Law** | `build_page_blocks` は pure function。hidden mutable state に依存しない（§7）。 |
| **Explicit Authority Law** | Authority Model を `SourceAuthoritative` と宣言済み。Write Mode は `Annotation`。 |
| **No Direct Mutation Law** | Notion page は derived view。Notion 上の手動編集は次回 sync で上書きされ、Lake には伝播しない。 |
| **Filtering-before-Exposure Law** | write-back 前に `FilteringGate::check()` を通す（§6.1 step 2）。restricted / opt-out 対象を除外。 |

### 0.3 スコープ

データの取得元である `StudentProfile`（`crates/profile-model/src/types.rs`）、`PersonProfile`（`crates/projections/person/src/person_page/types.rs`）、`FrontendProfile` の型定義には変更を加えない。変更対象は write-back アダプタが `WriteRecord` の `payload` を Notion API コールに変換する部分のみである。

### 関連ファイル（読み取り専用・変更不要）

```
crates/profile-model/src/types.rs        — StudentProfile, StudentProperties, ProfilePic, GalleryImage
crates/projections/person/src/person_page/types.rs           — PersonProfile, FrontendProfile, PersonActivity
crates/projections/person/src/person_page/projector.rs       — PersonPageProjector
crates/adapters/api/src/writeback.rs    — SaaSWriteAdapter, WriteRecord, WriteResult
crates/derivations/gemini/src/slide_analysis/gemini.rs       — GeminiSlideAnalyzer (Gemini プロンプト)
crates/policy/src/governance/              — PolicyEngine, FilteringGate, AuditLog
```

### 変更対象ファイル

```
crates/adapters/notion/src/notion/client/ — NotionWriteAdapter の実装（主な変更箇所）
crates/derivations/gemini/src/slide_analysis/gemini.rs           — Gemini プロンプトへの profile_pic 選定指示の追加
```

---

## 1. Notion Database プロパティスキーマ

### 1.1 表示プロパティ（ページヘッダ領域に表示）

ページタイトルプロパティ（Notion database の Title 列）は `Email` のまま維持する（既存 DB との互換性）。ただし本仕様では **別途 body 内で氏名を大きく表示する** ことでタイトルの役割を補う。

| Property Name | Notion 型 | ソースフィールド | 表示 | 備考 |
|---|---|---|---|---|
| *(title)* `Email` | Title | `profile.email \|\| profile.generated_email` | 表示 | 既存互換。DB ルックアップキー |
| `Nickname` | Rich text | `properties.nickname` | 表示 | |
| `Birthplace` | Rich text | `properties.birthplace` | 表示 | |
| `DoB` | Rich text | `properties.dob` | 表示 | |
| `Major_Interests` | Rich text | `properties.major` | 表示 | |
| `MBTI` | Rich text | `properties.mbti` | 表示 | |
| `Hashtag` | Rich text | `properties.hashtags.join(", ")` | 表示 | multi-select にしない（表記ゆれ問題回避のため rich text を維持） |
| `SNS` | URL | `properties.sns` | 表示 | URL 型でクリッカブルにする。値が URL でなければ rich text にフォールバック |

### 1.2 管理プロパティ（非表示に設定）

以下のプロパティは Notion database 上に存在するが、ページ閲覧時は **Hide property** にする。write-back アダプタはこれらを従来通り書き込む。

| Property Name | Notion 型 | 用途 | 値の由来 |
|---|---|---|---|
| `LETHE Person ID` | Rich text | 内部人物 ID | `PersonProfile.person_id` |
| `Source Slide URL` | URL | 元スライドへのリンク | `FrontendProfile.source_canonical_uri` |
| `Last Synced At` | Date | 最終同期日時 | write-back 実行時の UTC タイムスタンプ |
| `Projection Version` | Rich text | projection バージョン | `ProjectionBuildRequest.version` |
| `Status` | Status | sync ステータス | write-back adapter が成功/失敗に応じて設定 |
| `Visibility` | Checkbox | 公開フラグ | **`FilteringGate::check()` の結果に基づく**（下記参照） |

**Visibility の決定ロジック（Filtering-before-Exposure Law）:**

1. `FilteringGate::check(person_id, purpose=NotionWriteBack)` を呼ぶ
2. 結果が `Allow` → `Visibility = true`
3. 結果が `Deny` → **write-back 自体をスキップする**（Notion ページを作成/更新しない）
4. 結果が `RequireReview` → `Visibility = false` で write-back し、ページは非公開状態で作成。reviewer が承認後に `Visibility` を手動で `true` にする（この操作は `AuditLog` に記録される）

**注意:** Visibility の値は Notion 上で手動変更されても、Lake / Projection には逆流しない（No Direct Mutation Law）。次回 sync 時に FilteringGate の結果で再計算・上書きされる。

### 1.3 新設プロパティ

| Property Name | Notion 型 | ソースフィールド | 備考 |
|---|---|---|---|
| `Affiliation` | Rich text | `properties.affiliation` | 所属。現在は body 内にのみ記載されている |

`Affiliation` プロパティの追加は **optional**（ギャラリービューでフィルタする必要がなければ body 内記載のみで十分）。

---

## 2. ページ視覚構造

### 2.1 ページカバー画像

スライドのサムネイル画像を Notion ページのカバーに設定する。

**ソース優先順位:**

1. `FrontendProfile.thumbnail_url`（Google Slides export URL）
2. `FrontendProfile.thumbnail_ref` を `LETHE_PUBLIC_BASE_URL` 経由で公開 URL 化 → `GET /public/blobs/{sha256}`
3. Notion File Upload API で blob をアップロードし Notion-hosted URL を取得

**Notion API:**

```
PATCH /v1/pages/{page_id}
{
  "cover": {
    "type": "external",
    "external": { "url": "<thumbnail_url>" }
  }
}
```

カバー画像が取得できない場合（`thumbnail_url` も `thumbnail_ref` も `None`）はカバーを設定しない（`cover: null`）。

### 2.2 ページアイコン（プロフィール写真）

`StudentProfile.profile_pic` のデータに基づきプロフィール写真を設定する。

**⚠️ Confidence Gate（`governance-capability-model.md` §3.5 準拠）:**

プロフィール写真は Gemini AI が推定したものであり、identity resolution の一種である。materialization 前に confidence を評価する：

| Confidence | 動作 | 根拠 |
|---|---|---|
| **High**（`profile_pic.coordinates` あり かつ `description` に顔/portrait を示す語を含む） | アイコンに設定可 | `resolved_persons` 自動昇格相当 |
| **Medium**（`profile_pic.coordinates` あり だが `description` が曖昧） | Supplemental `resolution_candidates` に留める。アイコンは**設定しない** | manual review 必須。approval trace に残す |
| **Low**（`profile_pic` が `None`） | アイコンを設定しない | — |

> **MVP 簡易実装:** Confidence 判定を厳密に実装するまでは、`profile_pic.coordinates.is_some()` を High 相当として扱い、存在すればアイコンに設定してよい。ただし、`AuditLog` に `{ action: "profile_pic_materialized", person_id, confidence: "assumed_high", source: "gemini" }` を記録すること。将来の review / opt-out 対応時に遡及できるようにする。

**プロフィール写真の選定（§3 で詳述）:**  
Gemini AI がスライド画像内の「本人の顔写真またはアバター画像」の座標とキャプションを返す（`profile_pic.coordinates`, `profile_pic.description`）。write-back アダプタは以下の手順でアイコン画像を取得する。

1. `profile_pic.coordinates` が存在し、confidence gate を通過した場合:
   - スライドサムネイル画像（PNG）から座標に基づいてクロッピングする
   - クロッピング結果を blob store に保存し、公開 URL を生成する
2. `profile_pic.url` が存在し、confidence gate を通過した場合（Google Slides の imageElement source URL など）:
   - その URL をそのまま使用する
3. いずれもない場合、または confidence gate で reject された場合:
   - アイコンを設定しない（Notion デフォルトのページアイコン）

**Notion API:**

```
PATCH /v1/pages/{page_id}
{
  "icon": {
    "type": "external",
    "external": { "url": "<profile_pic_url>" }
  }
}
```

---

## 3. Gemini プロンプト変更: プロフィール写真選定の明示化

`crates/derivations/gemini/src/slide_analysis/gemini.rs` の `extract_profile_from_png` メソッド内のプロンプトに以下の指示を追加する。

### 3.1 現在のプロンプト内 `profile_pic` 部分

```json
"profile_pic": {
    "coordinates": { "x": 50, "y": 50 },
    "description": "Visual description of the person",
    "url": null
}
```

### 3.2 変更後のプロンプト内 `profile_pic` 部分

プロンプト文字列内の `profile_pic` のスキーマ説明を以下に差し替える:

```
"profile_pic": {
    "coordinates": { "x": <percentage 0-100 from left>, "y": <percentage 0-100 from top> },
    "description": "Visual description of the person in this photo",
    "url": null
}
```

さらに、プロンプトの冒頭指示部分（`"Analyze this student self-introduction slide..."` の直後）に以下のガイダンスを追加する:

```
For profile_pic: Identify the PRIMARY photo that shows the student themselves
(their face, portrait, or personal avatar). This is typically the largest
person photo on the slide, or a photo explicitly labeled as a profile picture.
Do NOT select group photos, landscape photos, pet photos, or hobby images.
The coordinates should point to the CENTER of that image as a percentage of
the total slide dimensions (x: 0=left edge, 100=right edge;
y: 0=top edge, 100=bottom edge). If no clear personal photo exists,
set profile_pic to null.

For gallery_images: List ALL other photos/images on the slide that are NOT
the profile picture. These typically show hobbies, pets, scenery, food, etc.
```

### 3.3 型への影響

`StudentProfile.profile_pic` (`ProfilePic` 型) と `StudentProfile.gallery_images` (`Vec<GalleryImage>`) の型定義は変更不要。既に `coordinates: Option<ImageCoordinates>`, `description: Option<String>`, `url: Option<String>` を持っている。

### 3.4 Confidence との連携

Gemini の出力は Supplemental derivation（reusable but non-canonical）に分類される（`domain-algebra.md` §4.1）。Gemini が返す `profile_pic` / `gallery_images` は自動的に Lake 正史にはならない。

write-back アダプタが Gemini 出力を Notion に materialization する際は §2.2 の confidence gate を通す。Gemini プロンプトの改善で精度が上がっても、**materialization 判定はアダプタ側で行い、プロンプト側には含めない**（Effect Isolation Law — ドメイン解釈は hidden mutable state に依存しない）。

---

## 4. ページ本文 Block 構成

write-back アダプタは Notion API の `PATCH /v1/blocks/{page_id}/children` で以下の block 配列を生成する。**値が `None` または空文字列のセクションは block 自体を生成しない** ことが最重要ルールである。

### 4.1 Block 配列の順序

```
[1] Callout         — bio_text（自己紹介文）
[2] Divider
[3] Heading 2       — "About"
[4] Column List     — [左カラム: プロフィール写真] [右カラム: 基本情報テーブル]
[5] Divider
[6] Heading 2       — "Highlights"
[7] Paragraph       — Hobbies（趣味）
[8] Paragraph       — Interests（興味）
[9] Paragraph       — Likes & Favorites
[10] Divider
[11] Toggle(s)      — 深掘りセクション（存在するもののみ）
[12] Divider
[13] Heading 2      — "Gallery"
[14] Column List    — ギャラリー画像群
[15] Divider
[16] Heading 2      — "Source"
[17] Bookmark       — 元スライドへのリンク
```

### 4.2 各 block の詳細仕様

#### [1] Callout — 自己紹介文

**条件:** `profile.bio_text.is_some()` かつ空文字列でない

```json
{
  "type": "callout",
  "callout": {
    "icon": { "type": "emoji", "emoji": "💬" },
    "rich_text": [
      { "type": "text", "text": { "content": "<bio_text>" } }
    ],
    "color": "gray_background"
  }
}
```

`bio_text` が 2000 文字を超える場合は 2000 文字で切り、末尾に `…` を付加する（Notion rich_text content の上限は 2000 文字）。

#### [2] Divider

```json
{ "type": "divider", "divider": {} }
```

Divider はセクション間に挿入する。前後のセクションが両方とも出力される場合のみ挿入する（片方が空でスキップされた場合、Divider もスキップ）。

#### [3]-[4] About セクション

**条件:** `profile_pic` が存在する、**または** `properties` の基本情報（nickname, birthplace, dob, major, affiliation, mbti, sns）のいずれかが存在する

**[3] Heading 2:**

```json
{
  "type": "heading_2",
  "heading_2": {
    "rich_text": [{ "type": "text", "text": { "content": "About" } }]
  }
}
```

**[4] Column List（2列）:**

左カラム（幅比率 1:2 — Notion API ではカラム幅は指定不可。カラム数で均等分割される。画像の表示サイズで視覚的な比率を調整）:

```json
{
  "type": "column_list",
  "column_list": { "children": [<left_column>, <right_column>] }
}
```

**左カラム:**

```json
{
  "type": "column",
  "column": {
    "children": [
      {
        "type": "image",
        "image": {
          "type": "external",
          "external": { "url": "<profile_pic_url>" }
        }
      }
    ]
  }
}
```

`profile_pic` が取得できない場合、左カラムごと省略し、右カラムのみの単一カラム構成にする（column_list ではなく直接 block を配置）。

**右カラム — 基本情報テーブル:**

```json
{
  "type": "column",
  "column": {
    "children": [
      {
        "type": "table",
        "table": {
          "table_width": 2,
          "has_column_header": false,
          "has_row_header": false,
          "children": [<table_rows>]
        }
      }
    ]
  }
}
```

テーブル行の生成ルール — **値が存在するフィールドのみ行を生成する**:

| 表示ラベル | ソースフィールド | 行生成条件 |
|---|---|---|
| `呼び名` | `properties.nickname` | `nickname.is_some()` |
| `出身` | `properties.birthplace` | `birthplace.is_some()` |
| `誕生日` | `properties.dob` | `dob.is_some()` |
| `専攻` | `properties.major` | `major.is_some()` |
| `所属` | `properties.affiliation` | `affiliation.is_some()` |
| `MBTI` | `properties.mbti` | `mbti.is_some()` |
| `SNS` | `properties.sns` | `sns.is_some()` |

各行の Notion API 表現:

```json
{
  "type": "table_row",
  "table_row": {
    "cells": [
      [{ "type": "text", "text": { "content": "呼び名" }, "annotations": { "bold": true } }],
      [{ "type": "text", "text": { "content": "<value>" } }]
    ]
  }
}
```

SNS フィールドが URL の場合は link 付き rich_text にする:

```json
[{ "type": "text", "text": { "content": "<url>", "link": { "url": "<url>" } } }]
```

テーブル行が 0 件の場合、About セクション全体（Heading 含む）をスキップする。

#### [5]-[9] Highlights セクション

**条件:** `properties.hobbies`, `properties.interests`, `properties.likes` のいずれかが空でない

**[6] Heading 2:**

```json
{
  "type": "heading_2",
  "heading_2": {
    "rich_text": [{ "type": "text", "text": { "content": "Highlights" } }]
  }
}
```

**[7] Hobbies:**

**条件:** `properties.hobbies` が空でない

```json
{
  "type": "paragraph",
  "paragraph": {
    "rich_text": [
      { "type": "text", "text": { "content": "🎯 Hobbies: " }, "annotations": { "bold": true } },
      { "type": "text", "text": { "content": "<hobbies.join(\", \")>" } }
    ]
  }
}
```

**[8] Interests:**

**条件:** `properties.interests` が空でない

```json
{
  "type": "paragraph",
  "paragraph": {
    "rich_text": [
      { "type": "text", "text": { "content": "🔍 Interests: " }, "annotations": { "bold": true } },
      { "type": "text", "text": { "content": "<interests.join(\", \")>" } }
    ]
  }
}
```

**[9] Likes & Favorites:**

**条件:** `properties.likes` が空でない

```json
{
  "type": "paragraph",
  "paragraph": {
    "rich_text": [
      { "type": "text", "text": { "content": "❤️ Likes: " }, "annotations": { "bold": true } },
      { "type": "text", "text": { "content": "<likes.join(\", \")>" } }
    ]
  }
}
```

3 つとも空の場合、Highlights セクション全体（Heading + Divider 含む）をスキップする。

#### [10]-[11] 深掘りセクション（Toggle blocks）

以下の各フィールドについて、値が存在する場合のみ Toggle block を生成する。

| Toggle タイトル | ソースフィールド | 絵文字 |
|---|---|---|
| `New Challenges` | `properties.new_challenges` | 🚀 |
| `Ask Me About` | `properties.ask_me_about` | 💡 |
| `Turning Point` | `properties.turning_point` | 🔄 |
| `BTW` | `properties.btw` | 💭 |
| `Message` | `properties.message` | ✉️ |
| `Dislikes` | `properties.dislikes` | 🙅 |

各 Toggle の Notion API 表現:

```json
{
  "type": "toggle",
  "toggle": {
    "rich_text": [
      { "type": "text", "text": { "content": "<emoji> <title>" }, "annotations": { "bold": true } }
    ],
    "children": [
      {
        "type": "paragraph",
        "paragraph": {
          "rich_text": [
            { "type": "text", "text": { "content": "<value>" } }
          ]
        }
      }
    ]
  }
}
```

すべての Toggle 対象フィールドが `None` の場合、Divider も含めてスキップする。

#### [12]-[14] Gallery セクション

**条件:** `profile.gallery_images` が空でない、**かつ** 少なくとも 1 つの画像が `coordinates` または `url` を持つ

**[13] Heading 2:**

```json
{
  "type": "heading_2",
  "heading_2": {
    "rich_text": [{ "type": "text", "text": { "content": "Gallery" } }]
  }
}
```

**[14] Column List（最大 3 列）:**

`gallery_images` を最大 3 個ずつの行にグループ化し、各行を `column_list` として出力する。

各カラムの中身:

```json
{
  "type": "column",
  "column": {
    "children": [
      {
        "type": "image",
        "image": {
          "type": "external",
          "external": { "url": "<gallery_image_url>" }
        }
      },
      {
        "type": "paragraph",
        "paragraph": {
          "rich_text": [
            {
              "type": "text",
              "text": { "content": "<description>" },
              "annotations": { "italic": true, "color": "gray" }
            }
          ]
        }
      }
    ]
  }
}
```

`description` が `None` の場合、キャプション paragraph は省略する。

**ギャラリー画像 URL の取得:**

1. `GalleryImage.url` が存在する場合はそのまま使用
2. `GalleryImage.coordinates` が存在する場合はスライドサムネイルからクロッピング → blob store → 公開 URL
3. いずれもない画像はスキップする

最大表示枚数: **9 枚**（3×3）。それ以上はスライドへのリンクで参照してもらう。

#### [15]-[17] Source セクション

**条件:** 常に出力する（元スライドのリンクは常に有用）

**[17] Bookmark:**

```json
{
  "type": "bookmark",
  "bookmark": {
    "url": "<source_canonical_uri || source_slide_url>",
    "caption": [
      { "type": "text", "text": { "content": "Google Slides — 自己紹介スライド原本" } }
    ]
  }
}
```

`source_canonical_uri` が `None` の場合は `Source Slide URL` プロパティの値を使用する。両方 `None` の場合は Source セクション全体をスキップする。

---

## 5. 画像クロッピング処理

### 5.1 概要

`ProfilePic.coordinates` および `GalleryImage.coordinates` は、スライド全体を 100×100 のパーセンテージ座標系で表した画像の中心点を示す。この座標からスライドサムネイル PNG 上の矩形領域を切り出す。

### 5.2 クロッピングアルゴリズム

```
入力:
  - image: PNG バイト列 (スライドサムネイル、通常 1920×1080 または相当)
  - center_x: f64 (0.0 - 100.0)
  - center_y: f64 (0.0 - 100.0)
  - crop_type: "profile" | "gallery"

処理:
  1. center_pixel_x = (center_x / 100.0) * image_width
     center_pixel_y = (center_y / 100.0) * image_height

  2. crop_size を決定:
     - profile: min(image_width, image_height) * 0.25  (正方形)
     - gallery: min(image_width, image_height) * 0.30  (正方形)

  3. 矩形を計算:
     left   = max(0, center_pixel_x - crop_size / 2)
     top    = max(0, center_pixel_y - crop_size / 2)
     right  = min(image_width,  left + crop_size)
     bottom = min(image_height, top  + crop_size)

  4. PNG としてクロッピング → バイト列を返す
```

### 5.3 実装場所

新規モジュール `crates/adapters/notion/src/image_crop.rs` を作成する。

```rust
pub struct CropRegion {
    pub left: u32,
    pub top: u32,
    pub width: u32,
    pub height: u32,
}

pub enum CropType {
    Profile,
    Gallery,
}

/// スライドサムネイルからクロッピングした PNG バイト列を返す。
/// image crate を使用する。
pub fn crop_from_thumbnail(
    thumbnail_png: &[u8],
    center_x_pct: f64,
    center_y_pct: f64,
    crop_type: CropType,
) -> Result<Vec<u8>, CropError>;
```

依存 crate: `image = "0.25"` を `Cargo.toml` に追加する。

### 5.4 クロッピング結果の保存と公開

1. `crop_from_thumbnail` で得た PNG バイト列を blob store に保存する（`BlobStore::put`）
2. `LETHE_PUBLIC_BASE_URL` が設定されている場合: `{base_url}/public/blobs/{sha256}` を URL として使用
3. `LETHE_PUBLIC_BASE_URL` が未設定の場合: Notion File Upload API でアップロードする（既存の thumbnail blob 公開ロジックと同じフォールバック）

**Filtering-before-Exposure:** クロッピングされた画像（特に顔写真）は incidental capture に該当しうる（`governance-capability-model.md` §3.4）。公開 URL を生成する前に以下を確認する：

- `FilteringGate::check(person_id, purpose=ImageExposure)` が `Allow` であること
- `Deny` の場合は画像を blob store には保存するが、公開 URL は生成せず、Notion ページ上の画像 block はスキップする
- blob store 保存は restricted canonical capture として許容される（capture と serve の policy は分離）

---

## 6. Write-back 実行フロー

### 6.1 `NotionWriteAdapter::write_record` の処理手順

```
入力: WriteRecord {
    entity_id: String,    // email
    title: String,        // name
    payload: Value,       // serde_json::to_value(&StudentProfile)
    external_id: Option<String>,  // 既存 Notion page ID (update 時)
}

処理:
  1. payload を StudentProfile にデシリアライズ
  2. **Policy Gate (Filtering-before-Exposure Law):**
     a. FilteringGate::check(person_id, purpose=NotionWriteBack) を呼ぶ
     b. Deny → WriteResult::Skipped を返す。AuditLog に記録。処理終了。
     c. RequireReview → visibility=false で続行（非公開状態で作成）
     d. Allow → visibility=true で続行
  3. StudentProfile.normalize_in_place() を呼ぶ（冪等性のため）

  4. Notion ページの properties を構築 (§1 に従う)
     - Visibility は step 2 の結果を使用
  5. cover 画像 URL を決定 (§2.1 に従う)
  6. icon 画像 URL を決定 (§2.2 confidence gate, §5 に従う)

  7-a. external_id が None の場合 (新規作成):
       POST /v1/pages { parent: { database_id }, properties, cover, icon }
  7-b. external_id が Some の場合 (更新):
       PATCH /v1/pages/{page_id} { properties, cover, icon }

  8. ページ本文を構築 (§4 に従う)
  9. 既存の body blocks を全削除:
       GET  /v1/blocks/{page_id}/children → 各 block の ID を取得
       DELETE /v1/blocks/{block_id}       → 全削除
  10. 新しい body blocks を追加:
       PATCH /v1/blocks/{page_id}/children { children: [<blocks>] }

  11. AuditLog に書き込み結果を記録:
      { action: "notion_writeback", person_id, page_id, write_mode: "Annotation",
        authority_model: "SourceAuthoritative", visibility, projection_version }

  12. WriteResult を返す
```

### 6.2 冪等性

同一の `StudentProfile` に対して `write_record` を複数回呼んでも結果が同一になること。これは以下により保証される:

- `normalize_in_place()` で入力を正規化してからレンダリングする
- body blocks は毎回全削除→再作成する（差分更新ではない）
- properties, cover, icon は PATCH で上書きする

### 6.3 レート制限

Notion API のレート制限（3 requests/sec）に対応するため、`write_batch` では各 `write_record` 呼び出しの間に 400ms の `sleep` を入れる。body block の削除ループでも同様。

### 6.4 Read-back 禁止（No Direct Mutation Law）

**Notion の properties / body を Lake や Supplemental に読み戻す操作を行ってはならない。**

- write-back の成否確認は HTTP status code で判定する（Notion ページの GET は行わない）
- Notion 上でユーザーが手動編集した内容は、次回 sync 時に Projection 出力で上書きされる
- 「Notion の編集を Lake に反映したい」というユースケースが発生した場合は、Notion を SourceAuthoritative な別 Observer として扱い、独立した Observation Contract を設計する（本仕様のスコープ外）

---

## 7. Block 配列生成関数のインターフェース

`crates/adapters/notion/src/notion/client/` に以下の関数を実装する。

```rust
/// StudentProfile から Notion page body の block 配列を生成する。
/// 値が None / 空のセクションは block を生成しない。
///
/// `profile_pic_url`: §5 のクロッピング処理で得た公開 URL (None ならアイコンなし)
/// `gallery_urls`:    §5 のクロッピング処理で得た公開 URL のリスト (index は gallery_images に対応)
/// `source_url`:      元スライドの canonical URI
fn build_page_blocks(
    profile: &StudentProfile,
    profile_pic_url: Option<&str>,
    gallery_urls: &[(usize, String)],  // (gallery_images index, url)
    source_url: Option<&str>,
) -> Vec<serde_json::Value>;
```

この関数は **pure function** であり、Notion API 呼び出しを行わない。テスト可能性のためにアダプタの I/O ロジックと分離する。

---

## 8. テスト要件

### 8.1 ユニットテスト (`crates/adapters/notion/src/notion/client/`)

| テスト名 | 検証内容 |
|---|---|
| `empty_profile_produces_minimal_blocks` | 全フィールドが None/空の `StudentProfile` に対して、block 配列が空 or Source セクションのみ |
| `full_profile_produces_all_sections` | 全フィールドが埋まった `StudentProfile` に対して、Callout → About → Highlights → Toggles → Gallery → Source の順序で block が生成される |
| `missing_bio_skips_callout` | `bio_text: None` の場合、Callout block が生成されない |
| `missing_hobbies_interests_likes_skips_highlights` | 3 フィールドとも空の場合、Highlights セクション全体が欠落する |
| `partial_toggles_only_present_fields` | `new_challenges` のみ Some で他が None の場合、Toggle が 1 つだけ生成される |
| `gallery_respects_max_9` | `gallery_images` が 12 個の場合、最大 9 枚分の block のみ生成される |
| `dividers_not_orphaned` | スキップされたセクション間の Divider が残らない |
| `sns_url_becomes_link` | `sns` が `https://` で始まる場合、link 付き rich_text になる |
| `about_table_skips_none_rows` | `nickname: Some, birthplace: None` の場合、テーブルが 1 行のみ |
| `normalize_is_idempotent` | 2 回 normalize → build_page_blocks した結果が 1 回と同一 |

### 8.2 クロッピングテスト (`crates/adapters/notion/src/image_crop.rs`)

| テスト名 | 検証内容 |
|---|---|
| `crop_center_returns_square` | 中央座標 (50, 50) でクロッピング結果が正方形 |
| `crop_edge_clamps_to_bounds` | 端の座標 (0, 0) でパニックしない |
| `profile_crop_smaller_than_gallery` | profile と gallery でサイズが異なる |

### 8.3 Governance テスト

| テスト名 | 検証内容 |
|---|---|
| `filtering_gate_deny_skips_writeback` | `FilteringGate::check` が `Deny` を返した場合、Notion API が呼ばれず `WriteResult::Skipped` が返る |
| `filtering_gate_require_review_sets_visibility_false` | `RequireReview` 時に `Visibility = false` で write-back される |
| `profile_pic_low_confidence_skips_icon` | `profile_pic` が `None` の場合、icon が設定されない |
| `audit_log_records_writeback` | write-back 成功時に AuditLog にエントリが記録される |
| `no_readback_from_notion` | write-back 後に Notion GET API が呼ばれていないことを検証（mock 検査） |

### 8.4 統合テスト

`cargo test` で既存の `person_page_with_slides_and_messages` 等のテストが引き続きパスすること。`PersonPageProjector` と `SlideAnalysisProjector` の型には変更を加えないため、これは自動的に満たされる。

---

## 9. 実装順序

| Phase | 作業内容 | 変更ファイル |
|---|---|---|
| **Phase 1** | `build_page_blocks` 関数の実装 + ユニットテスト | `crates/adapters/notion/src/notion/client/` |
| **Phase 2** | `NotionWriteAdapter::write_record` 内で `build_page_blocks` を使用。properties の書き込みを §1 に従い更新。cover 設定を追加 | `crates/adapters/notion/src/notion/client/` |
| **Phase 3** | `FilteringGate` 統合。write-back 前の policy check、Visibility 決定ロジック、AuditLog 記録を実装。governance テスト追加 | `crates/adapters/notion/src/notion/client/`, `crates/policy/src/governance/` |
| **Phase 4** | `image_crop.rs` の新規作成。`Cargo.toml` に `image` crate 追加。icon 設定ロジック + confidence gate を追加 | `crates/adapters/notion/src/image_crop.rs`, `Cargo.toml`, `crates/adapters/notion/src/lib.rs`, `crates/adapters/notion/src/notion/client/` |
| **Phase 5** | Gemini プロンプトに `profile_pic` 選定ガイダンスを追加 | `crates/derivations/gemini/src/slide_analysis/gemini.rs` |

Phase 1-2 だけで本文構造の改善は完了する。Phase 3 は governance 統合で、System Law 準拠に必須。Phase 4-5 はプロフィール写真の精度向上であり、独立してリリース可能。

---

## Appendix A: 完成イメージ（Notion ページ構造の擬似表現）

```
┌─────────────────────────────────────────────────────┐
│ [Cover: スライドサムネイル画像]                         │
│                                                       │
│ 🧑 [Icon: プロフィール写真]                            │
│                                                       │
│ sayaka.hikono@hlab.college                            │
│ Nickname: さやか、ひめこ  │  Birthplace: 栃木県        │
│ MBTI: ○○○○             │  Hashtag: #ネコ #スプラ     │
│ ─────────────────────────────────────────────────── │
│                                                       │
│ 💬 いつでもレンジにマッシュポテト                        │
│    (自分で決めたいくつか大切なこと…bio_text全文)          │
│                                                       │
│ ─────────────────────────────────────────────────── │
│                                                       │
│ ## About                                              │
│ ┌──────────┬──────────────────────────────────────┐  │
│ │ [Profile │  呼び名    │ さやか、ひめこ            │  │
│ │  Photo]  │  出身      │ 栃木県                   │  │
│ │          │  専攻      │ 電工学部…                │  │
│ │          │  MBTI      │ ○○○○                   │  │
│ │          │  SNS       │ https://...              │  │
│ └──────────┴──────────────────────────────────────┘  │
│                                                       │
│ ─────────────────────────────────────────────────── │
│                                                       │
│ ## Highlights                                         │
│ 🎯 Hobbies: ネコ、コジプリ、残酷、ちいかわ、スプラ       │
│ 🔍 Interests: ネコのいるいる出身、エネルギー             │
│ ❤️ Likes: コーンスープ、ちいかわ…                      │
│                                                       │
│ ─────────────────────────────────────────────────── │
│                                                       │
│ ▶ 🚀 New Challenges                                  │
│ ▶ 💡 Ask Me About                                    │
│ ▶ 🔄 Turning Point                                   │
│ ▶ 💭 BTW                                             │
│                                                       │
│ ─────────────────────────────────────────────────── │
│                                                       │
│ ## Gallery                                            │
│ ┌─────────┬─────────┬─────────┐                      │
│ │ [img 1] │ [img 2] │ [img 3] │                      │
│ │ caption │ caption │ caption │                      │
│ └─────────┴─────────┴─────────┘                      │
│                                                       │
│ ─────────────────────────────────────────────────── │
│                                                       │
│ ## Source                                             │
│ 🔗 Google Slides — 自己紹介スライド原本                 │
│                                                       │
└─────────────────────────────────────────────────────┘
```

---

## Appendix B: Notion API Rate Limit 対策

Notion API の公開レート制限は **3 requests/second per integration** である。1 人分の write-back は以下の API コールを必要とする:

| 操作 | API コール数 |
|---|---|
| ページ作成/更新 (properties + cover + icon) | 1 |
| 既存 body blocks 取得 | 1 |
| 既存 body blocks 削除 | N (既存 block 数) |
| 新規 body blocks 追加 | 1 (append は 1 回で最大 100 blocks) |

削除ステップの N が大きい場合が律速になる。対策として:

1. 各 API コール間に `sleep(Duration::from_millis(400))` を挿入する
2. 429 レスポンスに対しては `Retry-After` ヘッダを尊重して待機する（既存の `crates/adapters/api/src/retry.rs` のリトライポリシーを使用）
3. `write_batch` では人物間に追加で `sleep(Duration::from_secs(1))` を挿入する
