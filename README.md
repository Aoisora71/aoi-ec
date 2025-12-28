# EC-Automation（EC自動化システム）

楽天市場向けの包括的なEC自動化プラットフォーム。商品管理、注文処理、AIコンテンツ生成、画像処理、物流管理などの機能を提供します。

## 📋 目次

- [概要](#概要)
- [主な機能](#主な機能)
- [技術スタック](#技術スタック)
- [プロジェクト構成](#プロジェクト構成)
- [セットアップ](#セットアップ)
- [環境変数の設定](#環境変数の設定)
- [使用方法](#使用方法)
- [API エンドポイント](#api-エンドポイント)
- [開発](#開発)
- [トラブルシューティング](#トラブルシューティング)
- [ライセンス](#ライセンス)

## 📖 概要

EC-Automationは、楽天市場でのEC事業を効率化するための統合プラットフォームです。Rakumart APIとRakuten APIを統合し、商品検索、商品情報の自動翻訳・最適化、注文管理、在庫管理、画像処理などの機能を提供します。

### 主な特徴

- 🤖 **AI による自動コンテンツ生成**: OpenAIを使用した商品タイトル・説明文の自動生成
- 🌐 **多言語対応**: DeepL APIによる商品仕様の自動翻訳
- 🖼️ **画像処理**: Google Gemini APIによる商品画像の自動処理・最適化
- 📦 **注文管理**: 注文の作成、更新、キャンセル、追跡機能
- 📊 **在庫管理**: リアルタイム在庫情報の取得・更新
- 🚚 **物流管理**: 配送方法の管理と追跡
- ✅ **コンプライアンスチェック**: 楽天市場のガイドライン準拠チェック
- 📈 **分析機能**: 売上・パフォーマンス分析

## ✨ 主な機能

### フロントエンド（クライアント）

- **ダッシュボード**: システム全体の概要と主要メトリクス
- **商品管理**: 商品の検索、登録、編集、削除
- **商品リサーチ**: Rakumart APIを使用した商品検索と情報取得
- **注文管理**: 注文の一覧表示、詳細確認、ステータス更新
- **物流管理**: 配送方法の設定と管理
- **コンプライアンスチェック**: 商品情報の楽天ガイドライン準拠確認
- **分析**: 売上データとパフォーマンス指標の可視化
- **設定**: システム設定とユーザー管理

### バックエンド（サーバー）

- **RESTful API**: FastAPIベースの高性能APIサーバー
- **認証・認可**: JWT トークンベースの認証システム
- **データベース統合**: PostgreSQL データベースとの統合
- **外部API統合**: 
  - Rakumart API（商品検索、注文管理）
  - Rakuten API（商品登録、在庫管理）
  - OpenAI API（コンテンツ生成）
  - DeepL API（翻訳）
  - Google Gemini API（画像処理）
  - AWS S3（画像ストレージ）

## 🛠️ 技術スタック

### フロントエンド

- **フレームワーク**: Next.js 15.5.4
- **UI ライブラリ**: React 19.1.0
- **言語**: TypeScript 5
- **スタイリング**: Tailwind CSS 4.1.9
- **UI コンポーネント**: Radix UI
- **フォーム管理**: React Hook Form + Zod
- **チャート**: Recharts
- **テーマ**: next-themes（ダークモード対応）

### バックエンド

- **フレームワーク**: FastAPI 0.104.0+
- **言語**: Python 3.x
- **データベース**: PostgreSQL（psycopg2-binary）
- **認証**: PyJWT + bcrypt
- **AI/ML**:
  - OpenAI API（コンテンツ生成）
  - DeepL API（翻訳）
  - Google Gemini API（画像処理）
- **ストレージ**: AWS S3（boto3）
- **画像処理**: OpenCV, Pillow
- **その他**: python-dotenv, openpyxl, pytz

## 📁 プロジェクト構成

```
EC-automation/
├── client/                 # フロントエンド（Next.js）
│   ├── app/               # Next.js App Router
│   │   ├── analytics/     # 分析ページ
│   │   ├── compliance/   # コンプライアンスチェック
│   │   ├── logistics/     # 物流管理
│   │   ├── products/      # 商品管理
│   │   ├── research/      # 商品リサーチ
│   │   └── settings/      # 設定
│   ├── components/        # React コンポーネント
│   │   └── ui/           # UI コンポーネント（Radix UI）
│   ├── lib/              # ユーティリティ関数
│   └── hooks/            # カスタムフック
│
├── server/                # バックエンド（FastAPI）
│   ├── modules/          # モジュール
│   │   ├── api_search.py      # 商品検索API
│   │   ├── config.py          # 設定管理
│   │   ├── db.py              # データベース操作
│   │   ├── deepl_trans.py     # DeepL翻訳
│   │   ├── image_pro.py       # 画像処理
│   │   ├── openai_utils.py    # OpenAI統合
│   │   ├── orders.py           # 注文管理
│   │   └── upload_file.py      # ファイルアップロード
│   ├── api_server.py     # FastAPI アプリケーション
│   ├── main.py           # CLI エントリーポイント
│   ├── requirements.txt  # Python依存関係
│   └── env.example       # 環境変数テンプレート
│
└── README.md             # このファイル
```

## 🚀 セットアップ

### 前提条件

- **Node.js**: 18.x 以上
- **Python**: 3.9 以上
- **PostgreSQL**: 12.x 以上
- **pnpm**: パッケージマネージャー（推奨）または npm/yarn

### 1. リポジトリのクローン

```bash
git clone <repository-url>
cd EC-automation
```

### 2. フロントエンドのセットアップ

```bash
cd client
pnpm install  # または npm install
```

### 3. バックエンドのセットアップ

```bash
cd server
python -m venv venv

# Windows
venv\Scripts\activate

# macOS/Linux
source venv/bin/activate

pip install -r requirements.txt
```

### 4. データベースのセットアップ

PostgreSQLデータベースを作成し、接続情報を準備してください。

```sql
CREATE DATABASE ec_automation;
```

## ⚙️ 環境変数の設定

### バックエンド環境変数

`server/env.example` を `server/.env` にコピーし、実際の値を設定してください。

```bash
cd server
cp env.example .env
```

必須の環境変数：

```env
# データベース（必須）
DATABASE_URL=postgresql://username:password@host:5432/database_name

# 認証（必須）
JWT_SECRET_KEY=your-secret-key-change-in-production

# Rakumart API（必須）
APP_KEY=your_rakumart_app_key
APP_SECRET=your_rakumart_app_secret

# OpenAI API（必須 - コンテンツ生成用）
OPENAI_API_KEY=your_openai_api_key_here
OPENAI_MODEL=gpt-4o-mini

# DeepL API（必須 - 翻訳用）
DEEPL_API_KEY=your_deepl_api_key_here
```

オプションの環境変数：

```env
# API サーバーポート
API_PORT=8000

# Google Gemini API（画像処理用）
GEMINI_API_KEY=your_gemini_api_key_here
IMAGE_PROCESSING_ENABLED=true

# AWS S3（画像ストレージ用）
AWS_ACCESS_KEY_ID=your_aws_access_key
AWS_SECRET_ACCESS_KEY=your_aws_secret_key
AWS_REGION=us-east-1
S3_BUCKET=licel-product-image
S3_FOLDER=products

# 為替レート
EXCHANGE_RATE=22.4
```

詳細は `server/env.example` を参照してください。

### フロントエンド環境変数（必要に応じて）

`client/.env.local` を作成（通常は不要）：

```env
NEXT_PUBLIC_API_URL=http://localhost:8000
```

## 🎯 使用方法

### 開発サーバーの起動

#### バックエンドサーバー

```bash
cd server
python api_server.py
```

サーバーは `http://localhost:8000` で起動します。

#### フロントエンドサーバー

```bash
cd client
pnpm dev  # または npm run dev
```

フロントエンドは `http://localhost:6009` で起動します。

### 本番環境でのビルド

#### フロントエンド

```bash
cd client
pnpm build
pnpm start
```

#### バックエンド

```bash
cd server
uvicorn api_server:app --host 0.0.0.0 --port 8000
```

## 📡 API エンドポイント

### 認証

- `POST /api/auth/register` - ユーザー登録
- `POST /api/auth/login` - ログイン
- `POST /api/auth/refresh` - トークンリフレッシュ
- `GET /api/auth/me` - 現在のユーザー情報取得

### 商品管理

- `GET /api/products` - 商品一覧取得
- `GET /api/products/{id}` - 商品詳細取得
- `POST /api/products` - 商品作成
- `PUT /api/products/{id}` - 商品更新
- `DELETE /api/products/{id}` - 商品削除
- `POST /api/products/search` - 商品検索
- `POST /api/products/enrich` - 商品情報の拡充

### 注文管理

- `GET /api/orders` - 注文一覧取得
- `GET /api/orders/{id}` - 注文詳細取得
- `POST /api/orders` - 注文作成
- `PUT /api/orders/{id}/status` - 注文ステータス更新
- `DELETE /api/orders/{id}` - 注文キャンセル

### その他

- `GET /api/health` - ヘルスチェック
- `GET /api/logistics` - 物流方法一覧
- `GET /api/tags` - タグ一覧
- `POST /api/upload` - ファイルアップロード

詳細なAPI仕様は、サーバー起動後に `http://localhost:8000/docs` でSwagger UIを確認できます。

## 💻 開発

### コードスタイル

- **フロントエンド**: ESLint + Prettier（Next.js標準設定）
- **バックエンド**: PEP 8準拠（Black推奨）

### テスト

```bash
# フロントエンドテスト
cd client
pnpm test

# バックエンドテスト（実装されている場合）
cd server
pytest
```

### データベースマイグレーション

データベーススキーマの変更が必要な場合は、`server/modules/db.py` のマイグレーション関数を実行してください。

## 🔧 トラブルシューティング

### よくある問題

#### 1. データベース接続エラー

- `DATABASE_URL` が正しく設定されているか確認
- PostgreSQLが起動しているか確認
- データベースが存在するか確認

#### 2. API認証エラー

- `JWT_SECRET_KEY` が設定されているか確認
- トークンが有効期限内か確認

#### 3. 外部API接続エラー

- 各APIキーが正しく設定されているか確認
- インターネット接続を確認
- APIレート制限に達していないか確認

#### 4. 画像処理が動作しない

- `GEMINI_API_KEY` または `GOOGLE_APPLICATION_CREDENTIALS` が設定されているか確認
- `IMAGE_PROCESSING_ENABLED=true` が設定されているか確認

### ログの確認

- **バックエンド**: コンソール出力または `server/logs.json`
- **フロントエンド**: ブラウザの開発者ツール（F12）

## 📝 ライセンス

このプロジェクトはプライベートプロジェクトです。無断での使用・複製・配布を禁止します。

## 🤝 サポート

問題が発生した場合や質問がある場合は、プロジェクト管理者に連絡してください。

## 📚 関連ドキュメント

- [Next.js ドキュメント](https://nextjs.org/docs)
- [FastAPI ドキュメント](https://fastapi.tiangolo.com/)
- [Rakumart API ドキュメント](https://apiwww.rakumart.com/)
- [楽天市場 API ドキュメント](https://webservice.rakuten.co.jp/)

---

**注意**: 本番環境で使用する前に、すべての環境変数を適切に設定し、セキュリティ設定を確認してください。

