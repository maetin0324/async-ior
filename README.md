# async-ior

C [IOR](https://github.com/hpc/ior) / mdtest ベンチマークの Rust 実装。MPI 並列実行と非同期 I/O (スレッドプール) をサポートする。

## 概要

| クレート | 説明 |
|----------|------|
| `ior-core` | コアライブラリ: `Aiori` トレイト、`IorParam`/`IorError` 型、タイマー、C FFI ブリッジ |
| `ior-backend-posix` | POSIX バックエンド: pread/pwrite による同期 I/O + スレッドプールによる非同期 I/O |
| `ior-bench` | IOR ベンチマーク CLI: MPI 並列 I/O 性能測定 |
| `mdtest-bench` | mdtest ベンチマーク CLI: MPI 並列メタデータ性能測定 |

## ビルド要件

- **Rust**: 2024 edition (nightly or stable 1.85+)
- **MPI**: OpenMPI, MPICH, またはその他の MPI 実装
- **C コンパイラ**: MPI バインディングのリンクに必要

## ビルド

```bash
cargo build --release
```

バイナリは `target/release/ior-bench` と `target/release/mdtest-bench` に生成される。

## クイックスタート

### IOR ベンチマーク

```bash
# 1プロセスで 1MiB ブロックの Write/Read テスト
mpiexec -n 1 target/release/ior-bench -w -r

# 4プロセスで 256KiB 転送サイズ、Direct I/O 有効
mpiexec -n 4 target/release/ior-bench -w -r --direct-io -t 256k -b 1m

# 非同期 I/O (キュー深度 4)
mpiexec -n 4 target/release/ior-bench -w -r -q 4

# JSON 出力 (stdout)
mpiexec -n 1 target/release/ior-bench -w -r --json

# JSON をファイルに出力 (テキストも表示)
mpiexec -n 1 target/release/ior-bench -w -r --json-file /tmp/ior.json
```

### mdtest ベンチマーク

```bash
# 1プロセスで 100 ファイルのメタデータテスト
mpiexec -n 1 target/release/mdtest-bench -n 100 -F

# 4プロセス、ディレクトリ階層あり
mpiexec -n 4 target/release/mdtest-bench -n 1000 -z 2 -b 3

# JSON 出力
mpiexec -n 1 target/release/mdtest-bench -n 100 -F --json
```

## テスト

```bash
cargo test
```

## アーキテクチャ

```
┌─────────────┐  ┌──────────────┐
│  ior-bench   │  │ mdtest-bench │   CLI (clap) + MPI 並列制御
└──────┬───────┘  └──────┬───────┘
       │                 │
       └────────┬────────┘
                │
       ┌────────▼────────┐
       │    ior-core      │   Aiori トレイト、IorParam、BenchTimers、C FFI
       └────────┬────────┘
                │
       ┌────────▼────────┐
       │ ior-backend-posix│   POSIX pread/pwrite + ThreadPool async I/O
       └─────────────────┘
```

### Aiori トレイト

I/O バックエンドの抽象インターフェース。C IOR の `ior_aiori_t` に対応する。

主要メソッド:
- `create` / `open` / `close` / `delete` — ファイル操作
- `xfer_sync` — 同期転送 (pread/pwrite)
- `xfer_submit` / `poll` / `cancel` — 非同期転送
- `mkdir` / `rmdir` / `stat` / `rename` / `mknod` — メタデータ操作

### 非同期 I/O

`queue_depth > 1` の場合、スレッドプールベースのパイプライン I/O が有効になる。各ランクは独立にパイプラインを実行し、フェーズ境界で MPI バリアにより同期する。

### C FFI ブリッジ

`AioriVTable` を通じて外部 C バックエンドをRust の `Aiori` トレイト実装として利用できる。`ior_register_backend()` でランタイム登録が可能。

## 出力フォーマット

### テキスト出力 (デフォルト)

C IOR 互換のテーブル形式で結果を表示する。

### JSON 出力

`--json` フラグで stdout に、`--json-file <PATH>` でファイルに出力する。C IOR の `summaryFormat=JSON` と互換性のある構造を持つ。

詳細は [docs/usage.md](docs/usage.md) を参照。

## リファレンス

- [C IOR/mdtest](https://github.com/hpc/ior) — 元の C 実装 (`external/ior/` にサブモジュールとして含まれる)

## ライセンス

本プロジェクトのライセンスについては、リポジトリのライセンスファイルを参照してください。
