# Usage Guide

## 目次

- [ior-bench](#ior-bench)
  - [基本的な使い方](#基本的な使い方)
  - [コマンドラインオプション](#コマンドラインオプション)
  - [使用例](#使用例)
- [mdtest-bench](#mdtest-bench)
  - [基本的な使い方](#基本的な使い方-1)
  - [コマンドラインオプション](#コマンドラインオプション-1)
  - [使用例](#使用例-1)
- [JSON 出力](#json-出力)
  - [ior-bench JSON 構造](#ior-bench-json-構造)
  - [mdtest-bench JSON 構造](#mdtest-bench-json-構造)

---

## ior-bench

MPI 並列 I/O 性能ベンチマーク。C IOR と同等の機能を Rust で実装。

### 基本的な使い方

```bash
mpiexec -n <NPROCS> target/release/ior-bench [OPTIONS]
```

`-w` / `-r` のいずれも指定しない場合、Write と Read の両方が実行される。

### コマンドラインオプション

#### I/O 設定

| フラグ | ロング形式 | デフォルト | 説明 |
|--------|-----------|-----------|------|
| `-a` | `--api` | `POSIX` | I/O バックエンド API |
| `-b` | `--block-size` | `1m` | タスクあたりのブロックサイズ (k/m/g/t サフィックス対応) |
| `-t` | `--transfer-size` | `256k` | I/O 操作あたりの転送サイズ |
| `-s` | `--segment-count` | `1` | セグメント数 |
| `-o` | `--test-file` | `testFile` | テストファイルパス |
| `-q` | `--queue-depth` | `1` | 非同期 I/O キュー深度 (1 = 同期) |
| | `--direct-io` | `false` | O_DIRECT 使用 (OS キャッシュバイパス) |

#### テスト制御

| フラグ | ロング形式 | デフォルト | 説明 |
|--------|-----------|-----------|------|
| `-w` | `--write-file` | `false` | Write フェーズを実行 |
| `-r` | `--read-file` | `false` | Read フェーズを実行 |
| `-F` | `--file-per-proc` | `false` | プロセスごとに個別ファイル |
| `-z` | `--random-offset` | `false` | ランダムアクセスオフセット |
| `-i` | `--repetitions` | `1` | 繰り返し回数 |
| `-d` | `--inter-test-delay` | `0` | 繰り返し間の遅延 (秒) |
| `-D` | `--deadline` | `0` | Stonewalling デッドライン (秒, 0=無効) |
| `-T` | `--max-time-duration` | `0` | テストあたりの最大時間 (分, 0=無制限) |

#### データ整合性

| フラグ | ロング形式 | 説明 |
|--------|-----------|------|
| `-W` | `--check-write` | Write 後のデータ検証 |
| `-R` | `--check-read` | Read 後のデータ検証 |
| `-e` | `--fsync` | Write フェーズ後に fsync |
| `-Y` | `--fsync-per-write` | 各 Write 後に fsync |

#### MPI / タスク制御

| フラグ | ロング形式 | デフォルト | 説明 |
|--------|-----------|-----------|------|
| `-N` | `--num-tasks` | `-1` | MPI タスク数 (-1 = 全タスク使用) |
| `-C` | `--reorder-tasks` | `false` | Read 時のタスク再配置 (決定的シフト) |
| `-Z` | `--reorder-tasks-random` | `false` | Read 時のランダムタスク再配置 |
| `-g` | `--intra-test-barriers` | `false` | テスト内バリアを有効化 |

#### 出力制御

| フラグ | ロング形式 | デフォルト | 説明 |
|--------|-----------|-----------|------|
| `-v` | `--verbose` | `0` | 詳細度 (複数指定で増加) |
| `-k` | `--keep-file` | `false` | テスト後にファイルを残す |
| | `--json` | `false` | JSON を stdout に出力 (テキスト出力を抑制) |
| | `--json-file` | なし | JSON をファイルに出力 (テキスト出力は維持) |

### 使用例

```bash
# 基本的な Write + Read テスト
mpiexec -n 4 ior-bench -w -r

# 大きなブロックサイズ、Direct I/O
mpiexec -n 8 ior-bench -w -r -b 1g -t 1m --direct-io

# File-per-process モード、3回繰り返し
mpiexec -n 16 ior-bench -w -r -F -i 3

# 非同期 I/O (キュー深度 8)
mpiexec -n 4 ior-bench -w -r -q 8 -b 4m -t 256k

# Stonewalling (30秒以内)
mpiexec -n 4 ior-bench -w -r -D 30

# JSON 出力をパイプで jq に渡す
mpiexec -n 1 ior-bench -w -r --json | jq '.summary'

# JSON をファイルに保存 (テキストも表示)
mpiexec -n 4 ior-bench -w -r --json-file results.json
```

---

## mdtest-bench

MPI 並列メタデータ性能ベンチマーク。C mdtest と同等の機能を Rust で実装。

### 基本的な使い方

```bash
mpiexec -n <NPROCS> target/release/mdtest-bench [OPTIONS]
```

`-C`, `-T`, `-E`, `-r` のいずれも指定しない場合、全フェーズ (create, stat, read, remove) が実行される。
`-D`, `-F` のいずれも指定しない場合、ディレクトリとファイルの両方がテストされる。

### コマンドラインオプション

#### テスト構成

| フラグ | ロング形式 | デフォルト | 説明 |
|--------|-----------|-----------|------|
| `-a` | `--api` | `POSIX` | I/O バックエンド API |
| `-d` | `--test-dir` | `./out` | テストディレクトリパス |
| `-n` | `--items` | `0` | プロセスあたりの総アイテム数 |
| `-I` | `--items-per-dir` | `0` | ディレクトリあたりのアイテム数 |
| `-b` | `--branch-factor` | `1` | ディレクトリ階層の分岐係数 |
| `-z` | `--depth` | `0` | ディレクトリツリーの深さ |
| `-i` | `--iterations` | `1` | イテレーション回数 |

#### フェーズ制御

| フラグ | ロング形式 | 説明 |
|--------|-----------|------|
| `-C` | `--create-only` | Create のみ |
| `-T` | `--stat-only` | Stat のみ |
| `-E` | `--read-only` | Read のみ |
| `-r` | `--remove-only` | Remove のみ |
| `-D` | `--dirs-only` | ディレクトリのみ |
| `-F` | `--files-only` | ファイルのみ |

#### データ設定

| フラグ | ロング形式 | デフォルト | 説明 |
|--------|-----------|-----------|------|
| `-w` | `--write-bytes` | `0` | ファイルあたりの書き込みバイト数 |
| `-e` | `--read-bytes` | `0` | ファイルあたりの読み込みバイト数 |
| `-y` | `--sync-file` | `false` | 書き込み後に fsync |
| `-k` | `--make-node` | `false` | mknod でファイル作成 |

#### アクセスパターン

| フラグ | ロング形式 | デフォルト | 説明 |
|--------|-----------|-----------|------|
| `-u` | `--unique-dir-per-task` | `false` | タスクごとに固有ディレクトリ |
| `-S` | `--shared-file` | `false` | 共有ファイル |
| `-L` | `--leaf-only` | `false` | リーフノードのみにアイテム作成 |
| `-N` | `--nstride` | `0` | ネイバーストライド |
| `-R` | `--random` | `false` | ランダム stat アクセス順 |
| | `--rename-dirs` | `false` | ディレクトリリネームテスト |

#### タスクスケーリング

| フラグ | ロング形式 | デフォルト | 説明 |
|--------|-----------|-----------|------|
| `-f` | `--first` | `0` | 最初のタスク数 (0 = MPI size) |
| `-l` | `--last` | `0` | 最後のタスク数 (0 = MPI size) |
| `-s` | `--stride` | `1` | タスク数のストライド |

#### 出力制御

| フラグ | ロング形式 | デフォルト | 説明 |
|--------|-----------|-----------|------|
| `-v` | `--verbose` | `0` | 詳細度 |
| `-Z` | `--print-time` | `false` | レートの代わりに時間を表示 |
| `-B` | `--no-barriers` | `false` | フェーズ間バリアを無効化 |
| `-W` | `--stonewall-timer` | `0` | Stonewall タイマー (秒) |
| | `--json` | `false` | JSON を stdout に出力 (テキスト出力を抑制) |
| | `--json-file` | なし | JSON をファイルに出力 (テキスト出力は維持) |

### 使用例

```bash
# ファイルメタデータテスト (100 ファイル/プロセス)
mpiexec -n 4 mdtest-bench -n 100 -F

# ディレクトリメタデータテスト
mpiexec -n 4 mdtest-bench -n 100 -D

# 階層ディレクトリ構造でテスト
mpiexec -n 4 mdtest-bench -n 1000 -z 3 -b 2

# Create のみ、3 回繰り返し
mpiexec -n 8 mdtest-bench -n 500 -F -C -i 3

# ファイルにデータを書き込む
mpiexec -n 4 mdtest-bench -n 100 -F -w 4096 -e 4096

# タスクスケーリング (1, 2, 4, 8 プロセス)
mpiexec -n 8 mdtest-bench -n 100 -F -f 1 -l 8 -s 2

# JSON 出力
mpiexec -n 1 mdtest-bench -n 100 -F --json | jq '.summary'
```

---

## JSON 出力

両ベンチマークとも `--json` と `--json-file` フラグで JSON 出力をサポートする。

| フラグ | 動作 |
|--------|------|
| `--json` | JSON を stdout に出力。テキスト出力は抑制される。 |
| `--json-file <PATH>` | JSON を指定パスに出力。テキスト出力は通常通り表示される。 |
| 両方指定 | stdout と ファイルの両方に JSON を出力。テキスト出力は抑制される。 |

### ior-bench JSON 構造

```json
{
  "version": "0.1.0",
  "began": "Wed Feb 19 12:00:00 2026",
  "command_line": "ior-bench -w -r --json",
  "machine": "hostname Linux 5.15.0-164-generic",
  "tests": [
    {
      "TestID": 0,
      "StartTime": "Wed Feb 19 12:00:00 2026",
      "Parameters": {
        "api": "POSIX",
        "blockSize": 1048576,
        "transferSize": 262144,
        "segmentCount": 1,
        "numTasks": 1,
        "repetitions": 1,
        "filePerProc": false,
        "directIO": false,
        "queueDepth": 1,
        "testFileName": "testFile",
        "deadlineForStonewalling": 0,
        "keepFile": false,
        "fsync": false,
        "randomOffset": false
      },
      "Options": {
        "api": "POSIX",
        "apiVersion": "",
        "testFileName": "testFile",
        "access": "single-shared-file",
        "ordering": "sequential",
        "repetitions": 1,
        "xfersize": "256.00 KiB",
        "blocksize": "1.00 MiB",
        "aggregate filesize": "1.00 MiB"
      },
      "Results": [
        {
          "access": "write",
          "bwMiB": 512.34,
          "blockKiB": 1024.0,
          "xferKiB": 256.0,
          "iops": 2048.0,
          "latency": 0.000244,
          "openTime": 0.001,
          "wrRdTime": 0.002,
          "closeTime": 0.0001,
          "totalTime": 0.003,
          "numTasks": 1,
          "iter": 0
        },
        {
          "access": "read",
          "bwMiB": 1024.56,
          "...": "..."
        }
      ]
    }
  ],
  "summary": [
    {
      "operation": "write",
      "bwMaxMIB": 512.34,
      "bwMinMIB": 512.34,
      "bwMeanMIB": 512.34,
      "bwStdMIB": 0.0,
      "OPsMax": 2048.0,
      "OPsMin": 2048.0,
      "OPsMean": 2048.0,
      "OPsStdDev": 0.0,
      "MeanTime": 0.003
    }
  ],
  "finished": "Wed Feb 19 12:00:01 2026"
}
```

### mdtest-bench JSON 構造

```json
{
  "version": "0.1.0",
  "began": "Wed Feb 19 12:00:00 2026",
  "command_line": "mdtest-bench -n 100 -F --json",
  "machine": "hostname Linux 5.15.0-164-generic",
  "tests": [
    {
      "numTasks": 1,
      "parameters": {
        "api": "POSIX",
        "testDir": "./out",
        "branchFactor": 1,
        "depth": 0,
        "items": 100,
        "itemsPerDir": 100,
        "numDirsInTree": 1,
        "uniqueDirPerTask": false,
        "dirsOnly": false,
        "filesOnly": true,
        "createOnly": true,
        "statOnly": true,
        "readOnly": true,
        "removeOnly": true,
        "writeBytes": 0,
        "readBytes": 0,
        "iterations": 1
      },
      "iterations": [
        {
          "iteration": 0,
          "phases": [
            {
              "phase": "File creation",
              "rate": 15234.5,
              "time": 0.00656,
              "items": 100
            },
            {
              "phase": "File stat",
              "rate": 98765.4,
              "time": 0.00101,
              "items": 100
            },
            {
              "phase": "File read",
              "rate": 45678.9,
              "time": 0.00219,
              "items": 100
            },
            {
              "phase": "File removal",
              "rate": 23456.7,
              "time": 0.00426,
              "items": 100
            }
          ]
        }
      ]
    }
  ],
  "summary": [
    {
      "phase": "File creation",
      "max": 15234.5,
      "min": 15234.5,
      "mean": 15234.5,
      "stddev": 0.0
    },
    {
      "phase": "File stat",
      "max": 98765.4,
      "min": 98765.4,
      "mean": 98765.4,
      "stddev": 0.0
    }
  ],
  "finished": "Wed Feb 19 12:00:01 2026"
}
```

### JSON 出力の後処理例

```bash
# jq でサマリを抽出
mpiexec -n 4 ior-bench -w -r --json | jq '.summary'

# Write の帯域幅のみ取得
mpiexec -n 4 ior-bench -w --json | jq '.summary[] | select(.operation=="write") | .bwMeanMIB'

# mdtest のフェーズ別レート一覧
mpiexec -n 4 mdtest-bench -n 100 -F --json | jq '.summary[] | {phase, mean}'

# ファイルに保存して後から解析
mpiexec -n 4 ior-bench -w -r --json-file results.json
jq . results.json
```
