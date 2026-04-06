# Ztract Design Specification

**Date:** 2026-04-06
**Status:** Approved
**License:** Apache 2.0

## Mission

Extract mainframe EBCDIC binary files on a local PC using real COBOL copybooks — zero MIPS spent on the mainframe. Bidirectional connectivity enables mainframe-to-mainframe flows through Ztract.

## Target Users

Mainframe developers and DBAs who want to offload EBCDIC processing off the mainframe to save MIPS.

---

## 1. Architecture Overview

Ztract is a Python 3.10+ CLI tool that orchestrates EBCDIC data extraction, transformation, diff, and mock generation. All COBOL binary decoding/encoding is delegated to a Java engine (Cobrix cobol-parser) via subprocess. Python handles connectivity, orchestration, output writing, and observability.

```
                         ztract (Python)
                              |
         +----------+---------+---------+----------+
         |          |         |         |          |
     Connectors  Pipeline  Writers   Diff     Generator
     (FTP/SFTP/  (YAML     (CSV/    (daff/    (Faker/
      Zowe/      orchestr.  Parquet/  multidiff) Java
      local)     fan-out)   DB/JSONL/            encode)
         |          |       EBCDIC)
         +-----+----+
               |
         Java Engine Bridge
         (subprocess stdin/stdout)
               |
         ztract-engine.jar
         (Cobrix cobol-parser)
```

### Key Design Principles

- Java engine is a black box: Python never touches EBCDIC bytes, COMP-3 packing, REDEFINES, or RDW/BDW parsing
- Streaming throughout: no component ever loads a full file into memory
- Clean subprocess boundary: JSON Lines over stdin/stdout between Python and Java
- Fan-out architecture: multiple output targets written concurrently from one read pass
- Enterprise observability: structured logs, immutable audit trail, reject files with full context

---

## 2. Java Engine (`ztract-engine.jar`)

### 2.1 Source and Dependencies

- Module: `za.co.absa.cobrix:cobol-parser_2.12:2.10.1` (Apache 2.0)
- Zero Spark dependency — cobol-parser is standalone
- Built as fat JAR via Maven shade plugin, bundling only cobol-parser
- Ships inside the Python package at `ztract/engine/ztract-engine.jar`
- Requires JRE 11+ on PATH

### 2.2 Java Wrapper Source

Separate Maven project at `engine-java/` in the repo root. Thin CLI wrapper around cobol-parser exposing four modes.

### 2.3 Operating Modes

**Decode mode** (normal extract):
```
java -Xmx512m -Dfile.encoding=UTF-8 -jar ztract-engine.jar \
  --copybook CUST.cpy --input CUST.DAT \
  --recfm FB --lrecl 500 --codepage cp277 \
  --encoding ebcdic
```
- Reads raw binary file from disk via file path
- Writes JSON Lines to stdout (one JSON object per record)
- Errors to stderr
- Exit code: 0 success, 1 failure

**Encode mode** (reverse — write EBCDIC binary):
```
java -Xmx512m -Dfile.encoding=UTF-8 -jar ztract-engine.jar \
  --copybook CUST.cpy --output CUST_MOCK.DAT \
  --recfm FB --lrecl 500 --codepage cp277 \
  --encoding ebcdic --mode encode
```
- Reads JSON Lines from stdin
- Writes EBCDIC binary file to disk at `--output` path
- Used by mock generator and EBCDIC write-back

**Schema mode** (copybook introspection):
```
java -jar ztract-engine.jar \
  --copybook CUST.cpy --schema-only \
  --recfm FB --lrecl 500
```
- Dumps field definitions and layout metadata as JSON to stdout, then exits
- Accepts `--recfm` and `--lrecl` to validate copybook record length matches file LRECL
- Mismatch produces warning (not fatal): "WARNING: copybook record length 496 != lrecl 500. 4 bytes unaccounted. Proceeding anyway."

Schema output format:
```json
{
  "copybook": "CUSTMAST.cpy",
  "record_length": 500,
  "record_format": "FB",
  "fields": [
    {
      "name": "CUST-ID",
      "level": "05",
      "type": "NUMERIC",
      "pic": "9(10)",
      "usage": "DISPLAY",
      "offset": 0,
      "size": 10,
      "scale": 0,
      "signed": false,
      "occurs": null,
      "redefines": null,
      "redefines_group": null
    }
  ],
  "redefines_groups": [
    {
      "group": "STATIC-DETAILS",
      "variants": ["CONTACTS"]
    }
  ]
}
```

**Validate mode** (pre-flight check):
```
java -jar ztract-engine.jar \
  --copybook CUST.cpy --input CUST.DAT \
  --recfm FB --lrecl 500 --codepage cp277 \
  --mode validate --sample 1000
```
- Reads first N records (default 1000), attempts decode
- Reports: records decoded, records with warnings, records with errors, field-level stats (null %, min/max for numerics, sample values for PIC X)
- Exits without writing any output

### 2.4 CLI Arguments Summary

| Flag | Description |
|------|-------------|
| `--copybook` | Path to .cpy file |
| `--input` | Path to raw binary/text file (decode/validate) |
| `--output` | Path for output binary file (encode) |
| `--recfm` | F, FB, V, VB, FBA, VBA |
| `--lrecl` | Logical record length (required for F/FB) |
| `--codepage` | cp037, cp277, cp273, cp1047, etc. |
| `--encoding` | ebcdic (default), ascii |
| `--mode` | decode (default), encode, validate |
| `--schema-only` | Dump schema as JSON and exit |
| `--sample` | Number of records to validate (validate mode) |

### 2.5 Python Bridge (`engine/bridge.py`)

```python
class ZtractBridge:
    def __init__(self, jar_path, jvm_args):
        ...
    
    def check_jre(self) -> str:
        """Validate JRE 11+ on PATH, return version string.
        Parses 'java -version' stderr output.
        Raises with download link if missing or < 11."""
    
    def get_schema(self, copybook, recfm=None, lrecl=None) -> dict:
        """Call --schema-only, return parsed schema dict."""
    
    def validate(self, copybook, input_path, recfm, lrecl, 
                 codepage, sample=1000) -> ValidationReport:
        """Run validate mode, return stats. Never writes output."""
    
    def decode(self, copybook, input_path, recfm, lrecl, 
               codepage, encoding="ebcdic") -> Iterator[dict]:
        """Yield parsed record dicts from Java stdout. Streaming."""
    
    def encode(self, copybook, output_path, recfm, lrecl, 
               codepage, records: Iterator[dict]) -> int:
        """Stream records as JSON Lines to Java stdin. 
        Return record count written."""
    
    def shutdown(self):
        """Graceful subprocess cleanup. SIGTERM, wait 5s, SIGKILL."""
```

**JVM launch flags:**
- `-Xmx512m` (configurable via `.ztract/config.yaml` `engine.jvm_max_heap`)
- `-Dfile.encoding=UTF-8` (critical for Norwegian characters on all platforms)
- `-Dstdout.encoding=UTF-8` (Java 17+ — prevents Windows CP1252 corruption)
- Additional flags via `engine.jvm_args` config list

**Stderr classification:**
- FATAL (abort): "Exception in thread", "OutOfMemoryError", "ERROR:" prefix
- WARNING (log, continue): "WARN:" prefix, invalid sign nibble, truncated record
- IGNORE (discard): JVM startup messages, GC output, classpath warnings

**Graceful shutdown:**
- On SIGINT/SIGTERM: send SIGTERM to Java subprocess
- Wait up to 5 seconds for clean exit
- If not exited: SIGKILL
- Write partial audit entry with status ABORTED
- Close reject file cleanly
- Print records processed before abort

**JRE version detection:**
- Parse `java -version` output for major version number
- If < 11: clear error with Adoptium download link
- Store detected JRE version in audit log

---

## 3. Connectors

Four transport backends providing bidirectional file transfer. Each connector's job: get a file to/from local disk so the Java engine can process it.

### 3.1 Base Interface (`connectors/base.py`)

```python
class Connector(ABC):
    def download(self, source, local_path) -> Path:
        """Download file to local path. Return local path."""
    
    def upload(self, local_path, destination, 
               site_commands=None) -> None:
        """Upload local file to destination."""
    
    def list_datasets(self, pattern) -> List[str]:
        """List matching datasets (z/OS FTP/Zowe). Optional."""
    
    def exists(self, source) -> bool:
        """Check source exists before download. 
        Avoids 30-second FTP timeout on missing file."""
    
    def close(self) -> None:
        """Release connections."""
```

### 3.2 LocalConnector (`connectors/local.py`)

- `download`: no-op, returns path as-is
- `upload`: file copy
- Validates: file exists, readable, non-zero size

### 3.3 FTPConnector (`connectors/ftp.py`)

- Uses `ftplib.FTP`
- Transfer mode: binary (default, TYPE I, `retrbinary()`) or text (TYPE A, `retrlines()` — for pre-converted ASCII files)
- FTP mode: passive (default) or active, configurable in connection profile
- Timeout and retry: configurable, default 3 retries with exponential backoff
- Connection reuse via `step_context` connection pool

**Upload with SITE commands:**

z/OS FTP requires SITE commands in strict order before STOR:

1. `SITE RECFM=FB`
2. `SITE LRECL=500`
3. `SITE BLKSIZE=27920`
4. `SITE CYLINDERS` (or `TRACKS`)
5. `SITE PRIMARY=5`
6. `SITE SECONDARY=2`
7. `SITE MGMTCLAS=...` (if specified)
8. `SITE STORCLAS=...` (if specified)
9. `SITE DATACLAS=...` (if specified)
10. `SITE UNIT=...` (if specified)
11. `SITE VOLSER=...` (if specified)
12. `STOR BEL.CUST.OUTBOUND`

`ftp.py` enforces this order regardless of YAML key order.

### 3.4 SFTPConnector (`connectors/sftp.py`)

- Uses `paramiko.SFTPClient`
- Binary transfer
- SSH key auth or password, configurable in connection profile
- SFTP does NOT support SITE commands natively. z/OS SFTP dataset allocation is SMS-managed. Ztract SFTP connector uploads bytes only. Users needing explicit allocation control should use FTP.

### 3.5 ZoweConnector (`connectors/zowe.py`)

- Shells out to Zowe CLI
- Download: `zowe zos-files download data-set "DATASET" --binary`
- Upload: `zowe zos-files upload file-to-data-set`
- Uses Zowe CLI profile name from YAML config or `--zowe-profile` flag
- Version detection: parse `zowe --version`, use correct command syntax per version
- Raise clear error if Zowe CLI not on PATH or version < 2 (v1 is EOL)
- Store detected Zowe version in audit log

### 3.6 Dataset Format Module (`connectors/dataset_format.py`)

- Enum for record formats: F, FB, V, VB, FBA, VBA
- Validation: LRECL required for F/FB, optional for V/VB
- ASA byte stripping for FBA/VBA: done in Python after download, before passing to Java engine
  - For FB: strip first byte of every LRECL block
  - For VB: strip first byte of every RDW payload
  - Stripped file written to temp location, original preserved
  - Temp file cleaned up after job completes
- ASA byte constants: SINGLE_SPACE (`x'40'`), DOUBLE_SPACE (`x'F0'`), NEW_PAGE (`x'F1'`), OVERPRINT (`x'4E'`)

### 3.7 Temp File Management

- Connectors download to: `./.ztract_tmp/<job_id>/<step_name>/`
- Cleaned up after each step completes
- On abort: cleanup in `bridge.shutdown()`
- On crash: `.ztract_tmp` left behind with warning: "Temp files from aborted job in .ztract_tmp/. Run: ztract cleanup to remove"
- `.ztract_tmp` added to `.gitignore` by `ztract init`

### 3.8 Connection Pooling (`pipeline/step_context.py`)

- `_connection_pool: Dict[str, Connector]` keyed by connection URI (`ftp://user@host:port`)
- `get_connector()`: returns existing connection if alive (FTP NOOP ping), creates new if not
- All connections closed in `step_context.close()` at pipeline end
- Benefit: avoids re-authentication (FTP login on z/OS: 2-5 seconds per connection)

---

## 4. Writers

All writers implement a common interface and run as independent threads in fan-out.

### 4.1 Base Interface (`writers/base.py`)

```python
class Writer(ABC):
    def open(self, schema: dict) -> None:
        """Initialize output. Create table/file headers."""
    
    def write_batch(self, records: List[dict]) -> int:
        """Write batch, return count written."""
    
    def close(self) -> WriterStats:
        """Flush, close, return stats."""
    
    @property
    def name(self) -> str:
        """Display name for progress."""
    
    batch_size: int = 1000  # configurable per writer
```

### 4.2 Column Name Sanitization

COBOL field names use hyphens (`CUST-ID`). SQL, Parquet, and CSV headers use underscores. All writers auto-convert: `CUST-ID` -> `CUST_ID` (hyphen to underscore). Applied consistently across CSVWriter (header row), ParquetWriter (column names), and DatabaseWriter (column names). Documented in README for DBAs.

### 4.3 Null / Low-Value Representation

EBCDIC low-values (`x'00'`) decoded as null by Java engine.

| Writer | Null representation |
|--------|-------------------|
| CSVWriter | Empty field (default), configurable via `null_value` YAML key |
| JSONLWriter | JSON `null` |
| ParquetWriter | `pa.null()` |
| DatabaseWriter | SQL `NULL` |

### 4.4 OCCURS Field Handling

COBOL `OCCURS` produces array fields in JSON Lines:
```json
{"ITEM": [{"CODE":"A"},{"CODE":"B"},{"CODE":"C"}]}
```

| Writer | Strategy |
|--------|----------|
| JSONLWriter | Nested array as-is (natural JSON) |
| ParquetWriter | `pa.list_()` type |
| CSVWriter | Flatten: `ITEM_1_CODE`, `ITEM_2_CODE`, etc. |
| DatabaseWriter | Flatten same as CSV (Phase 1). PostgreSQL array type in Phase 2. |

### 4.5 CSVWriter (`writers/csv.py`)

- Python `csv` module
- Delimiter: configurable (default comma, common override: pipe `|`)
- Header row from schema field names (sanitized)
- Quoting: fields containing delimiter or newline
- Encoding: UTF-8 with BOM option (`utf-8-sig`) for Excel compatibility
- Streaming: flush every batch

### 4.6 JSONLWriter (`writers/jsonl.py`)

- One JSON object per line via `json.dumps`
- UTF-8, `ensure_ascii=False` (preserves ae o aa directly in output)
- Flush every batch

### 4.7 ParquetWriter (`writers/parquet.py`)

- `pyarrow.ParquetWriter` with streaming row groups
- Schema auto-derived from copybook via `--schema-only`
- Row group size: configurable (default 10,000 rows)
- Compression: snappy (default), gzip, zstd, none — configurable

Type mapping from COBOL to Arrow:

| COBOL | Arrow type |
|-------|-----------|
| PIC X(n) | `pa.string()` |
| PIC 9(n) | `pa.int32()` or `pa.int64()` (threshold: 9 digits) |
| PIC 9(n)V9(m) | `pa.decimal128(n+m, m)` |
| PIC S9(n) | `pa.int32()` or `pa.int64()` |
| COMP / COMP-4 | `pa.int32()` or `pa.int64()` |
| COMP-3 | `pa.decimal128()` |
| COMP-1 | `pa.float32()` |
| COMP-2 | `pa.float64()` |

### 4.8 DatabaseWriter (`writers/database.py`)

- SQLAlchemy engine + `executemany` batch inserts
- Connection string from YAML connection profile or CLI flag
- Auto-creates table from copybook schema if not exists
- Modes: `append` (default), `truncate` (TRUNCATE then INSERT), `upsert` (Phase 2)
- Batch size: configurable (default 1000)
- On constraint violation: record goes to reject file, batch continues
- Phase 1 drivers: psycopg2 (PostgreSQL), mysql-connector-python (MySQL), pyodbc (SQL Server)
- Others via SQLAlchemy in Phase 2

### 4.9 EBCDICWriter (`writers/ebcdic.py`)

- Reverse path: accepts record dicts, streams as JSON Lines to Java encode mode via `bridge.encode()`
- Python never touches EBCDIC bytes
- Output can be local file or temp file for subsequent FTP/SFTP upload
- Used by: mock generator output, mainframe-to-mainframe flows, any pipeline step writing back to z/OS

---

## 5. Pipeline Orchestration

### 5.1 Orchestrator (`pipeline/orchestrator.py`)

- Parses validated YAML job config (from `config/loader.py`)
- Iterates steps sequentially
- For each step: resolves `$ref` inputs from `step_context`, instantiates connector + writers, launches fan-out
- `continue_on_error: true` per step (default: stop on first failure)
- `--step <name>`: runs only the named step (auto-resolves dependencies producing `$ref` outputs needed by target step)
- `--dry-run`: validates config, checks connectivity (FTP login, DB connect, JRE present), prints execution plan, exits without processing data
- Returns pipeline-level exit code: 0 (all success), 1 (fatal), 2 (partial — rejects exist)

### 5.2 Fan-out (`pipeline/fanout.py`)

```
Java stdout
    |
Reader thread (parses JSON Lines)
    |
Per-writer queues (bounded, default 5000)
   / |  \
Writer-1  Writer-2  Writer-3
(CSV)     (Parquet)  (FTP back)
```

- One reader thread reads JSON Lines from Java stdout
- One bounded queue per writer (NOT one shared queue — avoids slow writer blocking fast writer)
- All writer threads receive every record (broadcast)
- Backpressure: if any writer's queue is full, reader blocks
- On writer failure: writer logs error, marks itself failed, drains queue (other writers continue)
- On reader failure (Java crash): poison pill sentinel to all writers, flush and close
- Completion: reader sends poison pill when Java stdout closes

### 5.3 Step Context (`pipeline/step_context.py`)

- `expose_as` outputs registered as `{name: {type: path}}` dict
- `$ref:step_name.output_type` resolved by lookup in this dict
- Connection pool (see Section 3.8)
- Aggregated reject count across all steps
- Per-step timing (start, end, elapsed)
- Temp file registry: tracked here, cleaned up in `close()`

---

## 6. Diff

### 6.1 Differ (`diff/differ.py`)

Pipeline:
1. Decode BEFORE file via `bridge.decode()` to temp JSONL
2. Decode AFTER file via `bridge.decode()` to temp JSONL
3. Load both into daff as tabular data
4. Run daff diff with `--key` fields (from `--key` CLI flag or YAML `key_fields`)
5. Format output per target
6. Clean up temp files

Output formats:
- `console`: colorized table via `rich` — added rows green, deleted red, changed yellow with field-level highlight
- `csv`: daff's standard CSV diff format
- `json`: structured diff with `added`, `deleted`, `changed` arrays; each changed record shows `before`/`after` values per field

Options:
- `--show-unchanged false` (default): only show differences
- `memory_threshold` (default 500,000 records): if exceeded, stream to temp SQLite and diff via SQL instead of in-memory daff

**Edge case — no key with different record counts:**
If `--key` not specified AND record counts differ, warn: "Files have different record counts (before: 1000000, after: 1000003). Using ordinal matching — extra records shown as ADDED/DELETED." Never silently misalign records.

### 6.2 REDEFINES Handler (`diff/redefines.py`)

- For fields in a REDEFINES group (identified via schema `redefines_groups`), extract raw bytes at the REDEFINES offset range from both files
- Hex-diff using `multidiff`
- Output: hex dump showing differing bytes, annotated with both REDEFINES variant interpretations where possible
- Triggered automatically when daff detects changes in REDEFINES group fields

---

## 7. Mock Data Generator

### 7.1 Generator (`generate/generator.py`)

Flow:
1. Call `bridge.get_schema()` to get field definitions
2. For each field, select a generator from `field_patterns.py` based on field name
3. Generate N records as dicts (streaming — one at a time, never hold all in memory)
4. Stream dicts as JSON Lines to `bridge.encode()` (Java writes EBCDIC binary)

Configuration:
- `--seed`: sets `Faker.seed()` and `random.seed()` for full reproducibility
- Faker locale from codepage mapping: cp277 -> `nb_NO`, cp273 -> `de_DE`, cp037 -> `en_US`
- Progress bar: records generated, target count, throughput

OCCURS field generation:
- `OCCURS n TIMES`: generate exactly n array elements
- `OCCURS 0 TO n DEPENDING ON`: generate random count between 1 and n (overridable via `--occurs-max` flag)
- Each element generated per its own field patterns

### 7.2 Field Patterns (`generate/field_patterns.py`)

Dict mapping regex patterns to generator functions. Patterns match against COBOL field names (case-insensitive, partial match).

**PIC X fields:**

| Pattern | Generator |
|---------|-----------|
| `*NAME*`, `*NAVN*` | `faker.name()` (locale-aware) |
| `*ADDR*`, `*ADRESSE*` | `faker.address()` |
| `*CITY*`, `*BY*` | `faker.city()` |
| `*PHONE*`, `*TELEFON*` | `faker.phone_number()` |
| `*EMAIL*` | `faker.email()` |
| `*ZIP*`, `*POST*` | `faker.postcode()` |
| `*COUNTRY*`, `*LAND*` | `faker.country()` |
| `*DESC*`, `*TEXT*` | `faker.sentence()` |
| `*CODE*`, `*KODE*` | random alphanumeric |
| (fallback) | random chars padded to PIC size |

**PIC 9 / COMP-3 fields:**

| Pattern | Generator |
|---------|-----------|
| `*AMT*`, `*AMOUNT*`, `*BELOP*` | realistic monetary amounts (100.00-999999.99) |
| `*DATE*`, `*DATO*` | valid YYYYMMDD or DDMMYY |
| `*ID*`, `*NR*`, `*NUM*` | sequential or random integer |
| (fallback) | random digits to PIC size |

Values truncated/padded to fit PIC size. Strings padded to exact PIC X length, numbers capped to PIC 9 digit count. Norwegian/Danish field name patterns included by default.

---

## 8. CLI

Click-based CLI with 8 commands under the `ztract` group.

### 8.1 Root Group (`cli/root.py`)

```
ztract [--version] [--debug] [--quiet] COMMAND
```

- `--debug`: log level DEBUG, show Java stderr in console
- `--quiet`: suppress progress bars, print only final summary line

### 8.2 Commands

**`ztract convert`** (`cli/convert.py`):
```
ztract convert \
  --copybook CUST.cpy \
  --input CUST.DAT \
  --output customers.csv \
  --recfm FB --lrecl 500 \
  --codepage cp277 \
  --format csv|jsonl|parquet \
  --delimiter "|" \
  --compression snappy|gzip|zstd|none
```
- `--format` inferred from `--output` extension if not specified
- `--input` can be local path or `ftp://`, `sftp://` URI (connector auto-selected)
- `--output` can also be a URI for write-back
- `--zowe-profile` + `--dataset` for Zowe input
- Multiple `--output` flags for fan-out
- If `--format` and `--output` extension conflict: `--format` wins with warning

**`ztract diff`** (`cli/diff.py`):
```
ztract diff \
  --copybook CUST.cpy \
  --before CUST_JAN.DAT --after CUST_FEB.DAT \
  --key CUST-ID \
  --format console|csv|json \
  --codepage cp277 --recfm FB --lrecl 500
```

**`ztract generate`** (`cli/generate.py`):
```
ztract generate \
  --copybook CUST.cpy \
  --records 100000 \
  --output CUST_MOCK.DAT \
  --codepage cp277 --recfm FB --lrecl 500 \
  --seed 42 --occurs-max 5
```

**`ztract run`** (`cli/run.py`):
```
ztract run job.yaml [--dry-run] [--step NAME] [--debug]
```

**`ztract validate`** (`cli/validate.py`):
```
ztract validate \
  --copybook CUST.cpy --input CUST.DAT \
  --recfm FB --lrecl 500 --codepage cp277 \
  --sample 1000
```
Pre-flight check: decodes sample records, reports stats, catches copybook/file mismatches in seconds.

**`ztract inspect`** (`cli/inspect.py`):
```
ztract inspect --copybook CUST.cpy
```
Displays copybook as formatted rich table:
```
+-----------+-------+--------+--------+----------+
| Field     | Level | PIC    | Offset | Size     |
+-----------+-------+--------+--------+----------+
| CUST-ID   | 05    | 9(10)  | 0      | 10       |
| CUST-NAME | 05    | X(50)  | 10     | 50       |
| CUST-AMT  | 05    | S9(9)  | 60     | 6 (COMP3)|
+-----------+-------+--------+--------+----------+
Total record length: 500 bytes
```
Uses `bridge.get_schema()`. Key differentiator — mainframe teams use this daily with no good existing tool.

**`ztract init`** (`cli/init.py`):
```
ztract init
```
Creates project scaffold:
```
./copybooks/        <- .cpy files
./jobs/             <- YAML job files
./output/           <- default output
./logs/             <- operational logs
./audit/            <- audit trail
./rejects/          <- reject files
./testdata/         <- generated mock data
./.ztract/
    config.yaml     <- machine-level config (gitignored)
./.gitignore        <- pre-configured for ztract
```

**`ztract status`** (`cli/status.py`):
```
ztract status [--job NAME]
```
Reads audit log and displays last N job runs as a rich table. Detailed history per job with `--job` flag. Reads from `audit/ztract_audit.log` — no extra database needed.

---

## 9. Configuration

### 9.1 YAML Job Files

Full pipeline definition with connection profiles, multiple steps, step references, and fan-out outputs.

**Structure:**
```yaml
version: "1.0"
job:
  name: customer-monthly-reconciliation
  description: Extract PROD vs TEST and diff

connections:
  prod_ftp: &prod_ftp
    type: ftp
    host: mf01.bank.com
    port: 21
    user: ${PROD_FTP_USER}
    password: ${PROD_FTP_PASS}
    transfer_mode: binary
    ftp_mode: passive

steps:
  - name: extract-prod
    action: convert
    input:
      connection: *prod_ftp
      dataset: BEL.CUST.MASTER
      record_format: FB
      lrecl: 500
      codepage: cp277
    copybook: ./copybooks/CUSTMAST.cpy
    transform:
      exclude_fillers: true
      null_if_low_values: true
    output:
      - type: csv
        path: ./output/prod_customers.csv
        delimiter: "|"
      - type: parquet
        path: ./output/prod_customers.parquet
    expose_as: prod_data

  - name: diff-envs
    action: diff
    input:
      before: $ref:prod_data.csv
      after: $ref:test_data.csv
    copybook: ./copybooks/CUSTMAST.cpy
    diff:
      key_fields: [CUST-ID, ACCT-NO]
      memory_threshold: 500000
    output:
      - type: console
        show_unchanged: false
      - type: csv
        path: ./output/diff_report.csv
```

**YAML-only features** (not available via CLI flags):
1. Connection profiles with YAML anchors (`&name` / `*name`)
2. Step references (`$ref:step_name.output_type`)
3. Multi-step pipelines with sequential execution
4. Notifications (on_failure, on_success) — email via SMTP
5. Environment variable interpolation (`${VAR_NAME}`)

**CLI vs YAML:** All CLI flags map to YAML keys. CLI = single step, quick use. YAML = multi-step pipelines, recurring jobs. CLI flags override YAML values when both specified.

**Running jobs:**
```
ztract run job.yaml
ztract run job.yaml --dry-run
ztract run job.yaml --step extract-prod
```

### 9.2 YAML Loader (`config/loader.py`)

- Parses YAML via `pyyaml`
- `${VAR_NAME}` interpolation from `os.environ`
- `.env` file auto-loaded (simple key=value parser, no extra dependency)
- `$ref:` strings stored as-is during parsing, resolved at runtime by `step_context`

### 9.3 Schema Validation (`config/schema.py`)

- Validates YAML structure against expected schema per action type
- Validates codepage values against `codepages.py` registry
- Validates record format values against `dataset_format.py` enum
- Validates connection profile completeness
- Clear error messages: "Step 'extract-prod': missing required field 'copybook'"

### 9.4 Global Config (`.ztract/config.yaml`)

Machine-level defaults (not committed to git):
- Default codepage, default recfm
- JVM heap size (`engine.jvm_max_heap: 512m`)
- Extra JVM args (`engine.jvm_args: []`)
- Log retention days
- SMTP settings for notifications

---

## 10. Observability

### 10.1 Structured Logging (`observability/logging.py`)

- Python `logging` module with JSON formatter
- Output: `./logs/ztract_YYYY-MM-DD.log` (JSON Lines)
- Levels: DEBUG, INFO (default), WARN, ERROR
- `--debug` flag enables DEBUG level
- Rotation: daily, 30-day retention (configurable), `TimedRotatingFileHandler`
- Console output: `rich` formatted (colored level, condensed) — separate from log file

Example log entry:
```json
{
  "timestamp": "2025-01-15T02:34:17.432Z",
  "level": "INFO",
  "job": "customer-monthly-reconciliation",
  "step": "extract-prod",
  "event": "records_processed",
  "records_read": 1000000,
  "records_written": 999998,
  "records_rejected": 2,
  "elapsed_sec": 47.3,
  "throughput_rps": 21141
}
```

### 10.2 Audit Trail (`observability/audit.py`)

- Append-only JSON Lines: `./audit/ztract_audit.log`
- One entry per job execution
- Cannot be disabled — hardcoded always-on
- Never rotated, never overwritten by Ztract
- Contains: audit_id (UUID), timestamps, user, machine, ztract version, JRE version, job file + SHA-256 hash, per-step stats, overall status, exit code

Example audit entry:
```json
{
  "audit_id": "a1b2c3d4-...",
  "timestamp_start": "2025-01-15T02:33:01.000Z",
  "timestamp_end": "2025-01-15T02:34:48.432Z",
  "user": "svcid",
  "machine": "WKSTN-RC01",
  "ztract_version": "1.0.0",
  "jre_version": "17.0.2",
  "job_file": "monthly-reconciliation.yaml",
  "job_file_hash": "sha256:ab12cd34...",
  "steps": [
    {
      "step": "extract-prod",
      "action": "convert",
      "source": "ftp://mf01/BEL.CUST.MASTER",
      "target": ["prod_customers.csv", "prod_customers.parquet"],
      "records_read": 1000000,
      "records_written": 999998,
      "records_rejected": 2,
      "reject_file": "extract-prod_20250115_rejects.jsonl",
      "status": "PARTIAL_SUCCESS"
    }
  ],
  "overall_status": "PARTIAL_SUCCESS",
  "exit_code": 2
}
```

### 10.3 Progress (`observability/progress.py`)

- `rich.progress` for TTY output: per-step bars with records/sec, ETA, elapsed
- Multiple bars stacked in pipeline mode
- Auto-suppressed when stdout is not a TTY
- `--quiet` suppresses progress, prints only final summary line
- Summary line always printed at job end (even in quiet mode)
- `tqdm` removed from dependencies — `rich` handles all progress display

### 10.4 Rejects (`observability/rejects.py`)

- JSON Lines format: `./rejects/<jobname>_<step>_<timestamp>_rejects.jsonl`
- `RejectHandler` instance passed to each writer — writers call `reject(record, reason)` on failure

Per-reject entry:
```json
{
  "record_num": 45231,
  "byte_offset": 22615000,
  "step": "extract-prod",
  "error_type": "DB_CONSTRAINT_VIOLATION",
  "error_msg": "duplicate key CUST-ID=12345",
  "target": "jdbc:postgresql://dwh/prod.customer",
  "decoded": {"CUST-ID":"12345","CUST-NAME":"Bjorn"},
  "raw_hex": "C2D1D6D9D540404040...",
  "timestamp": "2025-01-15T02:34:17.432Z"
}
```

**Job exit codes:**
- 0 = success, zero rejects
- 1 = fatal error, job aborted
- 2 = partial success, rejects exist

---

## 11. Codepages

### 11.1 Registry (`codepages.py`)

Central mapping of friendly aliases to canonical Cobrix codepage names.

| Codepage | Aliases |
|----------|---------|
| cp037 | `037`, `us`, `usa`, `canada`, `default` |
| cp277 | `277`, `norway`, `norwegian`, `danish`, `denmark`, `nordic` |
| cp273 | `273`, `germany`, `german`, `austria`, `switzerland` |
| cp875 | `875`, `greek`, `greece` |
| cp870 | `870`, `eastern_europe`, `poland`, `hungary`, `czech` |
| cp1047 | `1047`, `latin1`, `open_systems` |
| cp838 | `838`, `thailand`, `thai` |
| cp1025 | `1025`, `cyrillic`, `russian` |

- `resolve_codepage("norway")` returns `"cp277"`
- Unknown codepage raises clear error listing all supported values
- cp277 documented as primary/recommended in README

---

## 12. Packaging and Distribution

### 12.1 Python Package

- Build system: `setuptools` with `pyproject.toml`
- Package name: `ztract`
- Entry point: `ztract = ztract.cli.root:cli`
- Python requires: `>=3.10`
- JAR bundled via `package_data`: `ztract/engine/ztract-engine.jar`

**Dependencies:**

| Package | Purpose | License |
|---------|---------|---------|
| click | CLI framework | BSD-3 |
| pyyaml | YAML config parsing | MIT |
| pyarrow | Parquet output | Apache 2.0 |
| sqlalchemy | Database abstraction | MIT |
| psycopg2-binary | PostgreSQL driver | LGPL |
| pymysql | MySQL driver (default) | MIT |
| pyodbc | SQL Server driver | MIT |
| paramiko | SFTP connectivity | LGPL 2.1 |
| faker | Mock data generation | MIT |
| rich | Console output, progress, diff display | MIT |
| daff | Table diff engine | Apache 2.0 |
| multidiff | Binary hex diff | MIT |

Optional dependency groups:
- `[postgres]`: psycopg2-binary
- `[mysql-mit]`: pymysql (MIT, recommended)
- `[mysql-gpl]`: mysql-connector-python (GPL, Oracle official — see license note below)
- `[mssql]`: pyodbc
- `[all-db]`: psycopg2-binary + pymysql + pyodbc (all Apache 2.0 compatible)
- `[dev]`: pytest, pytest-cov, ruff, mypy

**MySQL license note:** mysql-connector-python is GPL licensed, which is incompatible with Apache 2.0 for distribution. PyMySQL (MIT) is the default and recommended MySQL driver. If you require the Oracle official driver, install it explicitly via `pip install ztract[mysql-gpl]` and ensure GPL compliance in your deployment.

All core and default optional dependencies are Apache 2.0 compatible.

### 12.2 JAR Distribution

- Primary: JAR committed to git, bundled in wheel
- Fallback: `ztract/engine/download_engine.py` downloads correct JAR version from GitHub Releases if JAR not present (e.g., sdist install without bundled JAR)
- Called automatically at startup if JAR missing

### 12.3 Java Engine Source

- Location: `engine-java/` in repo root
- `pom.xml` with maven-shade-plugin
- Single dependency: `za.co.absa.cobrix:cobol-parser_2.12:2.10.1`
- Built JAR copied to `ztract/engine/ztract-engine.jar`
- CI builds JAR from source and verifies it matches committed JAR

### 12.4 Repository Structure

```
ztract/
+-- ztract/                         # Python package
|   +-- __main__.py
|   +-- codepages.py
|   +-- cli/
|   |   +-- root.py
|   |   +-- convert.py
|   |   +-- diff.py
|   |   +-- generate.py
|   |   +-- run.py
|   |   +-- inspect.py
|   |   +-- validate.py
|   |   +-- init.py
|   |   +-- status.py
|   +-- config/
|   |   +-- loader.py
|   |   +-- schema.py
|   +-- engine/
|   |   +-- bridge.py
|   |   +-- download_engine.py
|   |   +-- ztract-engine.jar
|   +-- connectors/
|   |   +-- base.py
|   |   +-- dataset_format.py
|   |   +-- local.py
|   |   +-- ftp.py
|   |   +-- sftp.py
|   |   +-- zowe.py
|   +-- writers/
|   |   +-- base.py
|   |   +-- csv.py
|   |   +-- jsonl.py
|   |   +-- parquet.py
|   |   +-- database.py
|   |   +-- ebcdic.py
|   +-- pipeline/
|   |   +-- orchestrator.py
|   |   +-- fanout.py
|   |   +-- step_context.py
|   +-- diff/
|   |   +-- differ.py
|   |   +-- redefines.py
|   +-- generate/
|   |   +-- generator.py
|   |   +-- field_patterns.py
|   +-- observability/
|       +-- logging.py
|       +-- audit.py
|       +-- progress.py
|       +-- rejects.py
+-- engine-java/                    # Java wrapper source
|   +-- pom.xml
|   +-- src/main/java/...
+-- tests/
|   +-- conftest.py
|   +-- test_data/
|   |   +-- CUSTMAST.cpy
|   |   +-- CUST_FB.DAT
|   |   +-- CUST_VB.DAT
|   |   +-- CUST_CP277.DAT
|   +-- connectors/
|   |   +-- test_local.py
|   |   +-- test_ftp.py
|   |   +-- test_sftp.py
|   |   +-- test_zowe.py
|   +-- writers/
|   |   +-- test_csv.py
|   |   +-- test_parquet.py
|   |   +-- test_database.py
|   +-- diff/
|   |   +-- test_differ.py
|   |   +-- test_redefines.py
|   +-- generate/
|   |   +-- test_generator.py
|   +-- pipeline/
|   |   +-- test_orchestrator.py
|   +-- engine/
|       +-- test_bridge.py
+-- docs/
|   +-- superpowers/specs/
+-- pyproject.toml
+-- LICENSE
+-- NOTICE
+-- README.md
+-- CHANGELOG.md
+-- .github/
    +-- workflows/
```

### 12.5 Testing Strategy

- **Unit tests:** mock all external I/O (FTP, DB, Java subprocess)
- **Integration tests:** real Java engine, real files (marked `@pytest.mark.integration`, skipped unless JRE available)
- **All test data** generated via `ztract generate` with fixed `--seed 42`
- **Norwegian cp277 test data** always included — ae o aa must round-trip correctly
- **CI matrix:** Ubuntu, Windows, macOS x JRE 11, 17, 21
- **Windows cp277 test:** explicit CI test decoding cp277 data on Windows, verifying Norwegian characters in output (the #1 silent failure risk)

### 12.6 CI (GitHub Actions)

- Lint: ruff
- Type check: mypy
- Unit tests: pytest (no JRE needed)
- Integration tests: pytest with JRE (matrix: JRE 11, 17, 21)
- JAR build verification: Maven build from source, compare to committed JAR
- Platforms: Ubuntu, Windows, macOS

### 12.7 Distribution Strategy

**Phase 1: pip only**
```
pip install ztract
```
Primary users are DBAs on Windows laptops — pip is strictly lower friction than Docker for this audience. JRE 11+ is the only external prerequisite.

**Phase 2: Docker (CI/CD and server use)**
```
docker pull srrc1334/ztract
```
Target scenarios: CI/CD pipelines and server deployments where JRE setup is the friction point.

Dockerfile: `FROM eclipse-temurin:17-jre-alpine` + `pip install ztract`. Simple, small image.

Docker is NOT recommended for primary local PC use due to:
- Volume mount friction for local files
- FTP/SFTP network complexity from inside containers
- Zowe CLI profile access issues
- Degraded rich terminal output

---

## Attribution

- **Cobrix** by AbsaOSS (Apache 2.0) — COBOL copybook parsing and EBCDIC binary decoding engine
- NOTICE file credits Cobrix and all major dependencies
