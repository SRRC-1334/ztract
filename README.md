<div align="center">
  <img src="docs/logo.svg" alt="Ztract" width="720"/>

  <br/>

  [![Python 3.10+](https://img.shields.io/badge/python-3.10+-brightgreen.svg?style=flat-square&logo=python&logoColor=white)](https://www.python.org)
  [![License](https://img.shields.io/badge/license-Apache%202.0-brightgreen.svg?style=flat-square)](LICENSE)
  [![PyPI](https://img.shields.io/pypi/v/ztract?style=flat-square&logo=PyPI&logoColor=white&color=brightgreen)](https://pypi.org/project/ztract)
  [![JRE](https://img.shields.io/badge/JRE-11+-brightgreen.svg?style=flat-square&logo=openjdk&logoColor=white)](https://adoptium.net)
  [![Docs](https://readthedocs.org/projects/ztract/badge/?version=latest&style=flat-square&logo=readthedocs&logoColor=white)](https://ztract.readthedocs.io)

  > **Status:** 0.1.0.dev1 -- active development
  > `pip install ztract==0.1.0.dev1` to try early
  <br/>

  **Read any mainframe EBCDIC file on your laptop. Zero MIPS spent.**

</div>

---

## 30 Seconds

```bash
pip install ztract

ztract convert \
  --copybook CUSTMAST.cpy \
  --input    CUST.MASTER.DAT \
  --recfm    FB  --lrecl 500 \
  --codepage cp277 \
  --output   customers.csv
```

```
⠿ extract-prod  ████████████████████  2,400,000 rec  ✓
  15,234 rec/s · elapsed 2m 37s · 0 rejects

Done. 2,400,000 records → customers.csv
```

**2.4 million Norwegian customer records, correctly decoded (æ ø å Æ Ø Å), in under 3 minutes. On a laptop. Zero mainframe CPU.**

---

## What is Ztract?

Ztract is a Python CLI tool that extracts, transforms, and compares mainframe EBCDIC binary files using real COBOL copybooks — no Spark, no cluster, no proprietary tooling.

All the hard parsing (COMP-3 packed decimal, REDEFINES, OCCURS DEPENDING ON, RDW/BDW headers) is handled by **[Cobrix](https://github.com/AbsaOSS/cobrix)** — a battle-tested, open-source COBOL parser — running as a subprocess. Python handles connectivity, output, orchestration, and observability.

The result: pull files from your mainframe via FTP, SFTP, or Zowe, decode them with your existing `.cpy` copybooks, and write to CSV, Parquet, a database, or back to the mainframe. In one command.

---

## Features

- **Real COBOL copybooks** — use your `.cpy` files as-is, no conversion step, no JSON schema
- **All IBM record formats** — F, FB, V, VB, FBA, VBA (including BDW/RDW and ASA carriage control)
- **Norwegian & Scandinavian first** — cp277 primary, full æ ø å Æ Ø Å support out of the box
- **Bidirectional** — read from mainframe and write back; mainframe-to-mainframe flows via Ztract
- **Streaming** — never loads a full file into memory; millions of records handled on any machine
- **Field-level EBCDIC diff** — compare two EBCDIC files field-by-field using your copybook as schema
- **Mock data generator** — generate realistic synthetic EBCDIC test data from any copybook
- **YAML pipelines** — define multi-step extract/transform/load workflows in a single file
- **Copybook inspector** — visualise any `.cpy` file as a formatted field table in seconds
- **Enterprise observability** — structured JSON logs, immutable audit trail, reject files with full context

---

## Why not other tools?

| | Ztract | Python EBCDIC libs | Cobrix (Spark) | Proprietary tools |
|---|---|---|---|---|
| Real COBOL copybooks | ✅ | ❌ custom schema | ✅ | ✅ |
| REDEFINES / OCCURS | ✅ Cobrix | ⚠️ partial | ✅ | ✅ |
| cp277 Norwegian | ✅ | ⚠️ varies | ✅ | ✅ |
| No Spark required | ✅ | ✅ | ❌ | ✅ |
| pip install | ✅ | ✅ | ❌ | ❌ |
| EBCDIC diff | ✅ | ❌ | ❌ | ❌ |
| Mock generator | ✅ | ❌ | ❌ | ❌ |
| FTP/SFTP/Zowe built-in | ✅ | ❌ | ❌ | varies |
| Write back to mainframe | ✅ | ❌ | ❌ | varies |
| Open source | ✅ | ✅ | ✅ | ❌ |
| Cost | Free | Free | Free | $$$$ |

---

## Installation

```bash
pip install ztract
```

**Requirements:**
- Python 3.10+
- Java JRE 11+ on PATH (`java -version` to check — download from [Adoptium](https://adoptium.net) if needed)

That's it. Everything else — Cobrix engine, diff tools, progress bars — is bundled.

**Optional database drivers:**

```bash
pip install ztract[postgres]   # PostgreSQL (psycopg2)
pip install ztract[mysql]      # MySQL (PyMySQL)
pip install ztract[mssql]      # SQL Server (pyodbc)
pip install ztract[all-db]     # All three
```

---

## Quick Start

### Inspect a copybook

```bash
ztract inspect --copybook CUSTMAST.cpy
```

```
┌─────────────────┬───────┬───────────────┬────────┬────────────┐
│ Field           │ Level │ PIC           │ Offset │ Size       │
├─────────────────┼───────┼───────────────┼────────┼────────────┤
│ CUST-ID         │ 05    │ 9(10)         │ 0      │ 10         │
│ CUST-NAME       │ 05    │ X(50)         │ 10     │ 50         │
│ CUST-ADDR       │ 05    │ X(80)         │ 60     │ 80         │
│ CUST-CITY       │ 05    │ X(30)         │ 140    │ 30         │
│ CUST-AMT        │ 05    │ S9(9)V99      │ 170    │ 6 (COMP-3) │
│ CUST-DATE       │ 05    │ 9(8)          │ 176    │ 8          │
└─────────────────┴───────┴───────────────┴────────┴────────────┘
Total record length: 500 bytes
```

### Validate before extracting

```bash
ztract validate \
  --copybook CUSTMAST.cpy \
  --input    CUST.MASTER.DAT \
  --recfm    FB  --lrecl 500 \
  --codepage cp277 \
  --sample   1000
```

```
Validation complete (1,000 sample records)
  ✓ Decoded:   998
  ⚠ Warnings:    2  (invalid sign nibble — see rejects)
  ✗ Errors:      0
  CUST-AMT  min: 0.00   max: 9,999,999.99   null: 0.1%
  CUST-NAME sample: Bjørn Hansen, Åse Eriksen, Ole Nordmann
```

### Convert to CSV

```bash
ztract convert \
  --copybook CUSTMAST.cpy \
  --input    CUST.MASTER.DAT \
  --recfm    FB  --lrecl 500 \
  --codepage cp277 \
  --output   customers.csv
```

### Convert via FTP (pull direct from z/OS)

```bash
ztract convert \
  --copybook CUSTMAST.cpy \
  --input    ftp://mf01.bank.com/BEL.CUST.MASTER \
  --recfm    FB  --lrecl 500 \
  --codepage cp277 \
  --output   customers.parquet
```

### Multiple outputs in one pass

```bash
ztract convert \
  --copybook CUSTMAST.cpy \
  --input    CUST.MASTER.DAT \
  --recfm    FB  --lrecl 500 \
  --codepage cp277 \
  --output   customers.csv \
  --output   customers.parquet \
  --output   postgresql://user:pass@localhost/dwh?table=customer_master
```

All three targets written concurrently from a single read pass.

### Diff two EBCDIC files field-by-field

```bash
ztract diff \
  --copybook CUSTMAST.cpy \
  --before   CUST_JAN.DAT \
  --after    CUST_FEB.DAT \
  --key      CUST-ID \
  --codepage cp277 \
  --recfm    FB  --lrecl 500
```

```
ADDED    [CUST-ID=000456]  CUST-NAME=Bjørn Hansen
DELETED  [CUST-ID=000123]  CUST-NAME=Ole Nordmann
CHANGED  [CUST-ID=000789]
  CUST-ADDR:  "Oslo Gate 1" → "Bergen Gate 5"
  CUST-AMT:   12,345.67 → 12,500.00

Diff complete: 1 added · 1 deleted · 47 changed · 999,951 unchanged
of 1,000,000 total records · 43 seconds
```

### Generate synthetic EBCDIC test data

```bash
ztract generate \
  --copybook CUSTMAST.cpy \
  --records  100000 \
  --codepage cp277 \
  --recfm    FB  --lrecl 500 \
  --seed     42 \
  --output   CUST_MOCK.DAT
```

### Generate with boundary value edge cases

```bash
ztract generate \
  --copybook COMPLEX_NUMERIC.cpy \
  --records  1000 \
  --edge-cases \
  --seed     42 \
  --recfm    FB  --lrecl 300 \
  --output   NUMERIC_TEST.DAT
```

With `--edge-cases`, every 100th record cycles through boundary values: all zeros, all max values, all negatives. Catches encoding bugs that normal random data misses.

Norwegian field names automatically detected (NAVN, ADRESSE, TELEFON, BY) — generates realistic Scandinavian test data with valid packed decimal, correct EBCDIC encoding, and reproducible output.

---

## CLI Commands

| Command | Description |
|---|---|
| `ztract convert` | Extract EBCDIC → CSV / JSON Lines / Parquet / DB |
| `ztract diff` | Field-level comparison of two EBCDIC files |
| `ztract generate` | Generate synthetic EBCDIC test data from a copybook |
| `ztract run` | Execute a multi-step YAML pipeline |
| `ztract inspect` | Display copybook layout as a formatted field table |
| `ztract validate` | Pre-flight check: decode N sample records, report stats |
| `ztract status` | Show recent job history from audit log |
| `ztract init` | Scaffold a new Ztract project directory |

---

## YAML Pipelines

Define multi-step workflows in a single file:

```yaml
# monthly-reconciliation.yaml
version: "1.0"
job:
  name: customer-monthly-reconciliation

connections:
  prod: &prod
    type: ftp
    host: mf01.bank.com
    user: ${PROD_USER}
    password: ${PROD_PASS}
    transfer_mode: binary

steps:
  - name: extract-prod
    action: convert
    input:
      connection: *prod
      dataset: BEL.CUST.MASTER
      record_format: FB
      lrecl: 500
      codepage: cp277
    copybook: ./copybooks/CUSTMAST.cpy
    output:
      - type: csv
        path: ./output/prod_customers.csv
    expose_as: prod_data

  - name: diff-vs-last-month
    action: diff
    input:
      before: ./archive/CUST_LAST.DAT
      after:  $ref:prod_data.csv
    copybook: ./copybooks/CUSTMAST.cpy
    diff:
      key_fields: [CUST-ID]
    output:
      - type: console
      - type: csv
        path: ./output/monthly_changes.csv

  - name: push-report-to-mainframe
    action: upload
    input:
      path: ./output/monthly_changes.csv
    output:
      connection: *prod
      dataset: BEL.CUST.CHANGERPT
      site_commands:
        recfm: FB
        lrecl: 500
        blksize: 27920
        space_unit: CYLINDERS
        primary: 5
        secondary: 2
```

```bash
ztract run monthly-reconciliation.yaml
ztract run monthly-reconciliation.yaml --dry-run
ztract run monthly-reconciliation.yaml --step extract-prod
```

---

## Record Formats

| Format | Description |
|---|---|
| `F` | Fixed length — record size from copybook |
| `FB` | Fixed Blocked — records in fixed-size blocks |
| `V` | Variable length with 4-byte RDW headers |
| `VB` | Variable Blocked — BDW + RDW headers |
| `FBA` | Fixed Blocked + ASA carriage control (first byte stripped) |
| `VBA` | Variable Blocked + ASA carriage control (first byte stripped) |

---

## EBCDIC Code Pages

| Code page | Aliases | Region |
|---|---|---|
| `cp277` ⭐ | `norway`, `norwegian`, `danish`, `nordic` | Denmark / Norway — **primary** |
| `cp037` | `us`, `usa`, `canada`, `default` | USA / Canada — default |
| `cp273` | `germany`, `german`, `austria` | Germany / Austria |
| `cp875` | `greek`, `greece` | Greece |
| `cp870` | `eastern_europe`, `poland`, `czech` | Eastern Europe |
| `cp1047` | `latin1`, `open_systems` | Latin-1 / USS |
| `cp838` | `thailand`, `thai` | Thailand |
| `cp1025` | `cyrillic`, `russian` | Russia / CIS |

Use the alias anywhere a codepage is expected: `--codepage norway` or `--codepage cp277`.

---

## Output Targets

| Format | Description |
|---|---|
| `.csv` | Comma or pipe delimited, UTF-8, Excel-compatible BOM option |
| `.jsonl` | JSON Lines, one object per record, `ensure_ascii=False` |
| `.parquet` | Apache Parquet via pyarrow, schema auto-derived from copybook |
| `postgresql://...` | PostgreSQL via psycopg2 (optional: `pip install ztract[postgres]`) |
| `mysql://...` | MySQL via PyMySQL (optional: `pip install ztract[mysql]`) |
| `mssql://...` | SQL Server via pyodbc (optional: `pip install ztract[mssql]`) |
| `ftp://...` | Write back to z/OS via FTP with SITE commands for dataset allocation |
| `sftp://...` | Write back to z/OS via SFTP |

---

## Connectivity

| Type | Example |
|---|---|
| Local file | `--input ./CUST.DAT` |
| FTP | `--input ftp://user:pass@mf01.bank.com/BEL.CUST.DATA` |
| SFTP | `--input sftp://user@mf01.bank.com/BEL.CUST.DATA` |
| Zowe (z/OSMF) | `--zowe-profile MYPROD --dataset BEL.CUST.DATA` |
| Zowe (zftp) | `--zowe-profile MYPROD --zowe-backend zftp --dataset BEL.CUST.DATA` |

**Zowe transfer modes:** `binary` (default), `text`, `encoding`, `record` (zftp only, preserves VB RDW headers).

**SFTP z/OS paths:** MVS dataset names are auto-formatted (`BEL.CUST.DATA` -> `//'BEL.CUST.DATA'`). USS paths pass through unchanged.

Credentials support `${ENV_VAR}` interpolation in YAML. Passwords never hardcoded.

---

## Observability

Every job produces:

**Operational log** (`./logs/ztract_YYYY-MM-DD.log`) — structured JSON, rotated daily, 30-day retention, suitable for ELK/Splunk ingestion.

**Audit trail** (`./audit/ztract_audit.log`) — immutable append-only JSON Lines, one entry per job execution. Records user, machine, source dataset, record counts, status. Never rotated, never deleted. Compliance-ready.

**Reject file** (`./rejects/<job>_<step>_<timestamp>_rejects.jsonl`) — every failed record preserved with original EBCDIC hex bytes, decoded fields (if available), error type, error message, and byte offset. Re-processable.

---

## Acknowledgements

Ztract's COBOL parsing engine is built on **[Cobrix](https://github.com/AbsaOSS/cobrix)** by AbsaOSS — an outstanding open-source COBOL/EBCDIC parser for Apache Spark. We use the standalone `cobol-parser` module (no Spark dependency) and are grateful for the years of work that went into handling REDEFINES, OCCURS DEPENDING ON, packed decimal, and every IBM record format correctly. Standing on their shoulders.

Mainframe connectivity via **[Zowe CLI](https://www.zowe.org)** and the **[z/OS FTP Plugin](https://github.com/zowe/zowe-cli-ftp-plugin)** — the open source framework that makes z/OS accessible from modern tools. Zowe is a project of the [Open Mainframe Project](https://www.openmainframeproject.org/), hosted by the Linux Foundation.

Cloud storage output powered by **[fsspec](https://github.com/fsspec/filesystem_spec)** and the Apache Arrow ecosystem — enabling direct writes to S3, Azure Blob, and Google Cloud Storage from a single `pip install`.

Table diff powered by **[daff](https://github.com/paulfitz/daff)** (Apache 2.0).
Binary diff powered by **[multidiff](https://github.com/juhakivekas/multidiff)** (MIT).
Console output powered by **[rich](https://github.com/Textualize/rich)** (MIT).

**Full dependency table:**

| Package | Purpose | License |
|---|---|---|
| [Cobrix](https://github.com/AbsaOSS/cobrix) | COBOL parser engine | Apache 2.0 |
| [Zowe CLI](https://github.com/zowe/zowe-cli) | z/OS connectivity | EPL 2.0 |
| [daff](https://github.com/paulfitz/daff) | Table diff | Apache 2.0 |
| [multidiff](https://github.com/juhakivekas/multidiff) | Binary hex diff | MIT |
| [rich](https://github.com/Textualize/rich) | Console output | MIT |
| [pyarrow](https://arrow.apache.org/) | Parquet + cloud FS | Apache 2.0 |
| [fsspec](https://github.com/fsspec/filesystem_spec) | Cloud abstraction | BSD-3 |
| [paramiko](https://github.com/paramiko/paramiko) | SFTP | LGPL 2.1 |
| [click](https://github.com/pallets/click) | CLI framework | BSD-3 |
| [Faker](https://github.com/joke2k/faker) | Mock data | MIT |
| [SQLAlchemy](https://www.sqlalchemy.org/) | Database output | MIT |
| [PyYAML](https://github.com/yaml/pyyaml) | Config parsing | MIT |

See [NOTICE](NOTICE) for full attribution.

---

## Contributing

Copybook contributions especially welcome — if you have anonymised/synthetic copybooks for common mainframe layouts (banking, insurance, retail), consider adding them to `copybooks/`. They make Ztract more useful for everyone and are tested automatically via `ztract generate`.

See [CONTRIBUTING.md](CONTRIBUTING.md) to get started.

---

## License

Apache License 2.0 — see [LICENSE](LICENSE).

---

<div align="center">
  <sub>Built with ❤️ for the mainframe community · <a href="https://github.com/SRRC-1334/ztract">github.com/SRRC-1334/ztract</a></sub>
</div>
