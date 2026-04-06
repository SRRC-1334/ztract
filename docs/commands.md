# Commands

Ztract provides 8 CLI commands. All commands support `--debug` and `--quiet` global flags.

---

## Global Options

| Option | Description |
|---|---|
| `--version` | Show the installed Ztract version and exit |
| `--debug` | Enable verbose debug logging to stderr |
| `--quiet` | Suppress progress bars and informational output |

---

## ztract convert

Extract an EBCDIC binary file and write to one or more output targets.

```bash
ztract convert [OPTIONS]
```

| Option | Required | Description |
|---|---|---|
| `--copybook PATH` | Yes | Path to the COBOL copybook (`.cpy`) file |
| `--input PATH_OR_URL` | Yes | Input file path or connector URL (local path, `ftp://`, `sftp://`) |
| `--output PATH_OR_URL` | Yes | Output target; repeat for multiple outputs |
| `--recfm [F|FB|V|VB|FBA|VBA]` | Yes | IBM record format of the input file |
| `--lrecl INT` | Yes (F/FB) | Logical record length in bytes |
| `--codepage TEXT` | No | EBCDIC code page or alias (default: `cp037`) |
| `--format [csv|jsonl|parquet]` | No | Output format override; inferred from file extension if omitted |
| `--delimiter CHAR` | No | Field delimiter for CSV output (default: `,`) |
| `--encoding TEXT` | No | Output text encoding for CSV (default: `utf-8`) |

**Examples:**

```bash
# Local file to CSV
ztract convert \
  --copybook CUSTMAST.cpy \
  --input    CUST.DAT \
  --recfm    FB --lrecl 500 \
  --codepage cp277 \
  --output   customers.csv

# FTP source to Parquet
ztract convert \
  --copybook CUSTMAST.cpy \
  --input    ftp://user:pass@mf01.bank.com/BEL.CUST.MASTER \
  --recfm    FB --lrecl 500 \
  --codepage cp277 \
  --output   customers.parquet

# Multiple outputs in one pass
ztract convert \
  --copybook CUSTMAST.cpy \
  --input    CUST.DAT \
  --recfm    FB --lrecl 500 \
  --codepage cp277 \
  --output   customers.csv \
  --output   customers.parquet \
  --output   "postgresql://user:pass@localhost/dwh?table=customers"
```

---

## ztract diff

Compare two EBCDIC files field-by-field using a copybook as the schema.

```bash
ztract diff [OPTIONS]
```

| Option | Required | Description |
|---|---|---|
| `--copybook PATH` | Yes | Path to the COBOL copybook |
| `--before PATH` | Yes | The earlier/reference EBCDIC file |
| `--after PATH` | Yes | The later/comparison EBCDIC file |
| `--key FIELD` | Yes | Copybook field name to use as the record key |
| `--recfm [F|FB|V|VB|FBA|VBA]` | Yes | Record format |
| `--lrecl INT` | Yes (F/FB) | Logical record length |
| `--codepage TEXT` | No | EBCDIC code page (default: `cp037`) |
| `--format [console|csv|json]` | No | Diff output format (default: `console`) |

**Example:**

```bash
ztract diff \
  --copybook CUSTMAST.cpy \
  --before   CUST_JAN.DAT \
  --after    CUST_FEB.DAT \
  --key      CUST-ID \
  --codepage cp277 \
  --recfm    FB --lrecl 500
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

---

## ztract generate

Generate synthetic EBCDIC test data from a copybook.

```bash
ztract generate [OPTIONS]
```

| Option | Required | Description |
|---|---|---|
| `--copybook PATH` | Yes | Path to the COBOL copybook |
| `--records INT` | Yes | Number of records to generate |
| `--output PATH` | Yes | Output file path for generated EBCDIC data |
| `--codepage TEXT` | No | EBCDIC code page for output (default: `cp037`) |
| `--recfm [F|FB|V|VB]` | No | Record format for output (default: `FB`) |
| `--lrecl INT` | No | Logical record length; inferred from copybook if omitted |
| `--seed INT` | No | Random seed for reproducible output |
| `--edge-cases` | No | Include boundary value records every 100th record |

**Examples:**

```bash
# Basic generation
ztract generate \
  --copybook CUSTMAST.cpy \
  --records  100000 \
  --codepage cp277 \
  --recfm    FB --lrecl 500 \
  --seed     42 \
  --output   CUST_MOCK.DAT

# With edge cases for numeric boundary testing
ztract generate \
  --copybook COMPLEX_NUMERIC.cpy \
  --records  1000 \
  --edge-cases \
  --seed     42 \
  --recfm    FB --lrecl 300 \
  --output   NUMERIC_TEST.DAT
```

---

## ztract run

Execute a multi-step YAML pipeline file.

```bash
ztract run JOB_FILE [OPTIONS]
```

| Argument / Option | Required | Description |
|---|---|---|
| `JOB_FILE` | Yes | Path to the YAML pipeline file |
| `--dry-run` | No | Parse and validate the pipeline without executing any steps |
| `--step NAME` | No | Execute only the named step (useful for debugging or re-running a failed step) |

**Examples:**

```bash
# Run the full pipeline
ztract run monthly-reconciliation.yaml

# Validate pipeline syntax without running
ztract run monthly-reconciliation.yaml --dry-run

# Run a single step
ztract run monthly-reconciliation.yaml --step extract-prod
```

---

## ztract inspect

Display a copybook layout as a formatted field table.

```bash
ztract inspect [OPTIONS]
```

| Option | Required | Description |
|---|---|---|
| `--copybook PATH` | Yes | Path to the COBOL copybook |

**Example:**

```bash
ztract inspect --copybook CUSTMAST.cpy
```

Output includes field name, level, PIC clause, byte offset, and size (with COMP-3/COMP notes where applicable).

---

## ztract validate

Run a pre-flight check by decoding a sample of records and reporting statistics.

```bash
ztract validate [OPTIONS]
```

| Option | Required | Description |
|---|---|---|
| `--copybook PATH` | Yes | Path to the COBOL copybook |
| `--input PATH_OR_URL` | Yes | Input file path or connector URL |
| `--recfm [F|FB|V|VB|FBA|VBA]` | Yes | Record format |
| `--lrecl INT` | Yes (F/FB) | Logical record length |
| `--codepage TEXT` | No | EBCDIC code page (default: `cp037`) |
| `--sample INT` | No | Number of records to sample (default: `1000`) |

**Example:**

```bash
ztract validate \
  --copybook CUSTMAST.cpy \
  --input    CUST.MASTER.DAT \
  --recfm    FB --lrecl 500 \
  --codepage cp277 \
  --sample   5000
```

---

## ztract init

Scaffold a new Ztract project directory with example files.

```bash
ztract init [OPTIONS]
```

| Option | Required | Description |
|---|---|---|
| `--dir PATH` | No | Directory to initialise (default: current directory) |

Creates a project structure including a sample YAML pipeline, example copybook, and `.env` template for credentials.

---

## ztract status

Show recent job history from the audit log.

```bash
ztract status [OPTIONS]
```

| Option | Required | Description |
|---|---|---|
| `--job NAME` | No | Filter history to a specific job name |
| `--last INT` | No | Show only the N most recent entries (default: `10`) |

**Example:**

```bash
# Show last 10 jobs
ztract status

# Show last 5 runs of a specific job
ztract status --job customer-monthly-reconciliation --last 5
```

Output includes job name, start time, duration, record counts, and status (success / failed / partial).
