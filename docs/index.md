# Ztract — Extract Mainframe EBCDIC Data

**Read any mainframe EBCDIC file on your laptop. Zero MIPS spent.**

> **Status:** 0.1.0.dev1 — active development.
> Install the early preview with `pip install ztract==0.1.0.dev1`.

---

## What is Ztract?

Ztract is a Python CLI tool that extracts, transforms, and compares mainframe EBCDIC binary files using real COBOL copybooks — no Spark, no cluster, no proprietary tooling.

All the hard parsing (COMP-3 packed decimal, REDEFINES, OCCURS DEPENDING ON, RDW/BDW headers) is handled by [Cobrix](https://github.com/AbsaOSS/cobrix) — a battle-tested, open-source COBOL parser — running as a subprocess. Python handles connectivity, output, orchestration, and observability.

The result: pull files from your mainframe via FTP, SFTP, or Zowe, decode them with your existing `.cpy` copybooks, and write to CSV, Parquet, a database, or back to the mainframe — in one command.

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

## Comparison

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

## 30-Second Quick Start

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

2.4 million Norwegian customer records, correctly decoded (æ ø å Æ Ø Å), in under 3 minutes. On a laptop. Zero mainframe CPU.

---

## Documentation

| Page | Description |
|---|---|
| [Installation](installation.md) | Prerequisites, pip install, optional DB drivers, dev setup |
| [Quick Start](quickstart.md) | Step-by-step walkthrough from install to first extract |
| [Commands](commands.md) | Full CLI reference for all 8 commands |
| [Connectors](connectors.md) | Local, FTP, SFTP, and Zowe connectivity |
| [EBCDIC Code Pages](codepages.md) | Supported code pages and aliases |
| [YAML Pipelines](yaml-pipelines.md) | Multi-step pipeline format reference |
| [Copybooks](copybooks.md) | COBOL copybook guide and supported features |
| [Contributing](contributing.md) | How to contribute to Ztract |
