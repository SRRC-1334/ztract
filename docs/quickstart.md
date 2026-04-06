# Quick Start

This guide walks through a complete end-to-end extraction from install to output. Each step builds on the previous one.

---

## Step 1: Install

```bash
pip install ztract
```

Confirm the install worked:

```bash
ztract --version
```

```
ztract, version 0.1.0.dev1
```

See [Installation](installation.md) for prerequisites (Python 3.10+ and JRE 11+ are required).

---

## Step 2: Inspect Your Copybook

Before extracting any data, use `ztract inspect` to see exactly what fields are in your copybook and confirm the record length matches your data file.

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

Note the total record length — you will need it as `--lrecl` in subsequent commands.

---

## Step 3: Validate Before Extracting

Run a pre-flight check on a sample of records before committing to a full extraction. This catches encoding issues, truncated files, and LRECL mismatches early.

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

!!! note
    If warnings or errors are higher than expected, check that `--lrecl` matches the copybook's total record length and that `--codepage` matches the encoding used when the file was created.

---

## Step 4: Convert to CSV

Once validation looks good, run the full extraction:

```bash
ztract convert \
  --copybook CUSTMAST.cpy \
  --input    CUST.MASTER.DAT \
  --recfm    FB  --lrecl 500 \
  --codepage cp277 \
  --output   customers.csv
```

```
⠿ convert  ████████████████████  2,400,000 rec  ✓
  15,234 rec/s · elapsed 2m 37s · 0 rejects

Done. 2,400,000 records → customers.csv
```

The output file is UTF-8 CSV with a header row derived from your copybook field names.

---

## Step 5: Pull Direct from Mainframe via FTP

Skip the manual file transfer step by pulling the dataset directly from z/OS over FTP:

```bash
ztract convert \
  --copybook CUSTMAST.cpy \
  --input    ftp://user:pass@mf01.bank.com/BEL.CUST.MASTER \
  --recfm    FB  --lrecl 500 \
  --codepage cp277 \
  --output   customers.parquet
```

Ztract connects, streams the binary dataset, decodes it through the copybook, and writes Parquet — all in one pass. The FTP connection uses binary (image) transfer mode automatically for EBCDIC data.

See [Connectors](connectors.md) for FTP, SFTP, and Zowe options including credential handling.

---

## Step 6: Multiple Outputs in One Pass

Write to several targets simultaneously from a single read of the input file:

```bash
ztract convert \
  --copybook CUSTMAST.cpy \
  --input    CUST.MASTER.DAT \
  --recfm    FB  --lrecl 500 \
  --codepage cp277 \
  --output   customers.csv \
  --output   customers.parquet \
  --output   "postgresql://user:pass@localhost/dwh?table=customer_master"
```

All three targets are written concurrently from a single read pass. This is significantly faster than running three separate conversions and avoids reading a large mainframe dataset multiple times.

---

## Step 7: Generate Mock Test Data

Use `ztract generate` to create synthetic EBCDIC test files from a copybook. Useful for building test pipelines without real production data.

```bash
ztract generate \
  --copybook CUSTMAST.cpy \
  --records  100000 \
  --codepage cp277 \
  --recfm    FB  --lrecl 500 \
  --seed     42 \
  --output   CUST_MOCK.DAT
```

The `--seed` flag makes output reproducible — the same seed always produces the same records. Norwegian field names (NAVN, ADRESSE, BY, TELEFON) are detected automatically and generate realistic Scandinavian test values including correctly encoded æ ø å characters.

To include boundary-value edge cases every 100th record:

```bash
ztract generate \
  --copybook CUSTMAST.cpy \
  --records  1000 \
  --edge-cases \
  --seed     42 \
  --recfm    FB  --lrecl 500 \
  --output   CUST_EDGE.DAT
```

Edge case records cycle through: all zeros, all maximum values, all negatives. These catch encoding bugs that normal random data misses.

---

## Next Steps

- [Commands](commands.md) — full option reference for every CLI command
- [YAML Pipelines](yaml-pipelines.md) — automate multi-step workflows
- [Connectors](connectors.md) — FTP, SFTP, and Zowe connectivity details
- [Copybooks](copybooks.md) — supported COBOL features and record format guide
