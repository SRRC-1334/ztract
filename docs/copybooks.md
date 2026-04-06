# Copybooks

## What is a Copybook?

A COBOL copybook (`.cpy` file) is a reusable layout definition that describes the structure of a mainframe data record. It is the mainframe equivalent of a struct definition or a database schema — it names every field, specifies its data type and length, and gives its position within the binary record.

Ztract uses copybooks directly. You pass your existing `.cpy` file to every command with `--copybook`. No conversion, no JSON schema, no intermediate format — the copybook is the schema.

```bash
ztract convert --copybook CUSTMAST.cpy --input CUST.DAT ...
```

---

## Supported COBOL Features

Ztract supports the full range of COBOL data types and structural features that appear in real mainframe copybooks, delegating parsing to the [Cobrix](https://github.com/AbsaOSS/cobrix) engine.

### Data Types

| COBOL clause | Description | Notes |
|---|---|---|
| `PIC X(n)` | Alphanumeric / text | Decoded using `--codepage` |
| `PIC 9(n)` | Unsigned zoned decimal | |
| `PIC S9(n)` | Signed zoned decimal | |
| `PIC S9(n)V9(m)` | Signed zoned decimal with implied decimal | |
| `COMP-3` / `PACKED-DECIMAL` | Packed decimal | 2 digits per byte; common for amounts |
| `COMP` / `BINARY` | Binary integer | 2 or 4 bytes depending on PIC size |
| `COMP-1` | Single-precision floating point (32-bit) | |
| `COMP-2` | Double-precision floating point (64-bit) | |

### Structural Features

| Feature | Description |
|---|---|
| `REDEFINES` | Field overlaps another field's storage (union equivalent) |
| `OCCURS n TIMES` | Fixed-length array of n repetitions |
| `OCCURS DEPENDING ON field` | Variable-length array; length determined at runtime by another field |
| `FILLER` | Unnamed padding bytes; omitted from output |
| Nested group levels | `01`, `05`, `10`, `15`, etc. hierarchy fully supported |

!!! note
    REDEFINES and OCCURS DEPENDING ON are among the trickiest COBOL constructs. Cobrix handles them correctly for all record formats including VB.

---

## Record Formats

The `--recfm` flag tells Ztract how records are delimited in the binary file. IBM z/OS uses several record format conventions.

| Format | Description |
|---|---|
| `F` | **Fixed** — every record is exactly `--lrecl` bytes; no block structure |
| `FB` | **Fixed Blocked** — records are packed into fixed-size blocks; most common format |
| `V` | **Variable** — each record is preceded by a 4-byte Record Descriptor Word (RDW) containing the record length |
| `VB` | **Variable Blocked** — records are grouped into blocks; each block has a 4-byte Block Descriptor Word (BDW), each record has an RDW |
| `FBA` | **Fixed Blocked with ASA** — like FB, but the first byte of each record is an ASA carriage control character; Ztract strips it |
| `VBA` | **Variable Blocked with ASA** — like VB with ASA carriage control |

For `F` and `FB` formats, `--lrecl` is required and must match the total byte length reported by `ztract inspect`. For `V` and `VB`, the record length is read from the RDW headers embedded in the file.

---

## Using ztract inspect

Before running an extraction, inspect your copybook to confirm field names, sizes, and the total record length:

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

The **Total record length** at the bottom is what you pass as `--lrecl` for FB/F files.

---

## Common Copybook Patterns

### Customer Master

```cobol
01  CUSTOMER-RECORD.
    05  CUST-ID           PIC 9(10).
    05  CUST-NAME         PIC X(50).
    05  CUST-ADDR         PIC X(80).
    05  CUST-CITY         PIC X(30).
    05  CUST-POSTCODE     PIC 9(4).
    05  CUST-AMT          PIC S9(9)V99 COMP-3.
    05  CUST-DATE         PIC 9(8).
    05  FILLER            PIC X(312).
```

### Transaction Record

```cobol
01  TRANSACTION-RECORD.
    05  TXN-ID            PIC 9(12).
    05  TXN-DATE          PIC 9(8).
    05  TXN-TIME          PIC 9(6).
    05  TXN-AMOUNT        PIC S9(11)V99 COMP-3.
    05  TXN-CURRENCY      PIC X(3).
    05  TXN-TYPE          PIC X(2).
    05  TXN-ACCT-FROM     PIC 9(16).
    05  TXN-ACCT-TO       PIC 9(16).
    05  TXN-DESC          PIC X(80).
    05  FILLER            PIC X(100).
```

### Batch File Header / Trailer

```cobol
01  BATCH-HEADER.
    05  REC-TYPE          PIC X(1).     * 'H' for header
    05  BATCH-DATE        PIC 9(8).
    05  BATCH-SEQ         PIC 9(6).
    05  SOURCE-SYSTEM     PIC X(8).
    05  FILLER            PIC X(77).

01  BATCH-TRAILER.
    05  REC-TYPE          PIC X(1).     * 'T' for trailer
    05  RECORD-COUNT      PIC 9(10).
    05  CONTROL-TOTAL     PIC S9(13)V99 COMP-3.
    05  FILLER            PIC X(75).
```

---

## Tips and Common Mistakes

**LRECL must match the copybook total exactly.** If `ztract inspect` reports 500 bytes but you pass `--lrecl 490`, records will be misaligned and you will get decode errors or garbled output. Always run `ztract inspect` first.

**FILLER fields are skipped in output.** Copybooks frequently contain `FILLER` fields that are padding bytes with no business meaning. Ztract omits them from CSV, Parquet, and database outputs. They are accounted for in byte offset calculations.

**Hyphen vs underscore in field names.** COBOL uses hyphens in field names (`CUST-ID`). Ztract preserves hyphens in output column names by default. If your target system requires underscores, check output writer options.

**REDEFINES requires a record format that supports it.** When a copybook uses REDEFINES, all variants share the same bytes. Cobrix handles this by outputting all defined variants as separate columns; expect nullable columns where only one variant is populated per record.

**cp277 for Norwegian fields.** If your copybook has fields like NAVN (name), ADRESSE (address), BY (city), make sure to pass `--codepage cp277`. Using `cp037` will produce incorrect characters for æ, ø, and å.
