# EBCDIC Code Pages

EBCDIC (Extended Binary Coded Decimal Interchange Code) is IBM's character encoding for mainframe systems. Unlike ASCII, there is no single EBCDIC — different countries and regions use different variants. Ztract supports the most common IBM EBCDIC code pages and provides human-readable aliases for each.

---

## Supported Code Pages

| Code page | Aliases | Region | Notes |
|---|---|---|---|
| `cp277` ⭐ | `norway`, `norwegian`, `danish`, `nordic`, `277` | Denmark / Norway | **Primary** — includes æ ø å Æ Ø Å |
| `cp037` | `us`, `usa`, `canada`, `default`, `037` | USA / Canada | IBM default EBCDIC |
| `cp273` | `germany`, `german`, `austria`, `273` | Germany / Austria | Includes ä ö ü ß |
| `cp875` | `greek`, `greece`, `875` | Greece | Greek alphabet support |
| `cp870` | `eastern_europe`, `poland`, `czech`, `870` | Eastern Europe | Polish, Czech, Slovak |
| `cp1047` | `latin1`, `open_systems`, `1047` | Latin-1 / USS | UNIX System Services on z/OS |
| `cp838` | `thailand`, `thai`, `838` | Thailand | Thai character set |
| `cp1025` | `cyrillic`, `russian`, `1025` | Russia / CIS | Cyrillic alphabet |

---

## cp277 — Norwegian/Danish (Primary)

cp277 is the primary code page for Ztract because Scandinavian mainframe shops are a major user base. It correctly encodes and decodes the six extra characters that distinguish Norwegian and Danish from standard ASCII:

| Character | Description |
|---|---|
| `æ` / `Æ` | ae ligature (Norwegian/Danish letter) |
| `ø` / `Ø` | o with stroke |
| `å` / `Å` | a with ring above |

These characters occupy different byte positions in cp277 versus cp037. Using the wrong code page will produce garbage output for any record containing Norwegian names, addresses, or text fields.

**Example:** The name `Bjørn Åse` decoded with cp037 produces `Bj|rn }\e` — the classic symptom of a wrong code page.

---

## Specifying a Code Page

Pass `--codepage` with either the canonical name, a numeric code, or any alias. All three of these are equivalent:

```bash
--codepage cp277
--codepage norway
--codepage 277
```

In a YAML pipeline:

```yaml
steps:
  - name: extract
    action: convert
    input:
      codepage: cp277
      # or: codepage: norway
      # or: codepage: 277
```

If `--codepage` is omitted, Ztract defaults to `cp037` (US/Canada EBCDIC). Always specify the code page explicitly for production jobs to avoid surprises if the default changes in a future version.

---

## How to Determine the Right Code Page

If you do not know which code page a file was created with, ask the mainframe team. The code page is a property of how the data was written, not the file itself — there is no reliable way to auto-detect it from binary data alone.

Common indicators:

- **Norwegian/Danish shop** → try `cp277`
- **German shop** → try `cp273`
- **USA/Canada, no local characters** → `cp037`
- **z/OS USS (Unix paths, shell scripts)** → `cp1047`
- **Greek text fields** → `cp875`

When in doubt, run `ztract validate --sample 100` and inspect the decoded sample values in the output. If names and text fields look correct, you have the right code page.

---

## Adding New Code Pages

The full list of supported code pages and their aliases is defined in `ztract/codepages.py`. The file maps each code page name to its Python `codecs` name and lists all accepted aliases.

To add a new code page or alias:

1. Open `ztract/codepages.py`
2. Add an entry to the `CODEPAGE_ALIASES` dict following the existing pattern
3. Verify the code page name is accepted by Python's `codecs.lookup()` before submitting a PR

Pull requests adding well-documented code pages for new regions are welcome. See [Contributing](contributing.md).
