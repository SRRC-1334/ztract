# Connectors

Ztract supports four connectivity backends for reading and writing mainframe datasets. The backend is selected automatically based on the `--input` / `--output` URL scheme, or configured explicitly in YAML pipelines.

---

## Local File

The simplest backend — read or write a file already on your local filesystem.

```bash
ztract convert \
  --copybook CUSTMAST.cpy \
  --input    ./data/CUST.MASTER.DAT \
  --recfm    FB --lrecl 500 \
  --codepage cp277 \
  --output   customers.csv
```

No additional configuration is needed. Relative and absolute paths are both accepted.

---

## FTP

Connect directly to z/OS over FTP. Ztract streams the dataset in binary (image) mode, which is required for EBCDIC data to avoid translation by the FTP server.

### Inline URL

```bash
ztract convert \
  --copybook CUSTMAST.cpy \
  --input    ftp://myuser:mypass@mf01.bank.com/BEL.CUST.MASTER \
  --recfm    FB --lrecl 500 \
  --codepage cp277 \
  --output   customers.csv
```

### YAML Connection Profile

```yaml
connections:
  prod_ftp: &prod_ftp
    type: ftp
    host: mf01.bank.com
    user: ${PROD_USER}
    password: ${PROD_PASS}
    port: 21
    transfer_mode: binary     # binary (default) or text
    ftp_mode: passive         # passive (default) or active
```

### FTP Options

| Field | Default | Description |
|---|---|---|
| `host` | — | FTP server hostname or IP |
| `user` | — | FTP username |
| `password` | — | FTP password; use `${ENV_VAR}` to avoid hardcoding |
| `port` | `21` | FTP port |
| `transfer_mode` | `binary` | `binary` for EBCDIC data; `text` for text datasets |
| `ftp_mode` | `passive` | `passive` (PASV) or `active` (PORT) |

### Writing Back to z/OS (SITE Commands)

When writing a dataset back to the mainframe over FTP, Ztract issues SITE commands to allocate the dataset with the correct DCB attributes:

```yaml
steps:
  - name: push-report
    action: upload
    input:
      path: ./output/monthly_changes.csv
    output:
      connection: *prod_ftp
      dataset: BEL.CUST.CHANGERPT
      site_commands:
        recfm: FB
        lrecl: 500
        blksize: 27920
        space_unit: CYLINDERS
        primary: 5
        secondary: 2
```

| SITE parameter | Description |
|---|---|
| `recfm` | Record format (FB, VB, etc.) |
| `lrecl` | Logical record length |
| `blksize` | Block size |
| `space_unit` | `CYLINDERS` or `TRACKS` |
| `primary` | Primary space allocation |
| `secondary` | Secondary space allocation |

### Retry Behaviour

FTP connections retry automatically on transient failures using exponential backoff. The retry sequence is 1 s, 2 s, 4 s, 8 s, 16 s (5 attempts total). If all retries are exhausted, the job fails with a descriptive error.

---

## SFTP

Connect to z/OS over SSH/SFTP. Preferred over FTP when your mainframe accepts SSH connections, as credentials are encrypted in transit.

### Inline URL

```bash
ztract convert \
  --copybook CUSTMAST.cpy \
  --input    sftp://myuser@mf01.bank.com/BEL.CUST.MASTER \
  --recfm    FB --lrecl 500 \
  --codepage cp277 \
  --output   customers.csv
```

### YAML Connection Profile

```yaml
connections:
  prod_sftp: &prod_sftp
    type: sftp
    host: mf01.bank.com
    user: ${PROD_USER}
    password: ${PROD_PASS}         # password auth
    # key_path: ~/.ssh/id_rsa      # or key-based auth
    port: 22
```

### SFTP Options

| Field | Default | Description |
|---|---|---|
| `host` | — | SFTP server hostname or IP |
| `user` | — | SSH username |
| `password` | — | SSH password (mutually exclusive with `key_path`) |
| `key_path` | — | Path to SSH private key file |
| `port` | `22` | SSH port |

### z/OS MVS Path Formatting

MVS dataset names are automatically formatted into the z/OS SFTP path convention:

```
BEL.CUST.DATA  →  //'BEL.CUST.DATA'
```

You do not need to add the quotes or double-slash prefix manually — Ztract handles this when the path looks like an MVS dataset name (uppercase, dot-separated, no leading `/`).

USS paths (those starting with `/`) pass through unchanged.

!!! note
    SFTP to z/OS does not support SITE commands. Dataset allocation attributes (RECFM, LRECL, BLKSIZE) must be pre-configured via SMS or JCL before writing. For write-back operations requiring explicit DCB control, use FTP instead.

---

## Zowe

Zowe provides a modern REST/CLI gateway to z/OS services. Ztract supports two Zowe backends.

### Backends

| Backend | Flag | Description |
|---|---|---|
| z/OSMF (default) | `--zosmf-profile PROFILE` | REST API via z/OSMF; no extra Zowe plugin needed |
| zftp | `--zftp-profile PROFILE` | FTP-based Zowe plugin; supports `record` transfer mode for VB files |

### Transfer Modes

| Mode | Description |
|---|---|
| `binary` | Raw binary transfer (default; use for EBCDIC) |
| `text` | Text transfer with EBCDIC-to-ASCII conversion by z/OS |
| `encoding` | Specify explicit encoding pair for conversion |
| `record` | zftp only; preserves VB RDW headers exactly — use for VB/VBA files |

### YAML Connection Profile — z/OSMF

```yaml
connections:
  prod_zowe: &prod_zowe
    type: zowe
    backend: zosmf               # default
    profile: MYPROD              # Zowe CLI profile name
    transfer_mode: binary
```

### YAML Connection Profile — zftp

```yaml
connections:
  prod_zftp: &prod_zftp
    type: zowe
    backend: zftp
    profile: MYPROD_FTP
    transfer_mode: record        # record mode preserves RDW headers
    site_commands:               # zftp DCB allocation for writes
      recfm: VB
      lrecl: 32756
      blksize: 32760
```

### CLI Usage

```bash
# z/OSMF backend
ztract convert \
  --copybook     CUSTMAST.cpy \
  --zosmf-profile MYPROD \
  --dataset      BEL.CUST.MASTER \
  --recfm        FB --lrecl 500 \
  --codepage     cp277 \
  --output       customers.csv

# zftp backend
ztract convert \
  --copybook    CUSTMAST.cpy \
  --zftp-profile MYPROD_FTP \
  --dataset     BEL.CUST.MASTER \
  --recfm       VB \
  --codepage    cp277 \
  --output      customers.csv
```

!!! note
    The Zowe CLI must be installed and the specified profile must be configured and authenticated before running Ztract. Check with `zowe profiles list` and `zowe zosmf check status --zosmf-profile MYPROD`. For zftp, the `@zowe/zos-ftp-for-zowe-cli` plugin must be installed: `zowe plugins install @zowe/zos-ftp-for-zowe-cli`.
