# Cloud Storage Output

Ztract can write decoded EBCDIC data directly to cloud storage — no intermediate local file needed. All three file-based writers (CSV, JSON Lines, Parquet) support cloud URIs via [fsspec](https://filesystem-spec.readthedocs.io/).

---

## Supported Providers

| Provider | URI Scheme | Extra Package |
|---|---|---|
| AWS S3 | `s3://bucket/key` | `pip install ztract[aws]` |
| Azure Blob / ADLS | `az://container/path` or `abfs://` | `pip install ztract[azure]` |
| Google Cloud Storage | `gs://bucket/path` | `pip install ztract[gcp]` |
| All three | | `pip install ztract[cloud]` |

---

## Installation

```bash
pip install ztract[aws]     # AWS S3 only
pip install ztract[azure]   # Azure Blob only
pip install ztract[gcp]     # Google Cloud only
pip install ztract[cloud]   # All three providers
```

The base `pip install ztract` includes `fsspec` (transitive via pyarrow) but not the provider-specific packages.

---

## AWS S3

### Credentials via Environment Variables (recommended)

```bash
export AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
export AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
export AWS_DEFAULT_REGION=eu-west-1

ztract convert \
  --copybook CUSTMAST.cpy \
  --input CUST.DAT \
  --recfm FB --lrecl 500 --codepage cp277 \
  --output s3://my-data-lake/mainframe/customers.parquet
```

### YAML Pipeline with S3 Output

```yaml
steps:
  - name: extract-to-s3
    action: convert
    input:
      dataset: CUST.DAT
      record_format: FB
      lrecl: 500
      codepage: cp277
    copybook: ./CUSTMAST.cpy
    output:
      - type: parquet
        path: s3://my-data-lake/mainframe/customers.parquet
        storage_options:
          key: ${AWS_ACCESS_KEY_ID}
          secret: ${AWS_SECRET_ACCESS_KEY}
          client_kwargs:
            region_name: eu-west-1
```

### Using IAM Roles (EC2 / ECS / Lambda)

When running on AWS infrastructure with an IAM role attached, no credentials are needed — s3fs picks them up automatically:

```bash
ztract convert \
  --copybook CUSTMAST.cpy \
  --input CUST.DAT \
  --recfm FB --lrecl 500 --codepage cp277 \
  --output s3://my-bucket/output.parquet
```

---

## Azure Blob Storage

### Credentials via Connection String

```bash
export AZURE_STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=https;AccountName=..."

ztract convert \
  --copybook CUSTMAST.cpy \
  --input CUST.DAT \
  --recfm FB --lrecl 500 --codepage cp277 \
  --output az://raw-data/mainframe/customers.csv
```

### YAML Pipeline with Azure Output

```yaml
output:
  - type: csv
    path: abfs://container@storageaccount.dfs.core.windows.net/mainframe/customers.csv
    storage_options:
      connection_string: ${AZURE_STORAGE_CONNECTION_STRING}
```

---

## Google Cloud Storage

### Credentials via Service Account

```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json

ztract convert \
  --copybook CUSTMAST.cpy \
  --input CUST.DAT \
  --recfm FB --lrecl 500 --codepage cp277 \
  --output gs://my-bucket/mainframe/customers.jsonl
```

### YAML Pipeline with GCS Output

```yaml
output:
  - type: jsonl
    path: gs://my-bucket/mainframe/customers.jsonl
    storage_options:
      token: ${GOOGLE_APPLICATION_CREDENTIALS}
```

---

## Security Best Practices

!!! warning "Never hardcode credentials"
    Always use environment variables or IAM roles for cloud credentials.

- **Use IAM roles** where possible (no keys at all) — works on EC2, ECS, Lambda, GKE, Azure Managed Identity
- **Use `${ENV_VAR}`** in YAML — Ztract interpolates environment variables before parsing
- **`.env` files** are auto-loaded by Ztract and gitignored by `ztract init`
- **`storage_options`** in YAML are passed directly to fsspec — see provider docs for all options
- **Rotate keys regularly** and use least-privilege IAM policies

---

## Multiple Cloud Outputs

Write to multiple cloud targets in one pass:

```bash
ztract convert \
  --copybook CUSTMAST.cpy \
  --input CUST.DAT \
  --recfm FB --lrecl 500 --codepage cp277 \
  --output s3://data-lake/customers.parquet \
  --output az://archive/customers.csv \
  --output ./local-backup/customers.jsonl
```

All three targets are written concurrently from a single decode pass via the fan-out queue.
