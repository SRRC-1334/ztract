# YAML Pipelines

YAML pipelines let you define multi-step extract/transform/load workflows in a single file and run them with `ztract run`. This is the recommended approach for production jobs, scheduled extractions, and anything requiring more than one step.

---

## File Structure

A pipeline file has four top-level sections:

```yaml
version: "1.0"

job:
  name: my-pipeline
  description: Optional description
  continue_on_error: false

connections:
  # Named connection definitions

steps:
  # Ordered list of steps
```

---

## version

The pipeline format version. Currently `"1.0"`. This field is required and enables forward-compatible parsing as the format evolves.

---

## job

General metadata for the pipeline run.

| Field | Required | Description |
|---|---|---|
| `name` | Yes | Job name; appears in audit logs and `ztract status` output |
| `description` | No | Human-readable description of what the pipeline does |
| `continue_on_error` | No | If `true`, continue executing subsequent steps when a step fails (default: `false`) |

---

## connections

Named connection definitions. Define each connection once and reference it in multiple steps using YAML anchors.

```yaml
connections:
  prod: &prod
    type: ftp
    host: mf01.bank.com
    user: ${PROD_USER}
    password: ${PROD_PASS}
    transfer_mode: binary

  staging: &staging
    type: sftp
    host: mf01-staging.bank.com
    user: ${STAGING_USER}
    key_path: ~/.ssh/id_rsa
```

The `&name` syntax creates a YAML anchor. Use `*name` to reference it in a step. See [Connectors](connectors.md) for all connection type options.

---

## steps

An ordered list of steps. Steps run sequentially by default. Each step has an `action` that determines what it does.

### Action Types

| Action | Description |
|---|---|
| `convert` | Extract EBCDIC and write to output targets |
| `diff` | Field-level comparison of two EBCDIC files |
| `generate` | Generate synthetic EBCDIC test data |
| `upload` | Upload a local file to a mainframe dataset |

### convert step

```yaml
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
      - type: parquet
        path: ./output/prod_customers.parquet
    expose_as: prod_data
```

The `expose_as` field makes this step's output available for reference by later steps.

### diff step

```yaml
steps:
  - name: compare-months
    action: diff
    input:
      before: ./archive/CUST_LAST.DAT
      after:  $ref:prod_data.csv
    copybook: ./copybooks/CUSTMAST.cpy
    diff:
      key_fields: [CUST-ID]
      record_format: FB
      lrecl: 500
      codepage: cp277
    output:
      - type: console
      - type: csv
        path: ./output/monthly_changes.csv
```

### upload step

```yaml
steps:
  - name: push-report
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

---

## Step References

Use `$ref:step_name.output_type` to reference the output of a previous step as the input of the current step:

```yaml
after: $ref:prod_data.csv
```

This avoids hardcoding intermediate file paths and makes pipelines more portable.

---

## Environment Variable Interpolation

Use `${VAR_NAME}` anywhere in the YAML to substitute the value of an environment variable at runtime:

```yaml
connections:
  prod:
    host: ${PROD_HOST}
    user: ${PROD_USER}
    password: ${PROD_PASS}
```

If a referenced variable is not set, Ztract raises an error before executing any steps. This prevents silent failures from missing credentials.

### .env File Support

If a `.env` file exists in the working directory (or the directory containing the pipeline YAML), Ztract loads it automatically before resolving variable references. This avoids having to export variables manually in development:

```bash
# .env
PROD_HOST=mf01.bank.com
PROD_USER=myuser
PROD_PASS=mysecret
```

!!! note
    Never commit `.env` files containing real credentials to source control. Add `.env` to your `.gitignore`.

---

## Running a Pipeline

```bash
# Run all steps
ztract run monthly-reconciliation.yaml

# Validate syntax and connections without executing
ztract run monthly-reconciliation.yaml --dry-run

# Run a single named step (useful for re-running a failed step)
ztract run monthly-reconciliation.yaml --step extract-prod
```

`--dry-run` validates the entire pipeline — YAML syntax, connection reachability, copybook paths, and step references — without reading or writing any data.

---

## Full Example: Monthly Reconciliation Pipeline

```yaml
# monthly-reconciliation.yaml
version: "1.0"

job:
  name: customer-monthly-reconciliation
  description: >
    Pull customer master from production mainframe,
    diff against last month's snapshot, push change report back.
  continue_on_error: false

connections:
  prod: &prod
    type: ftp
    host: ${PROD_HOST}
    user: ${PROD_USER}
    password: ${PROD_PASS}
    transfer_mode: binary
    ftp_mode: passive

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
      record_format: FB
      lrecl: 500
      codepage: cp277
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

Run it:

```bash
ztract run monthly-reconciliation.yaml
```

Check the audit log afterwards:

```bash
ztract status --job customer-monthly-reconciliation --last 5
```
