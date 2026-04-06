# Installation

## Prerequisites

### Python 3.10+

Ztract requires Python 3.10 or later. Check your version:

```bash
python --version
```

Download from [python.org](https://www.python.org/downloads/) if needed.

### Java JRE 11+

Ztract uses [Cobrix](https://github.com/AbsaOSS/cobrix) under the hood for COBOL parsing. Cobrix runs as a bundled JAR subprocess, so a JRE must be on your PATH.

Check your Java version:

```bash
java -version
```

If Java is missing or older than 11, download a free JRE from [Adoptium](https://adoptium.net). The Temurin LTS builds (17 or 21) work well.

!!! note
    The JDK is not required — only the JRE. You do not need Maven, Gradle, or any build tooling.

---

## Installing Ztract

```bash
pip install ztract
```

To install the current development preview explicitly:

```bash
pip install ztract==0.1.0.dev1
```

The bundled Cobrix engine JAR is included in the wheel — no separate download needed.

---

## Optional Database Drivers

Ztract can write directly to relational databases. The database drivers are optional extras so that users who don't need them aren't burdened with the dependencies.

```bash
pip install ztract[postgres]    # PostgreSQL via psycopg2
pip install ztract[mysql-mit]   # MySQL via PyMySQL
pip install ztract[mssql]       # SQL Server via pyodbc
pip install ztract[all-db]      # All three drivers at once
```

Once installed, pass a connection URL as an `--output` target:

```bash
ztract convert \
  --copybook CUSTMAST.cpy \
  --input    CUST.MASTER.DAT \
  --recfm    FB --lrecl 500 \
  --codepage cp277 \
  --output   "postgresql://user:pass@localhost/dwh?table=customers"
```

---

## Verify the Installation

```bash
ztract --version
```

Expected output:

```
ztract, version 0.1.0.dev1
```

```bash
java -version
```

Expected output (exact version will vary):

```
openjdk version "21.0.2" 2024-01-16
OpenJDK Runtime Environment Temurin-21.0.2+13 (build 21.0.2+13)
OpenJDK 64-Bit Server VM Temurin-21.0.2+13 (build 21.0.2+13, mixed mode, sharing)
```

---

## Development Install

To contribute to Ztract or run the test suite:

```bash
git clone https://github.com/SRRC-1334/ztract.git
cd ztract
pip install -e ".[dev]"
```

The `[dev]` extra installs pytest, ruff, mypy, and other development dependencies. Run the tests to confirm everything works:

```bash
pytest tests/ -v
```

You should see 428 tests pass. The Java engine JAR is committed to the repository at `ztract/engine/ztract-engine.jar` — no Maven or JDK needed for Python development.

---

## Troubleshooting

### JRE not found

If you see an error like `FileNotFoundError: [Errno 2] No such file or directory: 'java'`, Java is not on your PATH.

**Fix:** Install a JRE from [Adoptium](https://adoptium.net) and make sure the `java` binary is accessible from your terminal. On Windows, you may need to restart your terminal after installation.

Verify with:

```bash
java -version
```

### JAR missing or corrupt

If you see an error referencing `ztract-engine.jar`, the wheel may not have installed cleanly.

**Fix:** Reinstall Ztract:

```bash
pip uninstall ztract
pip install ztract
```

If you are running from a development clone, check that the JAR exists at `ztract/engine/ztract-engine.jar`.

### Permission errors on Linux/macOS

If `pip install` fails with a permissions error, use the `--user` flag:

```bash
pip install --user ztract
```

Or use a virtual environment (recommended):

```bash
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install ztract
```
