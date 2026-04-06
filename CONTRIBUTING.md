# Contributing to Ztract

## Quick overview

There are two types of contributors with different requirements:

- **Python contributors** — need Python 3.10+, JRE 11+, and Git. The pre-built `ztract-engine.jar` is committed to the repo; no Maven or JDK needed.
- **Maintainers rebuilding the Java engine** — additionally need JDK 11+ and Maven 3.8+. This is only necessary when updating the Cobrix version or fixing the Java wrapper.

---

## Getting started (Python contributors)

### Prerequisites

- Python 3.10+
- JRE 11+ — download from [Adoptium](https://adoptium.net/)
- Git

### Setup

```bash
git clone https://github.com/<your-fork>/ztract.git
cd ztract
pip install -e ".[dev]"
```

### Verify your setup

```bash
pytest tests/ -v        # 428 tests should pass
ztract --version
```

---

## Development workflow

### TDD approach

Write tests before writing implementation code. Tests are the specification.

### Before committing

```bash
ruff check ztract/ tests/
mypy ztract/ --ignore-missing-imports
```

### Test markers

- Integration tests (`@pytest.mark.integration`) require JRE and the real JAR.
- Unit tests mock the Java subprocess — no JAR needed to run them.

---

## Project structure

```
ztract/
  cli/            # Click commands (convert, generate, diff, etc.)
  engine/         # Java subprocess wrapper + ztract-engine.jar
  connectors/     # Source connectors (local, S3, etc.)
  writers/        # Output writers (CSV, Parquet, JSON, etc.)
  pipeline/       # Orchestration and batching
  diff/           # Dataset diff functionality
  generate/       # Test data generation
  observability/  # Logging and metrics
```

---

## The Java engine JAR

`ztract/engine/ztract-engine.jar` is committed to git and ships inside the Python wheel.

- **Contributors do not need to rebuild it.**
- It is a fat JAR wrapping [Cobrix](https://github.com/AbsaOSS/cobrix) cobol-parser (Apache 2.0), which handles all COBOL/EBCDIC parsing operations.
- Python communicates with it via subprocess, exchanging JSON Lines over stdin/stdout.
- The JAR runs on Windows, Linux, and macOS — the JVM handles cross-platform differences.

---

## Rebuilding the Java engine (maintainers only)

Only necessary when:
- Updating the Cobrix dependency version
- Fixing a bug in the Java wrapper code

### Requirements

- JDK 11+
- Maven 3.8+

### Steps

```bash
cd engine-java
mvn package -q
cp target/ztract-engine-0.1.0.jar ../ztract/engine/ztract-engine.jar
```

### After rebuilding

```bash
pytest tests/ -v -m integration
```

Then commit the updated JAR.

---

## Adding new codepages

1. Edit `ztract/codepages.py` — add one dict entry.
2. Add tests in `tests/test_codepages.py`.

## Adding new output writers

1. Create `ztract/writers/your_writer.py` extending the `Writer` ABC.
2. Add tests in `tests/writers/`.
3. Register the writer in `ztract/cli/convert.py`.

## Adding new connectors

1. Create `ztract/connectors/your_connector.py` extending the `Connector` ABC.
2. Add tests with mocked I/O.

---

## Test data

- All test data is generated via `ztract generate` with `--seed 42`.
- No real mainframe data is ever committed to this repository.
- Sample copybook: `tests/test_data/CUSTMAST.cpy`

## Copybook contributions

Anonymised or synthetic copybooks for common mainframe layouts are welcome. Place them in `copybooks/` — they are tested automatically via `ztract generate`.

---

## Code style

- Linting: Ruff (config in `pyproject.toml`)
- Line length: 100
- Python 3.10+ features are fine (`match`/`case`, `X | Y` unions)
- No docstrings required on private methods
- Follow existing patterns in the codebase

---

## License

Ztract is Apache 2.0. All contributions must be Apache 2.0 compatible. Check `NOTICE` for attribution requirements before adding dependencies.
