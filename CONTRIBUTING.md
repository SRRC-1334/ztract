# Contributing to Ztract

There are three types of contributors, each with a different setup path. Find yours below.

---

## Section A: Python Development Setup (most contributors)

For contributors working on Python code — adding features, fixing bugs, extending codepages, writers, or connectors.

### Requirements

- Python 3.10+
- JRE 11+ — download from [Adoptium](https://adoptium.net)
- Git

### Setup

```bash
git clone https://github.com/SRRC-1334/ztract.git
cd ztract
make install-dev    # or: pip install -e ".[dev]"
pytest tests/ -v    # 428 tests, all should pass
ztract --version    # ztract, version 0.1.0
```

The Java engine JAR (`ztract/engine/ztract-engine.jar`) is committed to the repo — no Maven or JDK needed. It changes rarely (only on Cobrix version bumps or wrapper fixes).

---

## Section B: Java Engine Development (rare)

For contributors modifying the Java wrapper around the Cobrix cobol-parser.

### Requirements

Everything from Section A, plus:

- JDK 11+
- Maven 3.8+

### Setup

```bash
# After cloning and installing Python deps (Section A):
make rebuild-jar    # builds JAR from source and copies to ztract/engine/
pytest tests/ -v -m integration   # test with real JAR
```

### When to rebuild

- Cobrix releases a new version → update `engine-java/pom.xml` version → `make rebuild-jar`
- Bug fix in wrapper code → edit Java source → `make rebuild-jar`
- Normal Python development → **never** needs JAR rebuild

After rebuilding, commit the updated JAR along with any source changes.

---

## Section C: Building from Source (end users)

For users who want to install from the git repo instead of PyPI.

### Requirements

- Python 3.10+
- JRE 11+ — download from [Adoptium](https://adoptium.net)

### Setup

```bash
git clone https://github.com/SRRC-1334/ztract.git
cd ztract
pip install .
ztract --version
```

Identical to `pip install ztract` from PyPI. The JAR is committed to git, so build-from-source just works.

---

## Development workflow

### TDD approach

Write tests before writing implementation code. Tests are the specification.

### Before committing

```bash
ruff check ztract/ tests/
mypy ztract/ --ignore-missing-imports
```

Or via make:

```bash
make lint
make typecheck
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
engine-java/      # Java source for the engine JAR (Maven project)
```

---

## The Java engine JAR

`ztract/engine/ztract-engine.jar` is committed to git and ships inside the Python wheel.

**Why it's committed:**

- It is ~5–10 MB and changes rarely (only on Cobrix version bumps or wrapper fixes).
- Without it, `pip install` from source would require Maven and JDK — a heavy burden for Python contributors and CI.
- Committing it means `pip install .` and `pip install ztract` work identically.

The JAR wraps [Cobrix](https://github.com/AbsaOSS/cobrix) cobol-parser (Apache 2.0), which handles all COBOL/EBCDIC parsing operations. Python communicates with it via subprocess, exchanging JSON Lines over stdin/stdout. The JVM handles cross-platform differences — the same JAR runs on Windows, Linux, and macOS.

---

## Extension guides

### Adding new codepages

1. Edit `ztract/codepages.py` — add one dict entry.
2. Add tests in `tests/test_codepages.py`.

### Adding new output writers

1. Create `ztract/writers/your_writer.py` extending the `Writer` ABC.
2. Add tests in `tests/writers/`.
3. Register the writer in `ztract/cli/convert.py`.

### Adding new connectors

1. Create `ztract/connectors/your_connector.py` extending the `Connector` ABC.
2. Add tests with mocked I/O.

---

## Test data policy

- All test data is generated via `ztract generate` with `--seed 42`.
- No real mainframe data is ever committed to this repository.
- Sample copybook: `tests/test_data/CUSTMAST.cpy`

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
