# Ztract Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Python CLI tool that extracts mainframe EBCDIC binary files using COBOL copybooks via the Cobrix cobol-parser Java engine, with bidirectional connectivity (local/FTP/SFTP/Zowe), multiple output formats, field-level diff, and mock data generation.

**Architecture:** Python 3.10+ orchestrates a Java subprocess (Cobrix cobol-parser fat JAR) via JSON Lines over stdin/stdout. Python handles connectivity, output writing, pipeline orchestration, and observability. The Java engine is a black box for all COBOL binary operations.

**Tech Stack:** Python 3.10+, Click (CLI), PyYAML, pyarrow (Parquet), SQLAlchemy (DB), paramiko (SFTP), Faker (mock data), rich (console), daff (diff), Java 11+ (Cobrix engine)

**Spec:** `docs/superpowers/specs/2026-04-06-ztract-design.md`

---

## Phase Overview

| Phase | What | Outcome |
|-------|------|---------|
| 1 | Project scaffolding + packaging | `pip install -e .` works, `ztract --version` runs |
| 2 | Codepages + dataset formats | Shared modules used by all later phases |
| 3 | Java engine bridge | `ZtractBridge` can call all 4 Java modes |
| 4 | Observability | Structured logging, audit trail, reject handler, progress |
| 5 | Writers (CSV, JSONL, Parquet, DB) | All output formats working with fan-out |
| 6 | Local connector + convert CLI | `ztract convert` works end-to-end with local files |
| 7 | Remote connectors (FTP, SFTP, Zowe) | Bidirectional mainframe connectivity |
| 8 | YAML config + pipeline orchestrator | `ztract run job.yaml` with multi-step pipelines |
| 9 | Diff | `ztract diff` with daff + REDEFINES hex diff |
| 10 | Mock data generator + EBCDIC writer | `ztract generate` produces synthetic EBCDIC files |
| 11 | CLI extras (inspect, validate, init, status) | Remaining CLI commands |

---

## Phase 1: Project Scaffolding + Packaging

### Task 1.1: Create pyproject.toml and package structure

**Files:**
- Create: `pyproject.toml`
- Create: `ztract/__init__.py`
- Create: `ztract/__main__.py`
- Create: `ztract/cli/__init__.py`
- Create: `ztract/cli/root.py`

- [ ] **Step 1: Create pyproject.toml**

```toml
[build-system]
requires = ["setuptools>=68.0", "wheel"]
build-backend = "setuptools.backends._legacy:_Backend"

[project]
name = "ztract"
version = "0.1.0"
description = "Extract mainframe EBCDIC data using COBOL copybooks. Zero MIPS."
readme = "README.md"
license = {text = "Apache-2.0"}
requires-python = ">=3.10"
authors = [
    {name = "SRRC-1334"}
]
classifiers = [
    "Development Status :: 3 - Alpha",
    "Intended Audience :: Developers",
    "License :: OSI Approved :: Apache Software License",
    "Programming Language :: Python :: 3",
    "Programming Language :: Python :: 3.10",
    "Programming Language :: Python :: 3.11",
    "Programming Language :: Python :: 3.12",
    "Programming Language :: Python :: 3.13",
    "Topic :: Database",
    "Topic :: Utilities",
]
dependencies = [
    "click>=8.1",
    "pyyaml>=6.0",
    "pyarrow>=14.0",
    "sqlalchemy>=2.0",
    "paramiko>=3.0",
    "faker>=20.0",
    "rich>=13.0",
    "daff>=1.3",
    "multidiff>=0.4",
]

[project.optional-dependencies]
postgres = ["psycopg2-binary>=2.9"]
mysql-mit = ["pymysql>=1.1"]
mysql-gpl = ["mysql-connector-python>=8.0"]
mssql = ["pyodbc>=5.0"]
all-db = ["psycopg2-binary>=2.9", "pymysql>=1.1", "pyodbc>=5.0"]
dev = [
    "pytest>=7.0",
    "pytest-cov>=4.0",
    "ruff>=0.1",
    "mypy>=1.0",
]

[project.scripts]
ztract = "ztract.cli.root:cli"

[project.urls]
Homepage = "https://github.com/SRRC-1334/ztract"
Repository = "https://github.com/SRRC-1334/ztract"
Issues = "https://github.com/SRRC-1334/ztract/issues"

[tool.setuptools.packages.find]
include = ["ztract*"]

[tool.setuptools.package-data]
"ztract.engine" = ["*.jar"]

[tool.ruff]
target-version = "py310"
line-length = 100

[tool.ruff.lint]
select = ["E", "F", "I", "N", "W", "UP"]

[tool.pytest.ini_options]
testpaths = ["tests"]
markers = [
    "integration: requires JRE and real Java engine",
]

[tool.mypy]
python_version = "3.10"
warn_return_any = true
warn_unused_configs = true
```

- [ ] **Step 2: Create package init**

`ztract/__init__.py`:
```python
"""Ztract — Extract mainframe EBCDIC data using COBOL copybooks."""

__version__ = "0.1.0"
```

- [ ] **Step 3: Create __main__.py**

`ztract/__main__.py`:
```python
"""Allow running ztract as `python -m ztract`."""

from ztract.cli.root import cli

cli()
```

- [ ] **Step 4: Create CLI root with --version**

`ztract/cli/__init__.py`:
```python
```

`ztract/cli/root.py`:
```python
"""Ztract CLI root group."""

import click

from ztract import __version__


@click.group()
@click.version_option(version=__version__, prog_name="ztract")
@click.option("--debug", is_flag=True, help="Enable debug logging.")
@click.option("--quiet", is_flag=True, help="Suppress progress output.")
@click.pass_context
def cli(ctx: click.Context, debug: bool, quiet: bool) -> None:
    """Extract mainframe EBCDIC data using COBOL copybooks. Zero MIPS."""
    ctx.ensure_object(dict)
    ctx.obj["debug"] = debug
    ctx.obj["quiet"] = quiet
```

- [ ] **Step 5: Create empty subpackage __init__.py files**

Create empty `__init__.py` in each subpackage:
- `ztract/config/__init__.py`
- `ztract/engine/__init__.py`
- `ztract/connectors/__init__.py`
- `ztract/writers/__init__.py`
- `ztract/pipeline/__init__.py`
- `ztract/diff/__init__.py`
- `ztract/generate/__init__.py`
- `ztract/observability/__init__.py`

- [ ] **Step 6: Create tests directory structure**

Create empty `__init__.py` in:
- `tests/__init__.py`
- `tests/connectors/__init__.py`
- `tests/writers/__init__.py`
- `tests/diff/__init__.py`
- `tests/generate/__init__.py`
- `tests/pipeline/__init__.py`
- `tests/engine/__init__.py`

Create `tests/conftest.py`:
```python
"""Shared test fixtures for Ztract."""

from pathlib import Path

import pytest


@pytest.fixture
def test_data_dir() -> Path:
    """Return the path to the test data directory."""
    return Path(__file__).parent / "test_data"


@pytest.fixture
def sample_copybook(test_data_dir: Path) -> Path:
    """Return path to the sample CUSTMAST.cpy copybook."""
    return test_data_dir / "CUSTMAST.cpy"
```

Create `tests/test_data/CUSTMAST.cpy`:
```cobol
       01  CUSTOMER-RECORD.
           05  CUST-ID            PIC 9(10).
           05  CUST-NAME          PIC X(50).
           05  CUST-ADDR          PIC X(80).
           05  CUST-CITY          PIC X(30).
           05  CUST-ZIP           PIC X(10).
           05  CUST-COUNTRY       PIC X(20).
           05  CUST-PHONE         PIC X(20).
           05  CUST-EMAIL         PIC X(60).
           05  CUST-AMT           PIC S9(9)V99 COMP-3.
           05  CUST-DATE          PIC 9(8).
           05  CUST-STATUS        PIC X(1).
           05  FILLER             PIC X(209).
```

- [ ] **Step 7: Install in editable mode and verify**

Run: `pip install -e ".[dev]"`
Expected: Successfully installed ztract

Run: `ztract --version`
Expected: `ztract, version 0.1.0`

Run: `pytest tests/ -v`
Expected: collected 0 items (no tests yet, but no errors)

- [ ] **Step 8: Commit**

```bash
git add pyproject.toml ztract/ tests/
git commit -m "feat: scaffold project structure with CLI entry point"
```

---

## Phase 2: Codepages + Dataset Formats

### Task 2.1: Codepage registry

**Files:**
- Create: `ztract/codepages.py`
- Create: `tests/test_codepages.py`

- [ ] **Step 1: Write failing tests**

`tests/test_codepages.py`:
```python
"""Tests for EBCDIC codepage registry."""

import pytest

from ztract.codepages import resolve_codepage, list_codepages, CodepageError


class TestResolveCodepage:
    def test_canonical_name(self):
        assert resolve_codepage("cp277") == "cp277"

    def test_numeric_alias(self):
        assert resolve_codepage("277") == "cp277"

    def test_friendly_alias_norway(self):
        assert resolve_codepage("norway") == "cp277"

    def test_friendly_alias_norwegian(self):
        assert resolve_codepage("norwegian") == "cp277"

    def test_friendly_alias_danish(self):
        assert resolve_codepage("danish") == "cp277"

    def test_friendly_alias_nordic(self):
        assert resolve_codepage("nordic") == "cp277"

    def test_us_default(self):
        assert resolve_codepage("us") == "cp037"

    def test_default_alias(self):
        assert resolve_codepage("default") == "cp037"

    def test_germany(self):
        assert resolve_codepage("germany") == "cp273"

    def test_case_insensitive(self):
        assert resolve_codepage("NORWAY") == "cp277"
        assert resolve_codepage("Norway") == "cp277"

    def test_unknown_codepage_raises(self):
        with pytest.raises(CodepageError, match="Unknown codepage"):
            resolve_codepage("cp999")

    def test_error_lists_supported(self):
        with pytest.raises(CodepageError, match="cp277"):
            resolve_codepage("bogus")

    def test_open_systems(self):
        assert resolve_codepage("open_systems") == "cp1047"

    def test_cyrillic(self):
        assert resolve_codepage("cyrillic") == "cp1025"

    def test_greek(self):
        assert resolve_codepage("greek") == "cp875"

    def test_eastern_europe(self):
        assert resolve_codepage("eastern_europe") == "cp870"

    def test_thai(self):
        assert resolve_codepage("thai") == "cp838"


class TestListCodepages:
    def test_returns_all_codepages(self):
        pages = list_codepages()
        assert "cp277" in pages
        assert "cp037" in pages
        assert "cp273" in pages

    def test_each_has_aliases(self):
        pages = list_codepages()
        assert "norway" in pages["cp277"]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_codepages.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'ztract.codepages'`

- [ ] **Step 3: Implement codepages module**

`ztract/codepages.py`:
```python
"""EBCDIC codepage registry.

Maps friendly aliases to canonical Cobrix codepage names.
Central place to add new codepage support.
"""


class CodepageError(ValueError):
    """Raised when an unknown codepage is requested."""


# Canonical codepage → list of aliases (all lowercase)
_CODEPAGE_REGISTRY: dict[str, list[str]] = {
    "cp037": ["037", "us", "usa", "canada", "default"],
    "cp277": ["277", "norway", "norwegian", "danish", "denmark", "nordic"],
    "cp273": ["273", "germany", "german", "austria", "switzerland"],
    "cp875": ["875", "greek", "greece"],
    "cp870": ["870", "eastern_europe", "poland", "hungary", "czech"],
    "cp1047": ["1047", "latin1", "open_systems"],
    "cp838": ["838", "thailand", "thai"],
    "cp1025": ["1025", "cyrillic", "russian"],
}

# Build reverse lookup: alias → canonical name
_ALIAS_MAP: dict[str, str] = {}
for _canonical, _aliases in _CODEPAGE_REGISTRY.items():
    _ALIAS_MAP[_canonical] = _canonical
    for _alias in _aliases:
        _ALIAS_MAP[_alias] = _canonical


def resolve_codepage(name: str) -> str:
    """Resolve a codepage name or alias to its canonical Cobrix name.

    Args:
        name: Codepage name, number, or alias (case-insensitive).

    Returns:
        Canonical codepage name (e.g., "cp277").

    Raises:
        CodepageError: If the codepage is not recognized.
    """
    key = name.strip().lower()
    if key in _ALIAS_MAP:
        return _ALIAS_MAP[key]

    supported = ", ".join(sorted(_CODEPAGE_REGISTRY.keys()))
    raise CodepageError(
        f"Unknown codepage '{name}'. Supported codepages: {supported}. "
        f"Use 'ztract inspect --codepages' to see all aliases."
    )


def list_codepages() -> dict[str, list[str]]:
    """Return all supported codepages with their aliases.

    Returns:
        Dict mapping canonical codepage name to list of aliases.
    """
    return dict(_CODEPAGE_REGISTRY)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_codepages.py -v`
Expected: All 18 tests PASS

- [ ] **Step 5: Commit**

```bash
git add ztract/codepages.py tests/test_codepages.py
git commit -m "feat: add EBCDIC codepage registry with alias resolution"
```

### Task 2.2: Dataset format module

**Files:**
- Create: `ztract/connectors/dataset_format.py`
- Create: `tests/connectors/test_dataset_format.py`

- [ ] **Step 1: Write failing tests**

`tests/connectors/test_dataset_format.py`:
```python
"""Tests for dataset record format handling."""

import pytest

from ztract.connectors.dataset_format import (
    RecordFormat,
    validate_record_format,
    requires_lrecl,
    has_asa_byte,
    DatasetFormatError,
)


class TestRecordFormat:
    def test_all_formats_exist(self):
        assert RecordFormat.F.value == "F"
        assert RecordFormat.FB.value == "FB"
        assert RecordFormat.V.value == "V"
        assert RecordFormat.VB.value == "VB"
        assert RecordFormat.FBA.value == "FBA"
        assert RecordFormat.VBA.value == "VBA"

    def test_from_string(self):
        assert RecordFormat("FB") == RecordFormat.FB

    def test_from_string_case_insensitive(self):
        assert RecordFormat.from_str("fb") == RecordFormat.FB
        assert RecordFormat.from_str("Vb") == RecordFormat.VB


class TestValidateRecordFormat:
    def test_fb_requires_lrecl(self):
        validate_record_format(RecordFormat.FB, lrecl=500)  # should not raise

    def test_fb_without_lrecl_raises(self):
        with pytest.raises(DatasetFormatError, match="LRECL is required"):
            validate_record_format(RecordFormat.FB, lrecl=None)

    def test_f_requires_lrecl(self):
        with pytest.raises(DatasetFormatError, match="LRECL is required"):
            validate_record_format(RecordFormat.F, lrecl=None)

    def test_vb_without_lrecl_ok(self):
        validate_record_format(RecordFormat.VB, lrecl=None)  # should not raise

    def test_fba_requires_lrecl(self):
        with pytest.raises(DatasetFormatError, match="LRECL is required"):
            validate_record_format(RecordFormat.FBA, lrecl=None)


class TestRequiresLrecl:
    def test_fixed_formats_require(self):
        assert requires_lrecl(RecordFormat.F) is True
        assert requires_lrecl(RecordFormat.FB) is True
        assert requires_lrecl(RecordFormat.FBA) is True

    def test_variable_formats_optional(self):
        assert requires_lrecl(RecordFormat.V) is False
        assert requires_lrecl(RecordFormat.VB) is False
        assert requires_lrecl(RecordFormat.VBA) is False


class TestHasAsaByte:
    def test_asa_formats(self):
        assert has_asa_byte(RecordFormat.FBA) is True
        assert has_asa_byte(RecordFormat.VBA) is True

    def test_non_asa_formats(self):
        assert has_asa_byte(RecordFormat.F) is False
        assert has_asa_byte(RecordFormat.FB) is False
        assert has_asa_byte(RecordFormat.V) is False
        assert has_asa_byte(RecordFormat.VB) is False
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/connectors/test_dataset_format.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement dataset format module**

`ztract/connectors/dataset_format.py`:
```python
"""Dataset record format handling.

Defines record formats (F, FB, V, VB, FBA, VBA), validation rules,
and ASA carriage control byte constants. Shared by connectors and
the Java engine bridge.
"""

from enum import Enum


class DatasetFormatError(ValueError):
    """Raised for invalid record format configuration."""


class RecordFormat(Enum):
    """IBM mainframe dataset record formats."""

    F = "F"
    FB = "FB"
    V = "V"
    VB = "VB"
    FBA = "FBA"
    VBA = "VBA"

    @classmethod
    def from_str(cls, value: str) -> "RecordFormat":
        """Parse a record format string (case-insensitive).

        Args:
            value: Record format string (e.g., "FB", "fb", "Vb").

        Returns:
            RecordFormat enum member.

        Raises:
            ValueError: If the value is not a valid record format.
        """
        upper = value.strip().upper()
        try:
            return cls(upper)
        except ValueError:
            valid = ", ".join(m.value for m in cls)
            raise DatasetFormatError(
                f"Unknown record format '{value}'. Valid formats: {valid}"
            ) from None


# ASA carriage control byte values (EBCDIC)
ASA_SINGLE_SPACE = 0x40  # ' ' — advance one line
ASA_DOUBLE_SPACE = 0xF0  # '0' — advance two lines
ASA_NEW_PAGE = 0xF1  # '1' — start new page
ASA_OVERPRINT = 0x4E  # '+' — overprint (no advance)

_FIXED_FORMATS = {RecordFormat.F, RecordFormat.FB, RecordFormat.FBA}
_ASA_FORMATS = {RecordFormat.FBA, RecordFormat.VBA}


def requires_lrecl(recfm: RecordFormat) -> bool:
    """Check if a record format requires LRECL specification."""
    return recfm in _FIXED_FORMATS


def has_asa_byte(recfm: RecordFormat) -> bool:
    """Check if a record format has an ASA carriage control byte."""
    return recfm in _ASA_FORMATS


def validate_record_format(recfm: RecordFormat, lrecl: int | None) -> None:
    """Validate record format and LRECL combination.

    Args:
        recfm: Record format.
        lrecl: Logical record length (may be None for variable formats).

    Raises:
        DatasetFormatError: If LRECL is required but not provided.
    """
    if requires_lrecl(recfm) and lrecl is None:
        raise DatasetFormatError(
            f"LRECL is required for record format {recfm.value}. "
            f"Specify --lrecl <length>."
        )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/connectors/test_dataset_format.py -v`
Expected: All 14 tests PASS

- [ ] **Step 5: Commit**

```bash
git add ztract/connectors/dataset_format.py tests/connectors/test_dataset_format.py
git commit -m "feat: add dataset record format enum and validation"
```

---

## Phase 3: Java Engine Bridge

### Task 3.1: JRE version detection

**Files:**
- Create: `ztract/engine/bridge.py`
- Create: `tests/engine/test_bridge.py`

- [ ] **Step 1: Write failing tests for JRE check**

`tests/engine/test_bridge.py`:
```python
"""Tests for the Java engine bridge."""

from pathlib import Path
from unittest.mock import patch, MagicMock
import subprocess

import pytest

from ztract.engine.bridge import ZtractBridge, JREError


@pytest.fixture
def bridge(tmp_path: Path) -> ZtractBridge:
    """Create a bridge with a fake JAR path."""
    jar_path = tmp_path / "ztract-engine.jar"
    jar_path.touch()
    return ZtractBridge(jar_path=jar_path)


class TestCheckJRE:
    def test_valid_jre_17(self, bridge: ZtractBridge):
        mock_result = MagicMock()
        mock_result.stderr = 'openjdk version "17.0.2" 2022-01-18\n'
        with patch("subprocess.run", return_value=mock_result):
            version = bridge.check_jre()
        assert version == "17"

    def test_valid_jre_11(self, bridge: ZtractBridge):
        mock_result = MagicMock()
        mock_result.stderr = 'openjdk version "11.0.15" 2022-04-19\n'
        with patch("subprocess.run", return_value=mock_result):
            version = bridge.check_jre()
        assert version == "11"

    def test_valid_jre_21(self, bridge: ZtractBridge):
        mock_result = MagicMock()
        mock_result.stderr = 'openjdk version "21.0.1" 2023-10-17\n'
        with patch("subprocess.run", return_value=mock_result):
            version = bridge.check_jre()
        assert version == "21"

    def test_jre_8_too_old(self, bridge: ZtractBridge):
        mock_result = MagicMock()
        mock_result.stderr = 'java version "1.8.0_301"\n'
        with patch("subprocess.run", return_value=mock_result):
            with pytest.raises(JREError, match="Java 11 or later"):
                bridge.check_jre()

    def test_jre_not_found(self, bridge: ZtractBridge):
        with patch("subprocess.run", side_effect=FileNotFoundError):
            with pytest.raises(JREError, match="Java not found"):
                bridge.check_jre()

    def test_error_includes_download_link(self, bridge: ZtractBridge):
        with patch("subprocess.run", side_effect=FileNotFoundError):
            with pytest.raises(JREError, match="adoptium.net"):
                bridge.check_jre()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/engine/test_bridge.py::TestCheckJRE -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement JRE check**

`ztract/engine/bridge.py`:
```python
"""Java engine bridge — manages the Cobrix subprocess.

Handles JRE detection, subprocess lifecycle, and streaming
JSON Lines communication between Python and the Java engine.
"""

import json
import logging
import re
import signal
import subprocess
import sys
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path

logger = logging.getLogger(__name__)

ADOPTIUM_URL = "https://adoptium.net"
MIN_JRE_VERSION = 11


class JREError(RuntimeError):
    """Raised when JRE is missing or incompatible."""


class EngineError(RuntimeError):
    """Raised when the Java engine fails."""


@dataclass
class ValidationReport:
    """Results from the validate mode."""

    records_decoded: int = 0
    records_warnings: int = 0
    records_errors: int = 0
    field_stats: dict = field(default_factory=dict)


class ZtractBridge:
    """Bridge between Python and the Java ztract-engine.jar.

    Manages subprocess lifecycle and provides methods for each
    engine mode: decode, encode, schema, validate.
    """

    def __init__(
        self,
        jar_path: Path,
        jvm_max_heap: str = "512m",
        jvm_args: list[str] | None = None,
    ) -> None:
        self.jar_path = Path(jar_path)
        self.jvm_max_heap = jvm_max_heap
        self.jvm_args = jvm_args or []
        self._process: subprocess.Popen | None = None

    def _base_cmd(self) -> list[str]:
        """Build the base JVM command."""
        cmd = [
            "java",
            f"-Xmx{self.jvm_max_heap}",
            "-Dfile.encoding=UTF-8",
        ]
        # Java 17+ supports stdout.encoding
        jre_version = self._cached_jre_version
        if jre_version and int(jre_version) >= 17:
            cmd.append("-Dstdout.encoding=UTF-8")
        cmd.extend(self.jvm_args)
        cmd.extend(["-jar", str(self.jar_path)])
        return cmd

    _cached_jre_version: str | None = None

    def check_jre(self) -> str:
        """Validate JRE 11+ is on PATH and return major version string.

        Returns:
            Major version string (e.g., "17").

        Raises:
            JREError: If Java is not found or version < 11.
        """
        try:
            result = subprocess.run(
                ["java", "-version"],
                capture_output=True,
                text=True,
                timeout=10,
            )
        except FileNotFoundError:
            raise JREError(
                f"Java not found on PATH. Ztract requires Java {MIN_JRE_VERSION} or later. "
                f"Download: {ADOPTIUM_URL}"
            ) from None

        version_output = result.stderr
        major = self._parse_java_version(version_output)

        if major < MIN_JRE_VERSION:
            raise JREError(
                f"Ztract requires Java {MIN_JRE_VERSION} or later. "
                f"Your Java version: {major}. "
                f"Download: {ADOPTIUM_URL}"
            )

        self._cached_jre_version = str(major)
        return str(major)

    @staticmethod
    def _parse_java_version(version_output: str) -> int:
        """Parse major version number from 'java -version' output."""
        # Matches: "17.0.2", "11.0.15", "21.0.1", "1.8.0_301"
        match = re.search(r'"(\d+)(?:\.(\d+))?', version_output)
        if not match:
            raise JREError(
                f"Could not parse Java version from output: {version_output}"
            )
        major = int(match.group(1))
        # Java 8 and earlier: version "1.8.x" → major is 8
        if major == 1 and match.group(2):
            major = int(match.group(2))
        return major

    def _classify_stderr(self, line: str) -> str:
        """Classify a stderr line as FATAL, WARNING, or IGNORE.

        Returns:
            One of: "fatal", "warning", "ignore"
        """
        stripped = line.strip()
        if not stripped:
            return "ignore"

        fatal_patterns = ["Exception in thread", "OutOfMemoryError", "ERROR:"]
        for pattern in fatal_patterns:
            if pattern in stripped:
                return "fatal"

        warning_patterns = ["WARN:"]
        for pattern in warning_patterns:
            if pattern in stripped:
                return "warning"

        return "ignore"

    def get_schema(
        self,
        copybook: Path,
        recfm: str | None = None,
        lrecl: int | None = None,
    ) -> dict:
        """Get copybook schema via --schema-only mode.

        Args:
            copybook: Path to .cpy file.
            recfm: Optional record format for LRECL validation.
            lrecl: Optional logical record length for validation.

        Returns:
            Parsed schema dict with fields, redefines_groups, etc.
        """
        cmd = self._base_cmd()
        cmd.extend(["--copybook", str(copybook), "--schema-only"])
        if recfm:
            cmd.extend(["--recfm", recfm])
        if lrecl is not None:
            cmd.extend(["--lrecl", str(lrecl)])

        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=30
        )

        for line in result.stderr.splitlines():
            level = self._classify_stderr(line)
            if level == "fatal":
                raise EngineError(f"Engine error during schema read: {line}")
            elif level == "warning":
                logger.warning("Engine: %s", line)

        if result.returncode != 0:
            raise EngineError(
                f"Schema read failed (exit {result.returncode}): {result.stderr}"
            )

        return json.loads(result.stdout)

    def decode(
        self,
        copybook: Path,
        input_path: Path,
        recfm: str,
        lrecl: int | None,
        codepage: str,
        encoding: str = "ebcdic",
    ) -> Iterator[dict]:
        """Decode an EBCDIC binary file, yielding records as dicts.

        Streams JSON Lines from Java stdout. Never buffers full file.

        Args:
            copybook: Path to .cpy file.
            input_path: Path to raw binary file.
            recfm: Record format (F, FB, V, VB, FBA, VBA).
            lrecl: Logical record length (required for fixed formats).
            codepage: EBCDIC codepage (e.g., "cp277").
            encoding: "ebcdic" or "ascii".

        Yields:
            Parsed record dicts.
        """
        cmd = self._base_cmd()
        cmd.extend([
            "--copybook", str(copybook),
            "--input", str(input_path),
            "--recfm", recfm,
            "--codepage", codepage,
            "--encoding", encoding,
        ])
        if lrecl is not None:
            cmd.extend(["--lrecl", str(lrecl)])

        self._process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,  # line-buffered
        )

        try:
            for line in self._process.stdout:
                stripped = line.rstrip("\n")
                if stripped:
                    yield json.loads(stripped)

            self._process.wait()

            stderr_output = self._process.stderr.read()
            for stderr_line in stderr_output.splitlines():
                level = self._classify_stderr(stderr_line)
                if level == "fatal":
                    raise EngineError(f"Engine error during decode: {stderr_line}")
                elif level == "warning":
                    logger.warning("Engine: %s", stderr_line)

            if self._process.returncode != 0:
                raise EngineError(
                    f"Decode failed (exit {self._process.returncode}): {stderr_output}"
                )
        finally:
            self._process = None

    def encode(
        self,
        copybook: Path,
        output_path: Path,
        recfm: str,
        lrecl: int | None,
        codepage: str,
        records: Iterator[dict],
    ) -> int:
        """Encode records to EBCDIC binary via Java stdin.

        Args:
            copybook: Path to .cpy file.
            output_path: Path for output binary file.
            recfm: Record format.
            lrecl: Logical record length.
            codepage: EBCDIC codepage.
            records: Iterator of record dicts to encode.

        Returns:
            Number of records written.
        """
        cmd = self._base_cmd()
        cmd.extend([
            "--copybook", str(copybook),
            "--output", str(output_path),
            "--recfm", recfm,
            "--codepage", codepage,
            "--encoding", "ebcdic",
            "--mode", "encode",
        ])
        if lrecl is not None:
            cmd.extend(["--lrecl", str(lrecl)])

        self._process = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        count = 0
        try:
            for record in records:
                self._process.stdin.write(json.dumps(record) + "\n")
                count += 1
            self._process.stdin.close()
            self._process.wait()

            stderr_output = self._process.stderr.read()
            for stderr_line in stderr_output.splitlines():
                level = self._classify_stderr(stderr_line)
                if level == "fatal":
                    raise EngineError(f"Engine error during encode: {stderr_line}")
                elif level == "warning":
                    logger.warning("Engine: %s", stderr_line)

            if self._process.returncode != 0:
                raise EngineError(
                    f"Encode failed (exit {self._process.returncode}): {stderr_output}"
                )
        finally:
            self._process = None

        return count

    def validate(
        self,
        copybook: Path,
        input_path: Path,
        recfm: str,
        lrecl: int | None,
        codepage: str,
        sample: int = 1000,
    ) -> ValidationReport:
        """Validate a file by decoding a sample of records.

        Args:
            copybook: Path to .cpy file.
            input_path: Path to raw binary file.
            recfm: Record format.
            lrecl: Logical record length.
            codepage: EBCDIC codepage.
            sample: Number of records to sample.

        Returns:
            ValidationReport with decode statistics.
        """
        cmd = self._base_cmd()
        cmd.extend([
            "--copybook", str(copybook),
            "--input", str(input_path),
            "--recfm", recfm,
            "--codepage", codepage,
            "--mode", "validate",
            "--sample", str(sample),
        ])
        if lrecl is not None:
            cmd.extend(["--lrecl", str(lrecl)])

        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=120
        )

        if result.returncode != 0:
            raise EngineError(
                f"Validate failed (exit {result.returncode}): {result.stderr}"
            )

        data = json.loads(result.stdout)
        return ValidationReport(
            records_decoded=data.get("records_decoded", 0),
            records_warnings=data.get("records_warnings", 0),
            records_errors=data.get("records_errors", 0),
            field_stats=data.get("field_stats", {}),
        )

    def shutdown(self) -> None:
        """Gracefully shut down any running Java subprocess.

        Sends SIGTERM, waits 5 seconds, then SIGKILL if needed.
        """
        proc = self._process
        if proc is None or proc.poll() is not None:
            return

        logger.info("Shutting down Java engine (PID %d)...", proc.pid)

        if sys.platform == "win32":
            proc.terminate()
        else:
            proc.send_signal(signal.SIGTERM)

        try:
            proc.wait(timeout=5)
            logger.info("Java engine exited cleanly.")
        except subprocess.TimeoutExpired:
            logger.warning("Java engine did not exit in 5s, sending SIGKILL.")
            proc.kill()
            proc.wait()

        self._process = None
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/engine/test_bridge.py::TestCheckJRE -v`
Expected: All 6 tests PASS

- [ ] **Step 5: Commit**

```bash
git add ztract/engine/bridge.py tests/engine/test_bridge.py
git commit -m "feat: add Java engine bridge with JRE detection"
```

### Task 3.2: Bridge schema, decode, encode tests (mocked subprocess)

**Files:**
- Modify: `tests/engine/test_bridge.py`

- [ ] **Step 1: Add schema mode tests**

Append to `tests/engine/test_bridge.py`:
```python
class TestGetSchema:
    def test_returns_parsed_schema(self, bridge: ZtractBridge, tmp_path: Path):
        copybook = tmp_path / "TEST.cpy"
        copybook.write_text("       01 TEST-REC.\n           05 FIELD-A PIC X(10).\n")

        schema_json = json.dumps({
            "copybook": "TEST.cpy",
            "record_length": 10,
            "record_format": "FB",
            "fields": [
                {
                    "name": "FIELD-A",
                    "level": "05",
                    "type": "ALPHANUMERIC",
                    "pic": "X(10)",
                    "usage": "DISPLAY",
                    "offset": 0,
                    "size": 10,
                    "scale": 0,
                    "signed": False,
                    "occurs": None,
                    "redefines": None,
                    "redefines_group": None,
                }
            ],
            "redefines_groups": [],
        })

        mock_result = MagicMock()
        mock_result.stdout = schema_json
        mock_result.stderr = ""
        mock_result.returncode = 0

        with patch("subprocess.run", return_value=mock_result):
            # Pre-set JRE version to avoid check
            bridge._cached_jre_version = "17"
            schema = bridge.get_schema(copybook, recfm="FB", lrecl=10)

        assert schema["record_length"] == 10
        assert schema["fields"][0]["name"] == "FIELD-A"
        assert schema["fields"][0]["type"] == "ALPHANUMERIC"

    def test_engine_error_raises(self, bridge: ZtractBridge, tmp_path: Path):
        copybook = tmp_path / "TEST.cpy"
        copybook.write_text("       01 TEST-REC.\n")

        mock_result = MagicMock()
        mock_result.stdout = ""
        mock_result.stderr = "ERROR: Invalid copybook syntax"
        mock_result.returncode = 1

        with patch("subprocess.run", return_value=mock_result):
            bridge._cached_jre_version = "17"
            with pytest.raises(EngineError, match="Engine error"):
                bridge.get_schema(copybook)


class TestDecode:
    def test_yields_records(self, bridge: ZtractBridge, tmp_path: Path):
        copybook = tmp_path / "TEST.cpy"
        copybook.touch()
        input_file = tmp_path / "TEST.DAT"
        input_file.touch()

        records = [
            '{"FIELD-A": "hello"}',
            '{"FIELD-A": "world"}',
        ]
        stdout_data = "\n".join(records) + "\n"

        mock_proc = MagicMock()
        mock_proc.stdout = iter(stdout_data.splitlines(keepends=True))
        mock_proc.stderr.read.return_value = ""
        mock_proc.wait.return_value = None
        mock_proc.returncode = 0
        mock_proc.poll.return_value = 0

        bridge._cached_jre_version = "17"
        with patch("subprocess.Popen", return_value=mock_proc):
            result = list(bridge.decode(
                copybook, input_file, "FB", 10, "cp037"
            ))

        assert len(result) == 2
        assert result[0]["FIELD-A"] == "hello"
        assert result[1]["FIELD-A"] == "world"


class TestEncode:
    def test_writes_records(self, bridge: ZtractBridge, tmp_path: Path):
        copybook = tmp_path / "TEST.cpy"
        copybook.touch()
        output_file = tmp_path / "TEST.DAT"

        records = [{"FIELD-A": "hello"}, {"FIELD-A": "world"}]

        mock_proc = MagicMock()
        mock_proc.stdin = MagicMock()
        mock_proc.stderr.read.return_value = ""
        mock_proc.wait.return_value = None
        mock_proc.returncode = 0
        mock_proc.poll.return_value = 0

        bridge._cached_jre_version = "17"
        with patch("subprocess.Popen", return_value=mock_proc):
            count = bridge.encode(
                copybook, output_file, "FB", 10, "cp037", iter(records)
            )

        assert count == 2
        assert mock_proc.stdin.write.call_count == 2


class TestStderrClassification:
    def test_fatal_exception(self, bridge: ZtractBridge):
        assert bridge._classify_stderr("Exception in thread main") == "fatal"

    def test_fatal_oom(self, bridge: ZtractBridge):
        assert bridge._classify_stderr("java.lang.OutOfMemoryError") == "fatal"

    def test_fatal_error_prefix(self, bridge: ZtractBridge):
        assert bridge._classify_stderr("ERROR: bad copybook") == "fatal"

    def test_warning_prefix(self, bridge: ZtractBridge):
        assert bridge._classify_stderr("WARN: truncated record") == "warning"

    def test_ignore_empty(self, bridge: ZtractBridge):
        assert bridge._classify_stderr("") == "ignore"

    def test_ignore_jvm_noise(self, bridge: ZtractBridge):
        assert bridge._classify_stderr("Picked up JAVA_TOOL_OPTIONS") == "ignore"
```

Add `import json` to the top of the test file if not already present.

- [ ] **Step 2: Run all bridge tests**

Run: `pytest tests/engine/test_bridge.py -v`
Expected: All 15 tests PASS

- [ ] **Step 3: Commit**

```bash
git add tests/engine/test_bridge.py
git commit -m "test: add bridge schema, decode, encode, stderr tests"
```

---

## Phase 4: Observability

### Task 4.1: Reject handler

**Files:**
- Create: `ztract/observability/rejects.py`
- Create: `tests/test_observability.py`

- [ ] **Step 1: Write failing tests**

`tests/test_observability.py`:
```python
"""Tests for observability modules."""

import json
from pathlib import Path

import pytest

from ztract.observability.rejects import RejectHandler


class TestRejectHandler:
    def test_write_reject(self, tmp_path: Path):
        reject_file = tmp_path / "rejects.jsonl"
        handler = RejectHandler(reject_file)
        handler.open()
        handler.reject(
            record_num=100,
            byte_offset=50000,
            step="extract-prod",
            error_type="DB_CONSTRAINT_VIOLATION",
            error_msg="duplicate key CUST-ID=12345",
            target="postgresql://localhost/test",
            decoded={"CUST-ID": "12345", "CUST-NAME": "Test"},
            raw_hex="C1C2C3",
        )
        handler.close()

        lines = reject_file.read_text().strip().split("\n")
        assert len(lines) == 1
        entry = json.loads(lines[0])
        assert entry["record_num"] == 100
        assert entry["error_type"] == "DB_CONSTRAINT_VIOLATION"
        assert entry["decoded"]["CUST-ID"] == "12345"
        assert entry["raw_hex"] == "C1C2C3"
        assert "timestamp" in entry

    def test_reject_count(self, tmp_path: Path):
        reject_file = tmp_path / "rejects.jsonl"
        handler = RejectHandler(reject_file)
        handler.open()
        handler.reject(record_num=1, byte_offset=0, step="s",
                       error_type="E", error_msg="m", target="t")
        handler.reject(record_num=2, byte_offset=100, step="s",
                       error_type="E", error_msg="m", target="t")
        assert handler.count == 2
        handler.close()

    def test_no_file_created_if_no_rejects(self, tmp_path: Path):
        reject_file = tmp_path / "rejects.jsonl"
        handler = RejectHandler(reject_file)
        handler.open()
        handler.close()
        assert not reject_file.exists()

    def test_context_manager(self, tmp_path: Path):
        reject_file = tmp_path / "rejects.jsonl"
        with RejectHandler(reject_file) as handler:
            handler.reject(record_num=1, byte_offset=0, step="s",
                           error_type="E", error_msg="m", target="t")
        assert reject_file.exists()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_observability.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement reject handler**

`ztract/observability/rejects.py`:
```python
"""Reject file management.

Writes rejected records to JSONL files with full context:
record number, byte offset, error details, decoded data, and raw hex.
"""

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


class RejectHandler:
    """Manages a reject file for a single step/target.

    Usage:
        with RejectHandler(path) as handler:
            handler.reject(record_num=1, ...)
    """

    def __init__(self, file_path: Path) -> None:
        self.file_path = Path(file_path)
        self._file = None
        self._count = 0

    @property
    def count(self) -> int:
        """Number of rejected records."""
        return self._count

    def open(self) -> None:
        """Prepare for writing. File created on first reject, not here."""

    def reject(
        self,
        record_num: int,
        byte_offset: int,
        step: str,
        error_type: str,
        error_msg: str,
        target: str,
        decoded: dict[str, Any] | None = None,
        raw_hex: str | None = None,
    ) -> None:
        """Write a rejected record to the reject file.

        File is created lazily on first reject.
        """
        if self._file is None:
            self.file_path.parent.mkdir(parents=True, exist_ok=True)
            self._file = open(self.file_path, "w", encoding="utf-8")

        entry = {
            "record_num": record_num,
            "byte_offset": byte_offset,
            "step": step,
            "error_type": error_type,
            "error_msg": error_msg,
            "target": target,
            "decoded": decoded,
            "raw_hex": raw_hex,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        self._file.write(json.dumps(entry, ensure_ascii=False) + "\n")
        self._file.flush()
        self._count += 1

    def close(self) -> None:
        """Close the reject file."""
        if self._file is not None:
            self._file.close()
            self._file = None

    def __enter__(self) -> "RejectHandler":
        self.open()
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_observability.py -v`
Expected: All 4 tests PASS

- [ ] **Step 5: Commit**

```bash
git add ztract/observability/rejects.py tests/test_observability.py
git commit -m "feat: add reject handler with lazy file creation"
```

### Task 4.2: Structured logging

**Files:**
- Create: `ztract/observability/logging.py`

- [ ] **Step 1: Write failing tests**

Append to `tests/test_observability.py`:
```python
from ztract.observability.logging import setup_logging, JSONFormatter
import logging


class TestJSONFormatter:
    def test_formats_as_json(self):
        formatter = JSONFormatter()
        record = logging.LogRecord(
            name="ztract", level=logging.INFO, pathname="", lineno=0,
            msg="test message", args=(), exc_info=None,
        )
        record.job = "test-job"
        record.step = "extract"
        output = formatter.format(record)
        parsed = json.loads(output)
        assert parsed["level"] == "INFO"
        assert parsed["message"] == "test message"
        assert "timestamp" in parsed

    def test_includes_extra_fields(self):
        formatter = JSONFormatter()
        record = logging.LogRecord(
            name="ztract", level=logging.INFO, pathname="", lineno=0,
            msg="test", args=(), exc_info=None,
        )
        record.job = "myjob"
        record.step = "mystep"
        record.records_read = 1000
        output = formatter.format(record)
        parsed = json.loads(output)
        assert parsed.get("job") == "myjob"


class TestSetupLogging:
    def test_creates_log_directory(self, tmp_path: Path):
        log_dir = tmp_path / "logs"
        setup_logging(log_dir=log_dir, debug=False)
        assert log_dir.exists()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_observability.py::TestJSONFormatter -v`
Expected: FAIL

- [ ] **Step 3: Implement structured logging**

`ztract/observability/logging.py`:
```python
"""Structured JSON logging for Ztract.

Provides JSON-formatted log output for both file and console.
Log files: ./logs/ztract_YYYY-MM-DD.log (JSON Lines)
Console: rich-formatted condensed output.
"""

import json
import logging
import logging.handlers
from datetime import datetime, timezone
from pathlib import Path


class JSONFormatter(logging.Formatter):
    """Format log records as JSON Lines."""

    # Extra fields to include if present on the record
    _EXTRA_FIELDS = (
        "job", "step", "event", "records_read", "records_written",
        "records_rejected", "elapsed_sec", "throughput_rps",
        "source", "target",
    )

    def format(self, record: logging.LogRecord) -> str:
        entry: dict = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        for field in self._EXTRA_FIELDS:
            value = getattr(record, field, None)
            if value is not None:
                entry[field] = value

        if record.exc_info and record.exc_info[1]:
            entry["exception"] = self.formatException(record.exc_info)

        return json.dumps(entry, ensure_ascii=False)


def setup_logging(
    log_dir: Path,
    debug: bool = False,
    quiet: bool = False,
) -> None:
    """Configure Ztract logging.

    Args:
        log_dir: Directory for log files.
        debug: Enable DEBUG level.
        quiet: Suppress console output except errors.
    """
    log_dir = Path(log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)

    root = logging.getLogger("ztract")
    root.setLevel(logging.DEBUG if debug else logging.INFO)
    root.handlers.clear()

    # File handler: JSON Lines, daily rotation
    log_file = log_dir / f"ztract_{datetime.now().strftime('%Y-%m-%d')}.log"
    file_handler = logging.handlers.TimedRotatingFileHandler(
        log_file, when="midnight", backupCount=30, encoding="utf-8"
    )
    file_handler.setFormatter(JSONFormatter())
    file_handler.setLevel(logging.DEBUG)
    root.addHandler(file_handler)

    # Console handler: simple format
    if not quiet:
        console = logging.StreamHandler()
        console.setLevel(logging.DEBUG if debug else logging.INFO)
        console.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
        root.addHandler(console)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_observability.py -v`
Expected: All 7 tests PASS

- [ ] **Step 5: Commit**

```bash
git add ztract/observability/logging.py tests/test_observability.py
git commit -m "feat: add structured JSON logging with daily rotation"
```

### Task 4.3: Audit trail

**Files:**
- Create: `ztract/observability/audit.py`

- [ ] **Step 1: Write failing tests**

Append to `tests/test_observability.py`:
```python
from ztract.observability.audit import AuditWriter, AuditEntry, StepAudit


class TestAuditWriter:
    def test_write_audit_entry(self, tmp_path: Path):
        audit_file = tmp_path / "audit" / "ztract_audit.log"
        writer = AuditWriter(audit_file)

        entry = AuditEntry(
            job_file="test-job.yaml",
            ztract_version="0.1.0",
            jre_version="17",
        )
        entry.add_step(StepAudit(
            step="extract-prod",
            action="convert",
            source="local://CUST.DAT",
            targets=["customers.csv"],
            records_read=1000,
            records_written=998,
            records_rejected=2,
            status="PARTIAL_SUCCESS",
        ))
        entry.overall_status = "PARTIAL_SUCCESS"
        entry.exit_code = 2

        writer.write(entry)

        lines = audit_file.read_text().strip().split("\n")
        assert len(lines) == 1
        parsed = json.loads(lines[0])
        assert parsed["job_file"] == "test-job.yaml"
        assert parsed["overall_status"] == "PARTIAL_SUCCESS"
        assert parsed["exit_code"] == 2
        assert len(parsed["steps"]) == 1
        assert parsed["steps"][0]["records_read"] == 1000
        assert "audit_id" in parsed
        assert "timestamp_start" in parsed

    def test_appends_to_existing(self, tmp_path: Path):
        audit_file = tmp_path / "audit" / "ztract_audit.log"
        writer = AuditWriter(audit_file)

        for i in range(3):
            entry = AuditEntry(
                job_file=f"job-{i}.yaml",
                ztract_version="0.1.0",
            )
            entry.overall_status = "SUCCESS"
            entry.exit_code = 0
            writer.write(entry)

        lines = audit_file.read_text().strip().split("\n")
        assert len(lines) == 3
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_observability.py::TestAuditWriter -v`
Expected: FAIL

- [ ] **Step 3: Implement audit trail**

`ztract/observability/audit.py`:
```python
"""Immutable audit trail for Ztract job executions.

Append-only JSON Lines file. One entry per job execution.
Cannot be disabled — always on. Never rotated by Ztract.
"""

import getpass
import json
import platform
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from ztract import __version__


@dataclass
class StepAudit:
    """Audit data for a single pipeline step."""

    step: str
    action: str
    source: str = ""
    targets: list[str] = field(default_factory=list)
    records_read: int = 0
    records_written: int = 0
    records_rejected: int = 0
    reject_file: str = ""
    status: str = "PENDING"

    def to_dict(self) -> dict:
        return {
            "step": self.step,
            "action": self.action,
            "source": self.source,
            "target": self.targets,
            "records_read": self.records_read,
            "records_written": self.records_written,
            "records_rejected": self.records_rejected,
            "reject_file": self.reject_file,
            "status": self.status,
        }


@dataclass
class AuditEntry:
    """Audit data for a complete job execution."""

    job_file: str
    ztract_version: str = __version__
    jre_version: str = ""
    job_file_hash: str = ""
    steps: list[StepAudit] = field(default_factory=list)
    overall_status: str = "PENDING"
    exit_code: int = 0
    timestamp_start: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    def add_step(self, step: StepAudit) -> None:
        self.steps.append(step)

    def to_dict(self) -> dict:
        return {
            "audit_id": str(uuid.uuid4()),
            "timestamp_start": self.timestamp_start,
            "timestamp_end": datetime.now(timezone.utc).isoformat(),
            "user": getpass.getuser(),
            "machine": platform.node(),
            "ztract_version": self.ztract_version,
            "jre_version": self.jre_version,
            "job_file": self.job_file,
            "job_file_hash": self.job_file_hash,
            "steps": [s.to_dict() for s in self.steps],
            "overall_status": self.overall_status,
            "exit_code": self.exit_code,
        }


class AuditWriter:
    """Writes audit entries to the append-only audit log."""

    def __init__(self, audit_file: Path) -> None:
        self.audit_file = Path(audit_file)

    def write(self, entry: AuditEntry) -> None:
        """Append an audit entry to the log file.

        Creates parent directories if needed. Always appends.
        """
        self.audit_file.parent.mkdir(parents=True, exist_ok=True)
        with open(self.audit_file, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry.to_dict(), ensure_ascii=False) + "\n")
            f.flush()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_observability.py::TestAuditWriter -v`
Expected: All 2 tests PASS

- [ ] **Step 5: Commit**

```bash
git add ztract/observability/audit.py tests/test_observability.py
git commit -m "feat: add immutable append-only audit trail"
```

### Task 4.4: Progress display

**Files:**
- Create: `ztract/observability/progress.py`

- [ ] **Step 1: Write failing tests**

Append to `tests/test_observability.py`:
```python
from ztract.observability.progress import ProgressTracker


class TestProgressTracker:
    def test_create_and_update(self):
        tracker = ProgressTracker(quiet=True)
        task_id = tracker.add_step("extract-prod", total=1000)
        tracker.update(task_id, advance=500)
        assert tracker.get_count(task_id) == 500

    def test_quiet_mode_no_crash(self):
        tracker = ProgressTracker(quiet=True)
        task_id = tracker.add_step("test", total=100)
        tracker.update(task_id, advance=100)
        tracker.finish()

    def test_multiple_steps(self):
        tracker = ProgressTracker(quiet=True)
        t1 = tracker.add_step("step-1", total=100)
        t2 = tracker.add_step("step-2", total=200)
        tracker.update(t1, advance=50)
        tracker.update(t2, advance=200)
        assert tracker.get_count(t1) == 50
        assert tracker.get_count(t2) == 200
        tracker.finish()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_observability.py::TestProgressTracker -v`
Expected: FAIL

- [ ] **Step 3: Implement progress tracker**

`ztract/observability/progress.py`:
```python
"""Progress display using rich.

Shows per-step progress bars with records/sec, ETA, elapsed.
Auto-suppressed in quiet mode or when stdout is not a TTY.
"""

import sys
import time
from typing import Any

from rich.progress import (
    Progress,
    SpinnerColumn,
    BarColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
    MofNCompleteColumn,
    TaskID,
)


class ProgressTracker:
    """Tracks progress across pipeline steps."""

    def __init__(self, quiet: bool = False) -> None:
        self._quiet = quiet
        self._is_tty = sys.stdout.isatty() and not quiet
        self._counts: dict[TaskID, int] = {}

        if self._is_tty:
            self._progress = Progress(
                SpinnerColumn(),
                TextColumn("[bold blue]{task.description}"),
                BarColumn(),
                MofNCompleteColumn(),
                TextColumn("[green]{task.fields[rate]}/s"),
                TimeElapsedColumn(),
                TimeRemainingColumn(),
            )
            self._progress.start()
        else:
            self._progress = None

        self._start_times: dict[TaskID, float] = {}

    def add_step(self, name: str, total: int | None = None) -> TaskID:
        """Add a progress bar for a pipeline step.

        Args:
            name: Step display name.
            total: Total number of records (None if unknown).

        Returns:
            TaskID for updating progress.
        """
        if self._progress:
            task_id = self._progress.add_task(
                name, total=total, rate="0"
            )
        else:
            task_id = TaskID(len(self._counts))

        self._counts[task_id] = 0
        self._start_times[task_id] = time.monotonic()
        return task_id

    def update(self, task_id: TaskID, advance: int = 1) -> None:
        """Update progress for a step.

        Args:
            task_id: Task to update.
            advance: Number of records to advance by.
        """
        self._counts[task_id] = self._counts.get(task_id, 0) + advance

        if self._progress:
            elapsed = time.monotonic() - self._start_times.get(task_id, time.monotonic())
            rate = int(self._counts[task_id] / elapsed) if elapsed > 0 else 0
            self._progress.update(task_id, advance=advance, rate=str(rate))

    def get_count(self, task_id: TaskID) -> int:
        """Get current record count for a step."""
        return self._counts.get(task_id, 0)

    def finish(self) -> None:
        """Stop all progress bars."""
        if self._progress:
            self._progress.stop()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_observability.py::TestProgressTracker -v`
Expected: All 3 tests PASS

- [ ] **Step 5: Commit**

```bash
git add ztract/observability/progress.py tests/test_observability.py
git commit -m "feat: add rich progress tracker with per-step bars"
```

---

## Phase 5: Writers

### Task 5.1: Writer base class + column name sanitization

**Files:**
- Create: `ztract/writers/base.py`
- Create: `tests/writers/test_base.py`

- [ ] **Step 1: Write failing tests**

`tests/writers/test_base.py`:
```python
"""Tests for writer base class and utilities."""

import pytest

from ztract.writers.base import sanitize_column_name, flatten_occurs


class TestSanitizeColumnName:
    def test_hyphen_to_underscore(self):
        assert sanitize_column_name("CUST-ID") == "CUST_ID"

    def test_multiple_hyphens(self):
        assert sanitize_column_name("ACCT-BALANCE-AMT") == "ACCT_BALANCE_AMT"

    def test_no_hyphens_unchanged(self):
        assert sanitize_column_name("CUSTID") == "CUSTID"

    def test_filler(self):
        assert sanitize_column_name("FILLER") == "FILLER"


class TestFlattenOccurs:
    def test_flat_record_unchanged(self):
        record = {"CUST-ID": "123", "CUST-NAME": "Test"}
        schema_fields = [
            {"name": "CUST-ID", "occurs": None},
            {"name": "CUST-NAME", "occurs": None},
        ]
        result = flatten_occurs(record, schema_fields)
        assert result == {"CUST_ID": "123", "CUST_NAME": "Test"}

    def test_occurs_flattened(self):
        record = {
            "CUST-ID": "123",
            "ITEM": [{"CODE": "A", "QTY": 1}, {"CODE": "B", "QTY": 2}],
        }
        schema_fields = [
            {"name": "CUST-ID", "occurs": None},
            {"name": "ITEM", "occurs": 2},
        ]
        result = flatten_occurs(record, schema_fields)
        assert result["CUST_ID"] == "123"
        assert result["ITEM_1_CODE"] == "A"
        assert result["ITEM_1_QTY"] == 1
        assert result["ITEM_2_CODE"] == "B"
        assert result["ITEM_2_QTY"] == 2

    def test_empty_occurs_array(self):
        record = {"CUST-ID": "123", "ITEM": []}
        schema_fields = [
            {"name": "CUST-ID", "occurs": None},
            {"name": "ITEM", "occurs": 3},
        ]
        result = flatten_occurs(record, schema_fields)
        assert result["CUST_ID"] == "123"
        assert "ITEM_1" not in str(result)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/writers/test_base.py -v`
Expected: FAIL

- [ ] **Step 3: Implement base writer**

`ztract/writers/base.py`:
```python
"""Writer base class and shared utilities.

All writers implement the Writer ABC and run as independent
threads in the fan-out architecture.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any


@dataclass
class WriterStats:
    """Statistics returned by a writer on close."""

    records_written: int = 0
    elapsed_sec: float = 0.0
    errors: int = 0


class Writer(ABC):
    """Abstract base class for all output writers."""

    batch_size: int = 1000

    @abstractmethod
    def open(self, schema: dict) -> None:
        """Initialize output. Create table/file headers."""

    @abstractmethod
    def write_batch(self, records: list[dict]) -> int:
        """Write a batch of records. Return count written."""

    @abstractmethod
    def close(self) -> WriterStats:
        """Flush, close, return stats."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Display name for progress."""


def sanitize_column_name(name: str) -> str:
    """Convert COBOL field name to SQL/Parquet/CSV-safe column name.

    COBOL uses hyphens (CUST-ID). SQL/Parquet use underscores (CUST_ID).
    """
    return name.replace("-", "_")


def flatten_occurs(
    record: dict[str, Any],
    schema_fields: list[dict],
) -> dict[str, Any]:
    """Flatten OCCURS array fields for tabular output (CSV, DB).

    COBOL OCCURS produces nested arrays in JSON Lines:
        {"ITEM": [{"CODE":"A"},{"CODE":"B"}]}

    Flattened to:
        {"ITEM_1_CODE": "A", "ITEM_2_CODE": "B"}

    Non-OCCURS fields are passed through with sanitized names.

    Args:
        record: Parsed record dict from Java engine.
        schema_fields: Field definitions from schema.

    Returns:
        Flat dict with sanitized column names.
    """
    result: dict[str, Any] = {}

    for field_def in schema_fields:
        field_name = field_def["name"]
        value = record.get(field_name)

        if field_def.get("occurs") and isinstance(value, list):
            for i, element in enumerate(value, start=1):
                if isinstance(element, dict):
                    for sub_key, sub_val in element.items():
                        flat_key = sanitize_column_name(
                            f"{field_name}_{i}_{sub_key}"
                        )
                        result[flat_key] = sub_val
                else:
                    flat_key = sanitize_column_name(f"{field_name}_{i}")
                    result[flat_key] = element
        else:
            result[sanitize_column_name(field_name)] = value

    return result
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/writers/test_base.py -v`
Expected: All 7 tests PASS

- [ ] **Step 5: Commit**

```bash
git add ztract/writers/base.py tests/writers/test_base.py
git commit -m "feat: add writer base class with column sanitization and OCCURS flattening"
```

### Task 5.2: CSV writer

**Files:**
- Create: `ztract/writers/csv.py`
- Create: `tests/writers/test_csv.py`

- [ ] **Step 1: Write failing tests**

`tests/writers/test_csv.py`:
```python
"""Tests for CSV writer."""

import csv
from pathlib import Path

import pytest

from ztract.writers.csv import CSVWriter


@pytest.fixture
def schema() -> dict:
    return {
        "fields": [
            {"name": "CUST-ID", "occurs": None},
            {"name": "CUST-NAME", "occurs": None},
            {"name": "CUST-AMT", "occurs": None},
        ]
    }


class TestCSVWriter:
    def test_writes_header_and_rows(self, tmp_path: Path, schema: dict):
        output = tmp_path / "out.csv"
        writer = CSVWriter(output)
        writer.open(schema)
        writer.write_batch([
            {"CUST-ID": "001", "CUST-NAME": "Bjorn", "CUST-AMT": "100.00"},
            {"CUST-ID": "002", "CUST-NAME": "Ase", "CUST-AMT": "200.00"},
        ])
        stats = writer.close()

        assert stats.records_written == 2
        rows = list(csv.reader(output.open(encoding="utf-8")))
        assert rows[0] == ["CUST_ID", "CUST_NAME", "CUST_AMT"]
        assert rows[1] == ["001", "Bjorn", "100.00"]

    def test_custom_delimiter(self, tmp_path: Path, schema: dict):
        output = tmp_path / "out.csv"
        writer = CSVWriter(output, delimiter="|")
        writer.open(schema)
        writer.write_batch([
            {"CUST-ID": "001", "CUST-NAME": "Test", "CUST-AMT": "50.00"},
        ])
        writer.close()
        content = output.read_text(encoding="utf-8")
        assert "|" in content

    def test_null_values_empty_by_default(self, tmp_path: Path, schema: dict):
        output = tmp_path / "out.csv"
        writer = CSVWriter(output)
        writer.open(schema)
        writer.write_batch([
            {"CUST-ID": "001", "CUST-NAME": None, "CUST-AMT": "50.00"},
        ])
        writer.close()
        rows = list(csv.reader(output.open(encoding="utf-8")))
        assert rows[1][1] == ""

    def test_null_values_custom(self, tmp_path: Path, schema: dict):
        output = tmp_path / "out.csv"
        writer = CSVWriter(output, null_value="NULL")
        writer.open(schema)
        writer.write_batch([
            {"CUST-ID": "001", "CUST-NAME": None, "CUST-AMT": "50.00"},
        ])
        writer.close()
        rows = list(csv.reader(output.open(encoding="utf-8")))
        assert rows[1][1] == "NULL"

    def test_norwegian_characters(self, tmp_path: Path, schema: dict):
        output = tmp_path / "out.csv"
        writer = CSVWriter(output)
        writer.open(schema)
        writer.write_batch([
            {"CUST-ID": "001", "CUST-NAME": "Bjorn Hansen", "CUST-AMT": "100.00"},
        ])
        writer.close()
        content = output.read_text(encoding="utf-8")
        assert "Bjorn" in content

    def test_writer_name(self, tmp_path: Path):
        writer = CSVWriter(tmp_path / "out.csv")
        assert "csv" in writer.name.lower()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/writers/test_csv.py -v`
Expected: FAIL

- [ ] **Step 3: Implement CSV writer**

`ztract/writers/csv.py`:
```python
"""CSV writer with configurable delimiter and null handling."""

import csv
import time
from pathlib import Path
from typing import Any

from ztract.writers.base import Writer, WriterStats, sanitize_column_name


class CSVWriter(Writer):
    """Writes records to a CSV file.

    Column names are sanitized (CUST-ID -> CUST_ID).
    Null values are represented as empty string by default.
    """

    def __init__(
        self,
        output_path: Path,
        delimiter: str = ",",
        null_value: str = "",
        encoding: str = "utf-8",
        bom: bool = False,
    ) -> None:
        self.output_path = Path(output_path)
        self.delimiter = delimiter
        self.null_value = null_value
        self.encoding = "utf-8-sig" if bom else encoding
        self._file = None
        self._writer = None
        self._columns: list[str] = []
        self._count = 0
        self._start_time = 0.0

    @property
    def name(self) -> str:
        return f"CSV → {self.output_path.name}"

    def open(self, schema: dict) -> None:
        self._columns = [
            sanitize_column_name(f["name"]) for f in schema["fields"]
            if not f["name"].startswith("FILLER")
        ]
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        self._file = open(
            self.output_path, "w", newline="", encoding=self.encoding
        )
        self._writer = csv.writer(self._file, delimiter=self.delimiter)
        self._writer.writerow(self._columns)
        self._start_time = time.monotonic()

    def write_batch(self, records: list[dict]) -> int:
        written = 0
        for record in records:
            row = []
            for col in self._columns:
                # Try sanitized name first, then original with hyphens
                value = record.get(col)
                if value is None:
                    orig_name = col.replace("_", "-")
                    value = record.get(orig_name)
                if value is None:
                    row.append(self.null_value)
                else:
                    row.append(str(value))
            self._writer.writerow(row)
            written += 1
        self._count += written
        self._file.flush()
        return written

    def close(self) -> WriterStats:
        if self._file:
            self._file.close()
            self._file = None
        elapsed = time.monotonic() - self._start_time
        return WriterStats(records_written=self._count, elapsed_sec=elapsed)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/writers/test_csv.py -v`
Expected: All 6 tests PASS

- [ ] **Step 5: Commit**

```bash
git add ztract/writers/csv.py tests/writers/test_csv.py
git commit -m "feat: add CSV writer with delimiter and null handling"
```

### Task 5.3: JSON Lines writer

**Files:**
- Create: `ztract/writers/jsonl.py`
- Create: `tests/writers/test_jsonl.py`

- [ ] **Step 1: Write failing tests**

`tests/writers/test_jsonl.py`:
```python
"""Tests for JSON Lines writer."""

import json
from pathlib import Path

import pytest

from ztract.writers.jsonl import JSONLWriter


@pytest.fixture
def schema() -> dict:
    return {
        "fields": [
            {"name": "CUST-ID", "occurs": None},
            {"name": "CUST-NAME", "occurs": None},
        ]
    }


class TestJSONLWriter:
    def test_writes_one_json_per_line(self, tmp_path: Path, schema: dict):
        output = tmp_path / "out.jsonl"
        writer = JSONLWriter(output)
        writer.open(schema)
        writer.write_batch([
            {"CUST-ID": "001", "CUST-NAME": "Test"},
            {"CUST-ID": "002", "CUST-NAME": "Other"},
        ])
        stats = writer.close()
        assert stats.records_written == 2
        lines = output.read_text(encoding="utf-8").strip().split("\n")
        assert len(lines) == 2
        assert json.loads(lines[0])["CUST-ID"] == "001"

    def test_null_as_json_null(self, tmp_path: Path, schema: dict):
        output = tmp_path / "out.jsonl"
        writer = JSONLWriter(output)
        writer.open(schema)
        writer.write_batch([{"CUST-ID": "001", "CUST-NAME": None}])
        writer.close()
        line = json.loads(output.read_text().strip())
        assert line["CUST-NAME"] is None

    def test_norwegian_chars_preserved(self, tmp_path: Path, schema: dict):
        output = tmp_path / "out.jsonl"
        writer = JSONLWriter(output)
        writer.open(schema)
        writer.write_batch([{"CUST-ID": "001", "CUST-NAME": "Bjorn"}])
        writer.close()
        content = output.read_text(encoding="utf-8")
        assert "Bjorn" in content
        # ensure_ascii=False means no \\u escaping
        assert "\\u" not in content
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/writers/test_jsonl.py -v`
Expected: FAIL

- [ ] **Step 3: Implement JSON Lines writer**

`ztract/writers/jsonl.py`:
```python
"""JSON Lines writer — one JSON object per line."""

import json
import time
from pathlib import Path

from ztract.writers.base import Writer, WriterStats


class JSONLWriter(Writer):
    """Writes records as JSON Lines (one JSON object per line).

    Uses ensure_ascii=False to preserve Unicode characters directly.
    """

    def __init__(self, output_path: Path) -> None:
        self.output_path = Path(output_path)
        self._file = None
        self._count = 0
        self._start_time = 0.0

    @property
    def name(self) -> str:
        return f"JSONL → {self.output_path.name}"

    def open(self, schema: dict) -> None:
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        self._file = open(self.output_path, "w", encoding="utf-8")
        self._start_time = time.monotonic()

    def write_batch(self, records: list[dict]) -> int:
        written = 0
        for record in records:
            self._file.write(
                json.dumps(record, ensure_ascii=False) + "\n"
            )
            written += 1
        self._count += written
        self._file.flush()
        return written

    def close(self) -> WriterStats:
        if self._file:
            self._file.close()
            self._file = None
        elapsed = time.monotonic() - self._start_time
        return WriterStats(records_written=self._count, elapsed_sec=elapsed)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/writers/test_jsonl.py -v`
Expected: All 3 tests PASS

- [ ] **Step 5: Commit**

```bash
git add ztract/writers/jsonl.py tests/writers/test_jsonl.py
git commit -m "feat: add JSON Lines writer with Unicode preservation"
```

### Task 5.4: Parquet writer

**Files:**
- Create: `ztract/writers/parquet.py`
- Create: `tests/writers/test_parquet.py`

- [ ] **Step 1: Write failing tests**

`tests/writers/test_parquet.py`:
```python
"""Tests for Parquet writer."""

from pathlib import Path

import pyarrow.parquet as pq
import pytest

from ztract.writers.parquet import ParquetWriter, build_arrow_schema


@pytest.fixture
def schema() -> dict:
    return {
        "fields": [
            {"name": "CUST-ID", "type": "NUMERIC", "pic": "9(10)",
             "size": 10, "scale": 0, "usage": "DISPLAY", "occurs": None,
             "signed": False},
            {"name": "CUST-NAME", "type": "ALPHANUMERIC", "pic": "X(50)",
             "size": 50, "scale": 0, "usage": "DISPLAY", "occurs": None,
             "signed": False},
            {"name": "CUST-AMT", "type": "PACKED_DECIMAL", "pic": "S9(9)V99",
             "size": 6, "scale": 2, "usage": "COMP-3", "occurs": None,
             "signed": True},
        ]
    }


class TestBuildArrowSchema:
    def test_alphanumeric_to_string(self, schema: dict):
        arrow_schema = build_arrow_schema(schema["fields"])
        assert arrow_schema.field("CUST_NAME").type == "string"

    def test_numeric_to_int64(self, schema: dict):
        arrow_schema = build_arrow_schema(schema["fields"])
        assert str(arrow_schema.field("CUST_ID").type) == "int64"

    def test_comp3_to_decimal(self, schema: dict):
        arrow_schema = build_arrow_schema(schema["fields"])
        field_type = arrow_schema.field("CUST_AMT").type
        assert "decimal" in str(field_type)


class TestParquetWriter:
    def test_writes_parquet_file(self, tmp_path: Path, schema: dict):
        output = tmp_path / "out.parquet"
        writer = ParquetWriter(output)
        writer.open(schema)
        writer.write_batch([
            {"CUST-ID": 1234567890, "CUST-NAME": "Test", "CUST-AMT": "100.50"},
            {"CUST-ID": 9876543210, "CUST-NAME": "Other", "CUST-AMT": "200.75"},
        ])
        stats = writer.close()

        assert stats.records_written == 2
        table = pq.read_table(output)
        assert table.num_rows == 2
        assert "CUST_ID" in table.column_names
        assert "CUST_NAME" in table.column_names

    def test_null_values(self, tmp_path: Path, schema: dict):
        output = tmp_path / "out.parquet"
        writer = ParquetWriter(output)
        writer.open(schema)
        writer.write_batch([
            {"CUST-ID": 1, "CUST-NAME": None, "CUST-AMT": None},
        ])
        writer.close()
        table = pq.read_table(output)
        assert table.column("CUST_NAME")[0].as_py() is None
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/writers/test_parquet.py -v`
Expected: FAIL

- [ ] **Step 3: Implement Parquet writer**

`ztract/writers/parquet.py`:
```python
"""Parquet writer using pyarrow with streaming row groups."""

import time
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from ztract.writers.base import Writer, WriterStats, sanitize_column_name


def _cobol_to_arrow_type(field_def: dict) -> pa.DataType:
    """Map a COBOL field type to an Arrow data type."""
    usage = field_def.get("usage", "DISPLAY")
    field_type = field_def.get("type", "ALPHANUMERIC")
    scale = field_def.get("scale", 0)
    size = field_def.get("size", 0)

    if field_type == "ALPHANUMERIC":
        return pa.string()

    if usage in ("COMP-1",):
        return pa.float32()
    if usage in ("COMP-2",):
        return pa.float64()

    if usage == "COMP-3" or field_type == "PACKED_DECIMAL":
        # Packed decimal → Arrow decimal128
        # Estimate precision from PIC: S9(9)V99 → precision=11, scale=2
        precision = size * 2  # rough estimate, cap at 38
        precision = min(precision, 38)
        return pa.decimal128(max(precision, scale + 1), scale)

    # Numeric DISPLAY or COMP/COMP-4
    if field_type == "NUMERIC" or usage in ("COMP", "COMP-4"):
        if scale > 0:
            precision = min(size + scale, 38)
            return pa.decimal128(precision, scale)
        if size <= 9:
            return pa.int32()
        return pa.int64()

    return pa.string()


def build_arrow_schema(fields: list[dict]) -> pa.Schema:
    """Build an Arrow schema from copybook field definitions."""
    arrow_fields = []
    for f in fields:
        if f["name"].startswith("FILLER"):
            continue
        col_name = sanitize_column_name(f["name"])
        arrow_type = _cobol_to_arrow_type(f)
        arrow_fields.append(pa.field(col_name, arrow_type, nullable=True))
    return pa.schema(arrow_fields)


class ParquetWriter(Writer):
    """Writes records to Parquet with streaming row groups.

    Schema auto-derived from copybook field definitions.
    """

    def __init__(
        self,
        output_path: Path,
        row_group_size: int = 10_000,
        compression: str = "snappy",
    ) -> None:
        self.output_path = Path(output_path)
        self.row_group_size = row_group_size
        self.compression = compression
        self._schema: pa.Schema | None = None
        self._writer: pq.ParquetWriter | None = None
        self._buffer: list[dict] = []
        self._count = 0
        self._start_time = 0.0
        self._field_defs: list[dict] = []

    @property
    def name(self) -> str:
        return f"Parquet → {self.output_path.name}"

    def open(self, schema: dict) -> None:
        self._field_defs = [
            f for f in schema["fields"]
            if not f["name"].startswith("FILLER")
        ]
        self._schema = build_arrow_schema(self._field_defs)
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        self._writer = pq.ParquetWriter(
            str(self.output_path),
            self._schema,
            compression=self.compression,
        )
        self._start_time = time.monotonic()

    def write_batch(self, records: list[dict]) -> int:
        self._buffer.extend(records)
        written = 0

        while len(self._buffer) >= self.row_group_size:
            batch = self._buffer[:self.row_group_size]
            self._buffer = self._buffer[self.row_group_size:]
            self._flush_batch(batch)
            written += len(batch)

        # If this is a small final batch, count them as written too
        if records and not written:
            written = len(records)
            self._count += written
            return written

        self._count += written
        return written

    def _flush_batch(self, records: list[dict]) -> None:
        """Convert records to Arrow table and write as row group."""
        columns: dict[str, list] = {
            f.name: [] for f in self._schema
        }

        for record in records:
            for field_def in self._field_defs:
                col_name = sanitize_column_name(field_def["name"])
                value = record.get(field_def["name"])
                if value is None:
                    value = record.get(col_name)
                columns[col_name].append(value)

        arrays = []
        for field in self._schema:
            values = columns[field.name]
            if isinstance(field.type, pa.Decimal128Type):
                values = [
                    Decimal(str(v)) if v is not None else None
                    for v in values
                ]
            arrays.append(pa.array(values, type=field.type))

        table = pa.table(
            {f.name: arr for f, arr in zip(self._schema, arrays)},
            schema=self._schema,
        )
        self._writer.write_table(table)

    def close(self) -> WriterStats:
        # Flush remaining buffer
        if self._buffer:
            self._flush_batch(self._buffer)
            self._count += len(self._buffer)
            self._buffer = []

        if self._writer:
            self._writer.close()
            self._writer = None

        elapsed = time.monotonic() - self._start_time
        return WriterStats(records_written=self._count, elapsed_sec=elapsed)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/writers/test_parquet.py -v`
Expected: All 4 tests PASS

- [ ] **Step 5: Commit**

```bash
git add ztract/writers/parquet.py tests/writers/test_parquet.py
git commit -m "feat: add Parquet writer with auto schema from copybook"
```

### Task 5.5: Database writer

**Files:**
- Create: `ztract/writers/database.py`
- Create: `tests/writers/test_database.py`

- [ ] **Step 1: Write failing tests (using SQLite for test isolation)**

`tests/writers/test_database.py`:
```python
"""Tests for database writer using SQLite."""

from pathlib import Path

import pytest
from sqlalchemy import create_engine, text

from ztract.writers.database import DatabaseWriter


@pytest.fixture
def schema() -> dict:
    return {
        "fields": [
            {"name": "CUST-ID", "type": "NUMERIC", "pic": "9(10)",
             "size": 10, "scale": 0, "usage": "DISPLAY", "occurs": None,
             "signed": False},
            {"name": "CUST-NAME", "type": "ALPHANUMERIC", "pic": "X(50)",
             "size": 50, "scale": 0, "usage": "DISPLAY", "occurs": None,
             "signed": False},
            {"name": "CUST-AMT", "type": "PACKED_DECIMAL", "pic": "S9(9)V99",
             "size": 6, "scale": 2, "usage": "COMP-3", "occurs": None,
             "signed": True},
        ]
    }


@pytest.fixture
def db_url(tmp_path: Path) -> str:
    return f"sqlite:///{tmp_path / 'test.db'}"


class TestDatabaseWriter:
    def test_creates_table_and_inserts(self, db_url: str, schema: dict):
        writer = DatabaseWriter(db_url, table_name="customers")
        writer.open(schema)
        writer.write_batch([
            {"CUST-ID": 1234567890, "CUST-NAME": "Test", "CUST-AMT": 100.50},
            {"CUST-ID": 9876543210, "CUST-NAME": "Other", "CUST-AMT": 200.75},
        ])
        stats = writer.close()

        assert stats.records_written == 2
        engine = create_engine(db_url)
        with engine.connect() as conn:
            rows = conn.execute(text("SELECT * FROM customers")).fetchall()
        assert len(rows) == 2

    def test_column_names_sanitized(self, db_url: str, schema: dict):
        writer = DatabaseWriter(db_url, table_name="customers")
        writer.open(schema)
        writer.write_batch([
            {"CUST-ID": 1, "CUST-NAME": "Test", "CUST-AMT": 0},
        ])
        writer.close()

        engine = create_engine(db_url)
        with engine.connect() as conn:
            result = conn.execute(text("SELECT CUST_ID FROM customers"))
            assert result.fetchone()[0] == 1

    def test_append_mode(self, db_url: str, schema: dict):
        writer = DatabaseWriter(db_url, table_name="customers", mode="append")
        writer.open(schema)
        writer.write_batch([{"CUST-ID": 1, "CUST-NAME": "A", "CUST-AMT": 0}])
        writer.close()

        writer2 = DatabaseWriter(db_url, table_name="customers", mode="append")
        writer2.open(schema)
        writer2.write_batch([{"CUST-ID": 2, "CUST-NAME": "B", "CUST-AMT": 0}])
        writer2.close()

        engine = create_engine(db_url)
        with engine.connect() as conn:
            count = conn.execute(text("SELECT COUNT(*) FROM customers")).scalar()
        assert count == 2

    def test_truncate_mode(self, db_url: str, schema: dict):
        writer = DatabaseWriter(db_url, table_name="customers", mode="append")
        writer.open(schema)
        writer.write_batch([{"CUST-ID": 1, "CUST-NAME": "A", "CUST-AMT": 0}])
        writer.close()

        writer2 = DatabaseWriter(db_url, table_name="customers", mode="truncate")
        writer2.open(schema)
        writer2.write_batch([{"CUST-ID": 2, "CUST-NAME": "B", "CUST-AMT": 0}])
        writer2.close()

        engine = create_engine(db_url)
        with engine.connect() as conn:
            count = conn.execute(text("SELECT COUNT(*) FROM customers")).scalar()
        assert count == 1

    def test_null_values(self, db_url: str, schema: dict):
        writer = DatabaseWriter(db_url, table_name="customers")
        writer.open(schema)
        writer.write_batch([
            {"CUST-ID": 1, "CUST-NAME": None, "CUST-AMT": None},
        ])
        writer.close()

        engine = create_engine(db_url)
        with engine.connect() as conn:
            row = conn.execute(text("SELECT * FROM customers")).fetchone()
        assert row[1] is None  # CUST_NAME
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/writers/test_database.py -v`
Expected: FAIL

- [ ] **Step 3: Implement database writer**

`ztract/writers/database.py`:
```python
"""Database writer using SQLAlchemy.

Auto-creates tables from copybook schema. Supports append and truncate modes.
Column names sanitized from COBOL hyphens to SQL underscores.
"""

import logging
import time
from typing import Any

from sqlalchemy import (
    Column,
    Float,
    Integer,
    MetaData,
    Numeric,
    String,
    Table,
    create_engine,
    text,
)
from sqlalchemy.engine import Engine

from ztract.writers.base import Writer, WriterStats, sanitize_column_name

logger = logging.getLogger(__name__)


def _cobol_to_sql_type(field_def: dict) -> Any:
    """Map COBOL field type to SQLAlchemy column type."""
    usage = field_def.get("usage", "DISPLAY")
    field_type = field_def.get("type", "ALPHANUMERIC")
    scale = field_def.get("scale", 0)
    size = field_def.get("size", 0)

    if field_type == "ALPHANUMERIC":
        return String(size)

    if usage in ("COMP-1",):
        return Float()
    if usage in ("COMP-2",):
        return Float()

    if usage == "COMP-3" or field_type == "PACKED_DECIMAL":
        precision = min(size * 2, 38)
        return Numeric(precision=precision, scale=scale)

    if field_type == "NUMERIC" or usage in ("COMP", "COMP-4"):
        if scale > 0:
            return Numeric(precision=min(size + scale, 38), scale=scale)
        return Integer()

    return String(size or 255)


class DatabaseWriter(Writer):
    """Writes records to a database via SQLAlchemy.

    Auto-creates table from copybook schema if it doesn't exist.
    """

    def __init__(
        self,
        connection_url: str,
        table_name: str,
        mode: str = "append",
        batch_size: int = 1000,
    ) -> None:
        self.connection_url = connection_url
        self.table_name = table_name
        self.mode = mode
        self.batch_size = batch_size
        self._engine: Engine | None = None
        self._table: Table | None = None
        self._columns: list[str] = []
        self._count = 0
        self._start_time = 0.0

    @property
    def name(self) -> str:
        return f"DB → {self.table_name}"

    def open(self, schema: dict) -> None:
        self._engine = create_engine(self.connection_url)
        metadata = MetaData()

        columns = []
        self._columns = []
        for f in schema["fields"]:
            if f["name"].startswith("FILLER"):
                continue
            col_name = sanitize_column_name(f["name"])
            self._columns.append(col_name)
            columns.append(Column(col_name, _cobol_to_sql_type(f), nullable=True))

        self._table = Table(self.table_name, metadata, *columns)
        metadata.create_all(self._engine)

        if self.mode == "truncate":
            with self._engine.begin() as conn:
                conn.execute(self._table.delete())

        self._start_time = time.monotonic()

    def write_batch(self, records: list[dict]) -> int:
        rows = []
        for record in records:
            row = {}
            for col_name in self._columns:
                value = record.get(col_name)
                if value is None:
                    orig = col_name.replace("_", "-")
                    value = record.get(orig)
                row[col_name] = value
            rows.append(row)

        with self._engine.begin() as conn:
            conn.execute(self._table.insert(), rows)

        self._count += len(rows)
        return len(rows)

    def close(self) -> WriterStats:
        if self._engine:
            self._engine.dispose()
            self._engine = None
        elapsed = time.monotonic() - self._start_time
        return WriterStats(records_written=self._count, elapsed_sec=elapsed)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/writers/test_database.py -v`
Expected: All 5 tests PASS

- [ ] **Step 5: Commit**

```bash
git add ztract/writers/database.py tests/writers/test_database.py
git commit -m "feat: add database writer with auto table creation"
```

### Task 5.6: Fan-out queue

**Files:**
- Create: `ztract/pipeline/fanout.py`
- Create: `tests/pipeline/test_fanout.py`

- [ ] **Step 1: Write failing tests**

`tests/pipeline/test_fanout.py`:
```python
"""Tests for fan-out queue distributing records to multiple writers."""

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from ztract.pipeline.fanout import FanOut
from ztract.writers.base import Writer, WriterStats


class FakeWriter(Writer):
    def __init__(self, name: str):
        self._name = name
        self.batches: list[list[dict]] = []
        self._count = 0

    @property
    def name(self) -> str:
        return self._name

    def open(self, schema: dict) -> None:
        pass

    def write_batch(self, records: list[dict]) -> int:
        self.batches.append(records)
        self._count += len(records)
        return len(records)

    def close(self) -> WriterStats:
        return WriterStats(records_written=self._count)


class TestFanOut:
    def test_broadcasts_to_all_writers(self):
        w1 = FakeWriter("w1")
        w2 = FakeWriter("w2")
        schema = {"fields": []}

        records = [{"A": 1}, {"A": 2}, {"A": 3}]

        fanout = FanOut(writers=[w1, w2], schema=schema, batch_size=10)
        fanout.run(iter(records))

        assert sum(len(b) for b in w1.batches) == 3
        assert sum(len(b) for b in w2.batches) == 3

    def test_batches_records(self):
        w1 = FakeWriter("w1")
        schema = {"fields": []}

        records = [{"A": i} for i in range(25)]

        fanout = FanOut(writers=[w1], schema=schema, batch_size=10)
        fanout.run(iter(records))

        # 25 records with batch_size=10 → 3 batches (10, 10, 5)
        assert len(w1.batches) == 3
        assert len(w1.batches[0]) == 10
        assert len(w1.batches[2]) == 5

    def test_returns_total_count(self):
        w1 = FakeWriter("w1")
        schema = {"fields": []}
        records = [{"A": i} for i in range(7)]

        fanout = FanOut(writers=[w1], schema=schema, batch_size=100)
        count = fanout.run(iter(records))
        assert count == 7

    def test_empty_iterator(self):
        w1 = FakeWriter("w1")
        schema = {"fields": []}

        fanout = FanOut(writers=[w1], schema=schema)
        count = fanout.run(iter([]))
        assert count == 0
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/pipeline/test_fanout.py -v`
Expected: FAIL

- [ ] **Step 3: Implement fan-out**

`ztract/pipeline/fanout.py`:
```python
"""Fan-out queue distributing records to multiple writers.

Reads records from an iterator (Java stdout) and broadcasts
to all writers in batches. Writers run in threads for concurrent I/O.
"""

import logging
import queue
import threading
from collections.abc import Iterator
from typing import Any

from ztract.writers.base import Writer, WriterStats

logger = logging.getLogger(__name__)

_POISON = object()  # Sentinel for shutdown


class FanOut:
    """Distributes records from one source to multiple writers.

    Each writer gets its own queue and thread. The reader puts
    records into all queues (broadcast). Writers consume from
    their queue independently.
    """

    def __init__(
        self,
        writers: list[Writer],
        schema: dict,
        batch_size: int = 1000,
        queue_size: int = 5000,
    ) -> None:
        self.writers = writers
        self.schema = schema
        self.batch_size = batch_size
        self.queue_size = queue_size

    def run(self, records: Iterator[dict]) -> int:
        """Read all records and distribute to writers.

        Args:
            records: Iterator of record dicts.

        Returns:
            Total number of records read.
        """
        for w in self.writers:
            w.open(self.schema)

        if len(self.writers) == 1:
            count = self._run_single(records, self.writers[0])
        else:
            count = self._run_threaded(records)

        return count

    def _run_single(self, records: Iterator[dict], writer: Writer) -> int:
        """Optimized path for a single writer (no threads)."""
        batch: list[dict] = []
        count = 0

        for record in records:
            batch.append(record)
            count += 1
            if len(batch) >= self.batch_size:
                writer.write_batch(batch)
                batch = []

        if batch:
            writer.write_batch(batch)

        writer.close()
        return count

    def _run_threaded(self, records: Iterator[dict]) -> int:
        """Multi-writer path with per-writer queues and threads."""
        queues: list[queue.Queue] = [
            queue.Queue(maxsize=self.queue_size) for _ in self.writers
        ]
        threads: list[threading.Thread] = []

        for writer, q in zip(self.writers, queues):
            t = threading.Thread(
                target=self._writer_loop,
                args=(writer, q),
                daemon=True,
            )
            t.start()
            threads.append(t)

        count = 0
        batch: list[dict] = []

        for record in records:
            batch.append(record)
            count += 1
            if len(batch) >= self.batch_size:
                for q in queues:
                    q.put(list(batch))
                batch = []

        if batch:
            for q in queues:
                q.put(list(batch))

        # Send poison pill
        for q in queues:
            q.put(_POISON)

        for t in threads:
            t.join()

        return count

    @staticmethod
    def _writer_loop(writer: Writer, q: queue.Queue) -> None:
        """Writer thread main loop."""
        try:
            while True:
                item = q.get()
                if item is _POISON:
                    break
                writer.write_batch(item)
        except Exception:
            logger.exception("Writer %s failed", writer.name)
        finally:
            writer.close()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/pipeline/test_fanout.py -v`
Expected: All 4 tests PASS

- [ ] **Step 5: Commit**

```bash
git add ztract/pipeline/fanout.py tests/pipeline/test_fanout.py
git commit -m "feat: add fan-out queue for concurrent multi-writer output"
```

---

## Phase 6: Local Connector + Convert CLI

### Task 6.1: Connector base + local connector

**Files:**
- Create: `ztract/connectors/base.py`
- Create: `ztract/connectors/local.py`
- Create: `tests/connectors/test_local.py`

- [ ] **Step 1: Write failing tests**

`tests/connectors/test_local.py`:
```python
"""Tests for local file connector."""

from pathlib import Path

import pytest

from ztract.connectors.local import LocalConnector


class TestLocalConnector:
    def test_download_returns_same_path(self, tmp_path: Path):
        f = tmp_path / "test.dat"
        f.write_bytes(b"data")
        conn = LocalConnector()
        result = conn.download(str(f), str(tmp_path / "dest.dat"))
        assert Path(result) == f

    def test_download_missing_file_raises(self, tmp_path: Path):
        conn = LocalConnector()
        with pytest.raises(FileNotFoundError):
            conn.download(str(tmp_path / "missing.dat"), str(tmp_path / "dest.dat"))

    def test_download_empty_file_raises(self, tmp_path: Path):
        f = tmp_path / "empty.dat"
        f.write_bytes(b"")
        conn = LocalConnector()
        with pytest.raises(ValueError, match="empty"):
            conn.download(str(f), str(tmp_path / "dest.dat"))

    def test_upload_copies_file(self, tmp_path: Path):
        src = tmp_path / "src.csv"
        src.write_text("data")
        dest = tmp_path / "output" / "dest.csv"
        conn = LocalConnector()
        conn.upload(str(src), str(dest))
        assert dest.read_text() == "data"

    def test_exists(self, tmp_path: Path):
        f = tmp_path / "exists.dat"
        f.write_bytes(b"data")
        conn = LocalConnector()
        assert conn.exists(str(f)) is True
        assert conn.exists(str(tmp_path / "nope.dat")) is False

    def test_close_is_noop(self):
        conn = LocalConnector()
        conn.close()  # should not raise
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/connectors/test_local.py -v`
Expected: FAIL

- [ ] **Step 3: Implement connector base and local connector**

`ztract/connectors/base.py`:
```python
"""Connector base class — abstract interface for file transport backends."""

from abc import ABC, abstractmethod
from pathlib import Path


class Connector(ABC):
    """Abstract base for file transport (local, FTP, SFTP, Zowe)."""

    @abstractmethod
    def download(self, source: str, local_path: str) -> Path:
        """Download/locate a file. Return local path."""

    @abstractmethod
    def upload(self, local_path: str, destination: str,
               site_commands: dict | None = None) -> None:
        """Upload a local file to destination."""

    def list_datasets(self, pattern: str) -> list[str]:
        """List matching datasets. Optional — not all connectors support this."""
        raise NotImplementedError(
            f"{type(self).__name__} does not support dataset listing"
        )

    @abstractmethod
    def exists(self, source: str) -> bool:
        """Check if source exists."""

    @abstractmethod
    def close(self) -> None:
        """Release connections."""
```

`ztract/connectors/local.py`:
```python
"""Local filesystem connector."""

import shutil
from pathlib import Path

from ztract.connectors.base import Connector


class LocalConnector(Connector):
    """Connector for local files. Download is a no-op (returns path as-is)."""

    def download(self, source: str, local_path: str) -> Path:
        src = Path(source)
        if not src.exists():
            raise FileNotFoundError(f"File not found: {source}")
        if src.stat().st_size == 0:
            raise ValueError(f"File is empty: {source}")
        return src

    def upload(self, local_path: str, destination: str,
               site_commands: dict | None = None) -> None:
        dest = Path(destination)
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(local_path, dest)

    def exists(self, source: str) -> bool:
        return Path(source).exists()

    def close(self) -> None:
        pass
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/connectors/test_local.py -v`
Expected: All 6 tests PASS

- [ ] **Step 5: Commit**

```bash
git add ztract/connectors/base.py ztract/connectors/local.py tests/connectors/test_local.py
git commit -m "feat: add connector base class and local filesystem connector"
```

### Task 6.2: Convert CLI command

**Files:**
- Create: `ztract/cli/convert.py`
- Modify: `ztract/cli/root.py`

- [ ] **Step 1: Implement convert command**

`ztract/cli/convert.py`:
```python
"""ztract convert — extract EBCDIC binary to CSV/JSONL/Parquet/DB."""

import logging
import sys
from pathlib import Path

import click

from ztract.codepages import resolve_codepage, CodepageError
from ztract.connectors.dataset_format import RecordFormat, validate_record_format
from ztract.connectors.local import LocalConnector
from ztract.engine.bridge import ZtractBridge, EngineError
from ztract.observability.progress import ProgressTracker
from ztract.pipeline.fanout import FanOut
from ztract.writers.csv import CSVWriter
from ztract.writers.jsonl import JSONLWriter

logger = logging.getLogger(__name__)

# Lazy imports for optional heavy deps
_WRITERS = {
    ".csv": lambda p, **kw: CSVWriter(p, **kw),
    ".jsonl": lambda p, **kw: JSONLWriter(p),
}


def _get_writer(output_path: Path, fmt: str | None, **kwargs):
    """Create a writer based on format or file extension."""
    if fmt:
        ext = f".{fmt}"
    else:
        ext = output_path.suffix.lower()

    if ext in _WRITERS:
        return _WRITERS[ext](output_path, **kwargs)

    if ext == ".parquet":
        from ztract.writers.parquet import ParquetWriter
        return ParquetWriter(output_path, **{
            k: v for k, v in kwargs.items()
            if k in ("compression", "row_group_size")
        })

    raise click.BadParameter(
        f"Unknown output format '{ext}'. Supported: csv, jsonl, parquet"
    )


@click.command()
@click.option("--copybook", required=True, type=click.Path(exists=True),
              help="Path to COBOL copybook (.cpy)")
@click.option("--input", "input_path", required=True,
              help="Path to EBCDIC binary file or URI")
@click.option("--output", "output_paths", required=True, multiple=True,
              help="Output path(s) — repeat for fan-out")
@click.option("--recfm", required=True,
              type=click.Choice(["F", "FB", "V", "VB", "FBA", "VBA"],
                                case_sensitive=False),
              help="Record format")
@click.option("--lrecl", type=int, default=None,
              help="Logical record length")
@click.option("--codepage", default="cp037",
              help="EBCDIC codepage (e.g., cp277, norway)")
@click.option("--format", "fmt", default=None,
              help="Output format (inferred from extension if omitted)")
@click.option("--delimiter", default=",",
              help="CSV delimiter (default: comma)")
@click.option("--encoding", default="ebcdic",
              type=click.Choice(["ebcdic", "ascii"]),
              help="Input encoding")
@click.pass_context
def convert(
    ctx: click.Context,
    copybook: str,
    input_path: str,
    output_paths: tuple[str, ...],
    recfm: str,
    lrecl: int | None,
    codepage: str,
    fmt: str | None,
    delimiter: str,
    encoding: str,
) -> None:
    """Extract EBCDIC binary data to CSV, JSON Lines, or Parquet."""
    debug = ctx.obj.get("debug", False)
    quiet = ctx.obj.get("quiet", False)

    # Resolve codepage
    try:
        resolved_cp = resolve_codepage(codepage)
    except CodepageError as e:
        raise click.BadParameter(str(e)) from None

    # Validate record format
    recfm_enum = RecordFormat.from_str(recfm)
    try:
        validate_record_format(recfm_enum, lrecl)
    except Exception as e:
        raise click.BadParameter(str(e)) from None

    # Warn if --format conflicts with extension
    if fmt and output_paths:
        first_ext = Path(output_paths[0]).suffix.lower().lstrip(".")
        if first_ext and first_ext != fmt:
            click.echo(
                f"WARNING: --format {fmt} overrides .{first_ext} extension",
                err=True,
            )

    # Locate JAR
    jar_path = Path(__file__).parent.parent / "engine" / "ztract-engine.jar"
    bridge = ZtractBridge(jar_path=jar_path)

    # Check JRE
    try:
        jre_version = bridge.check_jre()
        if debug:
            click.echo(f"JRE version: {jre_version}")
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)

    # Get schema for writers
    try:
        schema = bridge.get_schema(
            Path(copybook), recfm=recfm_enum.value, lrecl=lrecl
        )
    except EngineError as e:
        click.echo(f"Error reading copybook: {e}", err=True)
        sys.exit(1)

    # Create writers
    writers = []
    for out_path in output_paths:
        writer = _get_writer(
            Path(out_path), fmt, delimiter=delimiter
        )
        writers.append(writer)

    # Resolve input (local for now)
    connector = LocalConnector()
    local_input = connector.download(input_path, input_path)

    # Decode and fan-out
    progress = ProgressTracker(quiet=quiet)
    task_id = progress.add_step("decode", total=None)

    try:
        records = bridge.decode(
            Path(copybook), local_input, recfm_enum.value,
            lrecl, resolved_cp, encoding,
        )

        def counted_records():
            for r in records:
                progress.update(task_id, advance=1)
                yield r

        fanout = FanOut(writers=writers, schema=schema)
        total = fanout.run(counted_records())

        progress.finish()
        click.echo(f"\nDone. {total:,} records written.")

    except EngineError as e:
        progress.finish()
        click.echo(f"\nError: {e}", err=True)
        sys.exit(1)
    finally:
        connector.close()
```

- [ ] **Step 2: Register convert command in root**

Add to `ztract/cli/root.py` at the bottom:
```python
from ztract.cli.convert import convert

cli.add_command(convert)
```

- [ ] **Step 3: Verify CLI wiring**

Run: `ztract convert --help`
Expected: Shows all convert options (--copybook, --input, --output, --recfm, etc.)

- [ ] **Step 4: Commit**

```bash
git add ztract/cli/convert.py ztract/cli/root.py
git commit -m "feat: add ztract convert command with fan-out support"
```

---

## Phase 7: Remote Connectors (FTP, SFTP, Zowe)

### Task 7.1: FTP connector

**Files:**
- Create: `ztract/connectors/ftp.py`
- Create: `tests/connectors/test_ftp.py`

- [ ] **Step 1: Write failing tests (mocked ftplib)**

`tests/connectors/test_ftp.py`:
```python
"""Tests for FTP connector (mocked ftplib)."""

from pathlib import Path
from unittest.mock import MagicMock, patch, call

import pytest

from ztract.connectors.ftp import FTPConnector


@pytest.fixture
def mock_ftp():
    with patch("ftplib.FTP") as MockFTP:
        instance = MockFTP.return_value
        instance.__enter__ = MagicMock(return_value=instance)
        instance.__exit__ = MagicMock(return_value=False)
        yield instance


class TestFTPConnector:
    def test_download_binary(self, mock_ftp, tmp_path: Path):
        dest = tmp_path / "out.dat"
        mock_ftp.retrbinary = MagicMock(side_effect=lambda cmd, cb: cb(b"data"))

        conn = FTPConnector(
            host="mf01.bank.com", user="user", password="pass"
        )
        conn._ftp = mock_ftp
        result = conn.download("BEL.CUST.DATA", str(dest))

        mock_ftp.retrbinary.assert_called_once()
        assert Path(result).exists()

    def test_upload_with_site_commands(self, mock_ftp, tmp_path: Path):
        src = tmp_path / "report.csv"
        src.write_text("data")

        conn = FTPConnector(
            host="mf01.bank.com", user="user", password="pass"
        )
        conn._ftp = mock_ftp
        conn.upload(
            str(src),
            "BEL.CUST.REPORT",
            site_commands={
                "recfm": "FB",
                "lrecl": 500,
                "blksize": 27920,
                "space_unit": "CYLINDERS",
                "primary": 5,
                "secondary": 2,
            },
        )

        # Verify SITE commands sent in correct order
        site_calls = [
            c for c in mock_ftp.sendcmd.call_args_list
            if "SITE" in str(c)
        ]
        assert len(site_calls) >= 2  # At least RECFM and LRECL

    def test_site_command_order(self, mock_ftp, tmp_path: Path):
        src = tmp_path / "data.dat"
        src.write_bytes(b"test")

        conn = FTPConnector(host="h", user="u", password="p")
        conn._ftp = mock_ftp
        conn.upload(
            str(src), "DS",
            site_commands={
                "primary": 5,
                "recfm": "FB",
                "blksize": 27920,
                "lrecl": 500,
                "secondary": 2,
                "space_unit": "CYLINDERS",
            },
        )

        sent = [str(c) for c in mock_ftp.sendcmd.call_args_list]
        # RECFM must come before LRECL, LRECL before BLKSIZE
        recfm_idx = next(i for i, s in enumerate(sent) if "RECFM" in s)
        lrecl_idx = next(i for i, s in enumerate(sent) if "LRECL" in s)
        blksize_idx = next(i for i, s in enumerate(sent) if "BLKSIZE" in s)
        assert recfm_idx < lrecl_idx < blksize_idx
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/connectors/test_ftp.py -v`
Expected: FAIL

- [ ] **Step 3: Implement FTP connector**

`ztract/connectors/ftp.py`:
```python
"""FTP connector for z/OS mainframe datasets.

Supports binary and text transfer modes, passive/active FTP,
and z/OS SITE commands for dataset allocation on upload.
"""

import ftplib
import logging
import time
from pathlib import Path

from ztract.connectors.base import Connector

logger = logging.getLogger(__name__)

# z/OS SITE command order — must be sent in this exact sequence
_SITE_CMD_ORDER = [
    "recfm",
    "lrecl",
    "blksize",
    "space_unit",  # CYLINDERS or TRACKS (no = sign)
    "primary",
    "secondary",
    "mgmtclas",
    "storclas",
    "dataclas",
    "unit",
    "volser",
]


class FTPConnector(Connector):
    """FTP connector for z/OS datasets.

    Handles binary/text transfer, SITE commands for allocation,
    and passive/active mode.
    """

    def __init__(
        self,
        host: str,
        user: str,
        password: str,
        port: int = 21,
        transfer_mode: str = "binary",
        ftp_mode: str = "passive",
        timeout: int = 30,
        retries: int = 3,
    ) -> None:
        self.host = host
        self.user = user
        self.password = password
        self.port = port
        self.transfer_mode = transfer_mode
        self.ftp_mode = ftp_mode
        self.timeout = timeout
        self.retries = retries
        self._ftp: ftplib.FTP | None = None

    def _connect(self) -> ftplib.FTP:
        """Establish FTP connection with retry."""
        if self._ftp is not None:
            try:
                self._ftp.voidcmd("NOOP")
                return self._ftp
            except (ftplib.error_temp, OSError):
                self._ftp = None

        for attempt in range(self.retries):
            try:
                ftp = ftplib.FTP()
                ftp.connect(self.host, self.port, timeout=self.timeout)
                ftp.login(self.user, self.password)
                ftp.set_pasv(self.ftp_mode == "passive")
                self._ftp = ftp
                return ftp
            except (ftplib.all_errors, OSError) as e:
                if attempt < self.retries - 1:
                    wait = 2 ** attempt
                    logger.warning(
                        "FTP connect attempt %d failed: %s. Retrying in %ds...",
                        attempt + 1, e, wait,
                    )
                    time.sleep(wait)
                else:
                    raise

    def download(self, source: str, local_path: str) -> Path:
        ftp = self._connect()
        dest = Path(local_path)
        dest.parent.mkdir(parents=True, exist_ok=True)

        with open(dest, "wb") as f:
            if self.transfer_mode == "binary":
                ftp.retrbinary(f"RETR {source}", f.write)
            else:
                ftp.retrlines(
                    f"RETR {source}",
                    lambda line: f.write((line + "\n").encode("utf-8")),
                )

        return dest

    def upload(
        self,
        local_path: str,
        destination: str,
        site_commands: dict | None = None,
    ) -> None:
        ftp = self._connect()

        if site_commands:
            self._send_site_commands(ftp, site_commands)

        with open(local_path, "rb") as f:
            ftp.storbinary(f"STOR {destination}", f)

    def _send_site_commands(
        self, ftp: ftplib.FTP, commands: dict
    ) -> None:
        """Send SITE commands in the correct z/OS order."""
        for key in _SITE_CMD_ORDER:
            value = commands.get(key)
            if value is None:
                continue

            if key == "space_unit":
                # CYLINDERS or TRACKS — no = sign
                cmd = f"SITE {str(value).upper()}"
            else:
                cmd = f"SITE {key.upper()}={value}"

            logger.debug("FTP SITE: %s", cmd)
            ftp.sendcmd(cmd)

    def list_datasets(self, pattern: str) -> list[str]:
        ftp = self._connect()
        lines: list[str] = []
        ftp.retrlines(f"LIST {pattern}", lines.append)
        return lines

    def exists(self, source: str) -> bool:
        ftp = self._connect()
        try:
            ftp.size(source)
            return True
        except ftplib.error_perm:
            return False

    def close(self) -> None:
        if self._ftp:
            try:
                self._ftp.quit()
            except (ftplib.all_errors, OSError):
                pass
            self._ftp = None
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/connectors/test_ftp.py -v`
Expected: All 3 tests PASS

- [ ] **Step 5: Commit**

```bash
git add ztract/connectors/ftp.py tests/connectors/test_ftp.py
git commit -m "feat: add FTP connector with z/OS SITE command ordering"
```

### Task 7.2: SFTP connector

**Files:**
- Create: `ztract/connectors/sftp.py`
- Create: `tests/connectors/test_sftp.py`

- [ ] **Step 1: Write failing tests (mocked paramiko)**

`tests/connectors/test_sftp.py`:
```python
"""Tests for SFTP connector (mocked paramiko)."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from ztract.connectors.sftp import SFTPConnector


class TestSFTPConnector:
    def test_download(self, tmp_path: Path):
        dest = tmp_path / "out.dat"

        mock_sftp = MagicMock()
        mock_transport = MagicMock()

        conn = SFTPConnector(host="mf01", user="user", password="pass")
        conn._sftp = mock_sftp
        conn._transport = mock_transport

        conn.download("/u/data/CUST.DAT", str(dest))
        mock_sftp.get.assert_called_once_with("/u/data/CUST.DAT", str(dest))

    def test_upload(self, tmp_path: Path):
        src = tmp_path / "report.csv"
        src.write_text("data")

        mock_sftp = MagicMock()
        mock_transport = MagicMock()

        conn = SFTPConnector(host="mf01", user="user", password="pass")
        conn._sftp = mock_sftp
        conn._transport = mock_transport

        conn.upload(str(src), "/u/data/REPORT.CSV")
        mock_sftp.put.assert_called_once()

    def test_exists(self, tmp_path: Path):
        mock_sftp = MagicMock()
        mock_sftp.stat.return_value = MagicMock()
        mock_transport = MagicMock()

        conn = SFTPConnector(host="mf01", user="user", password="pass")
        conn._sftp = mock_sftp
        conn._transport = mock_transport

        assert conn.exists("/u/data/CUST.DAT") is True
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/connectors/test_sftp.py -v`
Expected: FAIL

- [ ] **Step 3: Implement SFTP connector**

`ztract/connectors/sftp.py`:
```python
"""SFTP connector using paramiko.

Binary transfer only. Does NOT support SITE commands —
z/OS SFTP uses SMS-managed allocation.
"""

import logging
from pathlib import Path

import paramiko

from ztract.connectors.base import Connector

logger = logging.getLogger(__name__)


class SFTPConnector(Connector):
    """SFTP connector for z/OS via SSH.

    Note: SFTP does not support SITE commands. Dataset allocation
    on z/OS via SFTP is SMS-managed. For explicit allocation
    control, use FTP instead.
    """

    def __init__(
        self,
        host: str,
        user: str,
        password: str | None = None,
        key_path: str | None = None,
        port: int = 22,
    ) -> None:
        self.host = host
        self.user = user
        self.password = password
        self.key_path = key_path
        self.port = port
        self._transport: paramiko.Transport | None = None
        self._sftp: paramiko.SFTPClient | None = None

    def _connect(self) -> paramiko.SFTPClient:
        if self._sftp is not None:
            return self._sftp

        self._transport = paramiko.Transport((self.host, self.port))

        if self.key_path:
            key = paramiko.RSAKey.from_private_key_file(self.key_path)
            self._transport.connect(username=self.user, pkey=key)
        else:
            self._transport.connect(
                username=self.user, password=self.password
            )

        self._sftp = paramiko.SFTPClient.from_transport(self._transport)
        return self._sftp

    def download(self, source: str, local_path: str) -> Path:
        sftp = self._connect()
        dest = Path(local_path)
        dest.parent.mkdir(parents=True, exist_ok=True)
        sftp.get(source, str(dest))
        return dest

    def upload(
        self,
        local_path: str,
        destination: str,
        site_commands: dict | None = None,
    ) -> None:
        if site_commands:
            logger.warning(
                "SFTP does not support SITE commands. "
                "Dataset allocation is SMS-managed on z/OS. "
                "Use FTP for explicit allocation control."
            )
        sftp = self._connect()
        sftp.put(local_path, destination)

    def exists(self, source: str) -> bool:
        sftp = self._connect()
        try:
            sftp.stat(source)
            return True
        except FileNotFoundError:
            return False

    def close(self) -> None:
        if self._sftp:
            self._sftp.close()
            self._sftp = None
        if self._transport:
            self._transport.close()
            self._transport = None
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/connectors/test_sftp.py -v`
Expected: All 3 tests PASS

- [ ] **Step 5: Commit**

```bash
git add ztract/connectors/sftp.py tests/connectors/test_sftp.py
git commit -m "feat: add SFTP connector using paramiko"
```

### Task 7.3: Zowe connector

**Files:**
- Create: `ztract/connectors/zowe.py`
- Create: `tests/connectors/test_zowe.py`

- [ ] **Step 1: Write failing tests (mocked subprocess)**

`tests/connectors/test_zowe.py`:
```python
"""Tests for Zowe CLI connector (mocked subprocess)."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from ztract.connectors.zowe import ZoweConnector, ZoweError


class TestZoweConnector:
    def test_download_binary(self, tmp_path: Path):
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = ""
        mock_result.stderr = ""

        dest = tmp_path / "out.dat"

        with patch("subprocess.run", return_value=mock_result) as mock_run:
            conn = ZoweConnector(profile="MYPROD")
            conn._zowe_version = "3"
            result = conn.download("BEL.CUST.DATA", str(dest))

        call_args = mock_run.call_args[0][0]
        assert "download" in " ".join(call_args)
        assert "--binary" in call_args

    def test_zowe_not_found_raises(self):
        with patch("subprocess.run", side_effect=FileNotFoundError):
            conn = ZoweConnector(profile="TEST")
            with pytest.raises(ZoweError, match="Zowe CLI not found"):
                conn.check_zowe()

    def test_version_detection(self):
        mock_result = MagicMock()
        mock_result.stdout = "3.0.0"
        mock_result.returncode = 0

        with patch("subprocess.run", return_value=mock_result):
            conn = ZoweConnector(profile="TEST")
            version = conn.check_zowe()
        assert version == "3"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/connectors/test_zowe.py -v`
Expected: FAIL

- [ ] **Step 3: Implement Zowe connector**

`ztract/connectors/zowe.py`:
```python
"""Zowe CLI connector for z/OS datasets.

Shells out to the Zowe CLI for dataset download/upload.
Detects Zowe version and uses correct command syntax.
"""

import logging
import subprocess
from pathlib import Path

from ztract.connectors.base import Connector

logger = logging.getLogger(__name__)


class ZoweError(RuntimeError):
    """Raised when Zowe CLI is missing or fails."""


class ZoweConnector(Connector):
    """Connector using Zowe CLI for z/OS dataset access."""

    def __init__(self, profile: str) -> None:
        self.profile = profile
        self._zowe_version: str | None = None

    def check_zowe(self) -> str:
        """Verify Zowe CLI is on PATH and return major version."""
        try:
            result = subprocess.run(
                ["zowe", "--version"],
                capture_output=True, text=True, timeout=10,
            )
        except FileNotFoundError:
            raise ZoweError(
                "Zowe CLI not found on PATH. "
                "Install: npm install -g @zowe/cli"
            ) from None

        version_str = result.stdout.strip()
        major = version_str.split(".")[0]
        if int(major) < 2:
            raise ZoweError(
                f"Zowe CLI v{version_str} is EOL. "
                f"Please upgrade to v2 or later."
            )
        self._zowe_version = major
        return major

    def download(self, source: str, local_path: str) -> Path:
        if not self._zowe_version:
            self.check_zowe()

        dest = Path(local_path)
        dest.parent.mkdir(parents=True, exist_ok=True)

        cmd = [
            "zowe", "zos-files", "download", "data-set",
            f'"{source}"',
            "--file", str(dest),
            "--binary",
            "--zosmf-profile", self.profile,
        ]

        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=300,
        )

        if result.returncode != 0:
            raise ZoweError(
                f"Zowe download failed: {result.stderr}"
            )

        return dest

    def upload(
        self,
        local_path: str,
        destination: str,
        site_commands: dict | None = None,
    ) -> None:
        if not self._zowe_version:
            self.check_zowe()

        cmd = [
            "zowe", "zos-files", "upload", "file-to-data-set",
            local_path,
            f'"{destination}"',
            "--binary",
            "--zosmf-profile", self.profile,
        ]

        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=300,
        )

        if result.returncode != 0:
            raise ZoweError(
                f"Zowe upload failed: {result.stderr}"
            )

    def exists(self, source: str) -> bool:
        if not self._zowe_version:
            self.check_zowe()

        cmd = [
            "zowe", "zos-files", "list", "data-set",
            f'"{source}"',
            "--zosmf-profile", self.profile,
        ]

        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=30,
        )
        return result.returncode == 0

    def list_datasets(self, pattern: str) -> list[str]:
        if not self._zowe_version:
            self.check_zowe()

        cmd = [
            "zowe", "zos-files", "list", "data-set",
            f'"{pattern}"',
            "--zosmf-profile", self.profile,
        ]

        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=30,
        )

        if result.returncode != 0:
            return []

        return [line.strip() for line in result.stdout.splitlines() if line.strip()]

    def close(self) -> None:
        pass
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/connectors/test_zowe.py -v`
Expected: All 3 tests PASS

- [ ] **Step 5: Commit**

```bash
git add ztract/connectors/zowe.py tests/connectors/test_zowe.py
git commit -m "feat: add Zowe CLI connector with version detection"
```

---

## Phase 8: YAML Config + Pipeline Orchestrator

### Task 8.1: YAML config loader

**Files:**
- Create: `ztract/config/loader.py`
- Create: `tests/test_config.py`

- [ ] **Step 1: Write failing tests**

`tests/test_config.py`:
```python
"""Tests for YAML config loader."""

import os
from pathlib import Path

import pytest

from ztract.config.loader import load_job_config, interpolate_env_vars


class TestInterpolateEnvVars:
    def test_replaces_env_var(self):
        os.environ["TEST_USER"] = "admin"
        result = interpolate_env_vars("user: ${TEST_USER}")
        assert result == "user: admin"
        del os.environ["TEST_USER"]

    def test_missing_env_var_raises(self):
        with pytest.raises(ValueError, match="MISSING_VAR"):
            interpolate_env_vars("${MISSING_VAR}")

    def test_no_vars_unchanged(self):
        result = interpolate_env_vars("plain text")
        assert result == "plain text"

    def test_multiple_vars(self):
        os.environ["A"] = "1"
        os.environ["B"] = "2"
        result = interpolate_env_vars("${A} and ${B}")
        assert result == "1 and 2"
        del os.environ["A"]
        del os.environ["B"]


class TestLoadJobConfig:
    def test_loads_simple_job(self, tmp_path: Path):
        job_yaml = tmp_path / "job.yaml"
        job_yaml.write_text("""
version: "1.0"
job:
  name: test-job
steps:
  - name: extract
    action: convert
    copybook: ./CUST.cpy
    input:
      dataset: CUST.DAT
      record_format: FB
      lrecl: 500
      codepage: cp277
    output:
      - type: csv
        path: ./out.csv
""")
        config = load_job_config(job_yaml)
        assert config["job"]["name"] == "test-job"
        assert len(config["steps"]) == 1
        assert config["steps"][0]["action"] == "convert"

    def test_env_var_interpolation(self, tmp_path: Path):
        os.environ["MY_HOST"] = "mainframe.bank.com"
        job_yaml = tmp_path / "job.yaml"
        job_yaml.write_text("""
version: "1.0"
job:
  name: test
connections:
  prod:
    host: ${MY_HOST}
steps: []
""")
        config = load_job_config(job_yaml)
        assert config["connections"]["prod"]["host"] == "mainframe.bank.com"
        del os.environ["MY_HOST"]

    def test_dotenv_loaded(self, tmp_path: Path):
        env_file = tmp_path / ".env"
        env_file.write_text("SECRET_USER=admin\n")
        job_yaml = tmp_path / "job.yaml"
        job_yaml.write_text("""
version: "1.0"
job:
  name: test
steps:
  - name: s1
    action: convert
    user: ${SECRET_USER}
""")
        config = load_job_config(job_yaml)
        assert config["steps"][0]["user"] == "admin"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_config.py -v`
Expected: FAIL

- [ ] **Step 3: Implement config loader**

`ztract/config/loader.py`:
```python
"""YAML job config loader with env var interpolation.

Supports ${VAR_NAME} syntax, .env file loading, and YAML anchors.
"""

import os
import re
from pathlib import Path

import yaml


def _load_dotenv(directory: Path) -> None:
    """Load .env file from directory into os.environ."""
    env_file = directory / ".env"
    if not env_file.exists():
        return
    for line in env_file.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip().strip("'\"")
        os.environ.setdefault(key, value)


def interpolate_env_vars(text: str) -> str:
    """Replace ${VAR_NAME} with environment variable values.

    Raises:
        ValueError: If a referenced variable is not set.
    """
    def replacer(match: re.Match) -> str:
        var_name = match.group(1)
        value = os.environ.get(var_name)
        if value is None:
            raise ValueError(
                f"Environment variable '{var_name}' is not set. "
                f"Set it or add it to your .env file."
            )
        return value

    return re.sub(r"\$\{([^}]+)\}", replacer, text)


def load_job_config(path: Path) -> dict:
    """Load and parse a YAML job configuration file.

    1. Loads .env from the job file's directory
    2. Reads YAML with anchor support
    3. Interpolates ${VAR_NAME} references

    Args:
        path: Path to the YAML job file.

    Returns:
        Parsed and interpolated config dict.
    """
    path = Path(path)
    _load_dotenv(path.parent)

    raw_text = path.read_text(encoding="utf-8")
    interpolated = interpolate_env_vars(raw_text)
    config = yaml.safe_load(interpolated)

    return config
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_config.py -v`
Expected: All 6 tests PASS

- [ ] **Step 5: Commit**

```bash
git add ztract/config/loader.py tests/test_config.py
git commit -m "feat: add YAML config loader with env var interpolation"
```

### Task 8.2: Config schema validation

**Files:**
- Create: `ztract/config/schema.py`

- [ ] **Step 1: Write failing tests**

Append to `tests/test_config.py`:
```python
from ztract.config.schema import validate_job_config, ConfigError


class TestValidateJobConfig:
    def test_valid_convert_step(self):
        config = {
            "version": "1.0",
            "job": {"name": "test"},
            "steps": [
                {
                    "name": "extract",
                    "action": "convert",
                    "copybook": "CUST.cpy",
                    "input": {
                        "record_format": "FB",
                        "lrecl": 500,
                        "codepage": "cp277",
                    },
                    "output": [{"type": "csv", "path": "out.csv"}],
                }
            ],
        }
        validate_job_config(config)  # should not raise

    def test_missing_copybook_raises(self):
        config = {
            "version": "1.0",
            "job": {"name": "test"},
            "steps": [
                {
                    "name": "extract",
                    "action": "convert",
                    "input": {"record_format": "FB"},
                    "output": [{"type": "csv", "path": "out.csv"}],
                }
            ],
        }
        with pytest.raises(ConfigError, match="copybook"):
            validate_job_config(config)

    def test_invalid_codepage_raises(self):
        config = {
            "version": "1.0",
            "job": {"name": "test"},
            "steps": [
                {
                    "name": "extract",
                    "action": "convert",
                    "copybook": "CUST.cpy",
                    "input": {
                        "record_format": "FB",
                        "codepage": "cp999",
                    },
                    "output": [{"type": "csv", "path": "out.csv"}],
                }
            ],
        }
        with pytest.raises(ConfigError, match="codepage"):
            validate_job_config(config)

    def test_missing_job_name_raises(self):
        config = {"version": "1.0", "steps": []}
        with pytest.raises(ConfigError, match="job"):
            validate_job_config(config)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_config.py::TestValidateJobConfig -v`
Expected: FAIL

- [ ] **Step 3: Implement schema validation**

`ztract/config/schema.py`:
```python
"""YAML job config schema validation.

Validates structure, required fields, codepages, and record formats.
Provides clear error messages with step names and field paths.
"""

from ztract.codepages import resolve_codepage, CodepageError
from ztract.connectors.dataset_format import RecordFormat, DatasetFormatError


class ConfigError(ValueError):
    """Raised for invalid job configuration."""


def validate_job_config(config: dict) -> None:
    """Validate a parsed YAML job config.

    Raises:
        ConfigError: With clear error message for any validation failure.
    """
    if "job" not in config or "name" not in config.get("job", {}):
        raise ConfigError(
            "Missing required 'job.name' in config. "
            "Every job file must have a job section with a name."
        )

    steps = config.get("steps", [])
    for i, step in enumerate(steps):
        step_name = step.get("name", f"step-{i}")
        _validate_step(step, step_name)


def _validate_step(step: dict, step_name: str) -> None:
    """Validate a single pipeline step."""
    action = step.get("action")
    if not action:
        raise ConfigError(
            f"Step '{step_name}': missing required field 'action'"
        )

    if action == "convert":
        _validate_convert_step(step, step_name)
    elif action == "diff":
        _validate_diff_step(step, step_name)
    elif action == "generate":
        _validate_generate_step(step, step_name)
    elif action in ("upload",):
        pass  # minimal validation
    else:
        raise ConfigError(
            f"Step '{step_name}': unknown action '{action}'. "
            f"Valid actions: convert, diff, generate, upload"
        )


def _validate_convert_step(step: dict, step_name: str) -> None:
    if "copybook" not in step:
        raise ConfigError(
            f"Step '{step_name}': missing required field 'copybook'"
        )

    input_cfg = step.get("input", {})
    codepage = input_cfg.get("codepage")
    if codepage:
        try:
            resolve_codepage(codepage)
        except CodepageError:
            raise ConfigError(
                f"Step '{step_name}': invalid codepage '{codepage}'"
            ) from None

    recfm = input_cfg.get("record_format")
    if recfm:
        try:
            RecordFormat.from_str(recfm)
        except DatasetFormatError:
            raise ConfigError(
                f"Step '{step_name}': invalid record_format '{recfm}'"
            ) from None


def _validate_diff_step(step: dict, step_name: str) -> None:
    if "copybook" not in step:
        raise ConfigError(
            f"Step '{step_name}': missing required field 'copybook'"
        )


def _validate_generate_step(step: dict, step_name: str) -> None:
    if "copybook" not in step:
        raise ConfigError(
            f"Step '{step_name}': missing required field 'copybook'"
        )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_config.py -v`
Expected: All 10 tests PASS

- [ ] **Step 5: Commit**

```bash
git add ztract/config/schema.py tests/test_config.py
git commit -m "feat: add YAML config schema validation"
```

### Task 8.3: Pipeline orchestrator + step context + run CLI

**Files:**
- Create: `ztract/pipeline/orchestrator.py`
- Create: `ztract/pipeline/step_context.py`
- Create: `ztract/cli/run.py`
- Modify: `ztract/cli/root.py`
- Create: `tests/pipeline/test_orchestrator.py`

- [ ] **Step 1: Write failing tests**

`tests/pipeline/test_orchestrator.py`:
```python
"""Tests for pipeline orchestrator."""

from pathlib import Path

import pytest

from ztract.pipeline.step_context import StepContext


class TestStepContext:
    def test_expose_and_resolve(self):
        ctx = StepContext()
        ctx.expose("prod_data", "csv", Path("/tmp/prod.csv"))
        resolved = ctx.resolve_ref("$ref:prod_data.csv")
        assert resolved == Path("/tmp/prod.csv")

    def test_resolve_unknown_raises(self):
        ctx = StepContext()
        with pytest.raises(KeyError, match="test_data"):
            ctx.resolve_ref("$ref:test_data.csv")

    def test_track_timing(self):
        ctx = StepContext()
        ctx.start_step("extract")
        ctx.end_step("extract")
        assert ctx.get_elapsed("extract") >= 0

    def test_reject_count(self):
        ctx = StepContext()
        ctx.add_rejects("step1", 5)
        ctx.add_rejects("step2", 3)
        assert ctx.total_rejects == 8
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/pipeline/test_orchestrator.py -v`
Expected: FAIL

- [ ] **Step 3: Implement step context**

`ztract/pipeline/step_context.py`:
```python
"""Pipeline step context — runtime state for a pipeline execution.

Holds step outputs ($ref resolution), connection pool,
reject counts, and timing.
"""

import time
from pathlib import Path

from ztract.connectors.base import Connector


class StepContext:
    """Runtime context shared across pipeline steps."""

    def __init__(self) -> None:
        self._outputs: dict[str, dict[str, Path]] = {}
        self._connections: dict[str, Connector] = {}
        self._rejects: dict[str, int] = {}
        self._timings: dict[str, dict[str, float]] = {}
        self._temp_files: list[Path] = []

    def expose(self, name: str, output_type: str, path: Path) -> None:
        """Register a step output for $ref resolution."""
        if name not in self._outputs:
            self._outputs[name] = {}
        self._outputs[name][output_type] = path

    def resolve_ref(self, ref: str) -> Path:
        """Resolve a $ref:step_name.output_type reference.

        Args:
            ref: Reference string like "$ref:prod_data.csv"

        Returns:
            Path to the referenced output file.

        Raises:
            KeyError: If the reference cannot be resolved.
        """
        # Parse "$ref:step_name.output_type"
        ref_body = ref.replace("$ref:", "")
        parts = ref_body.rsplit(".", 1)
        if len(parts) != 2:
            raise KeyError(f"Invalid $ref format: '{ref}'. Expected '$ref:name.type'")

        step_name, output_type = parts
        if step_name not in self._outputs:
            raise KeyError(
                f"Step output '{step_name}' not found. "
                f"Available: {list(self._outputs.keys())}"
            )
        if output_type not in self._outputs[step_name]:
            raise KeyError(
                f"Output type '{output_type}' not found for step '{step_name}'. "
                f"Available: {list(self._outputs[step_name].keys())}"
            )
        return self._outputs[step_name][output_type]

    def get_connector(self, uri: str, factory) -> Connector:
        """Get or create a connector, reusing connections to the same host."""
        if uri in self._connections:
            return self._connections[uri]
        conn = factory()
        self._connections[uri] = conn
        return conn

    def start_step(self, step_name: str) -> None:
        self._timings[step_name] = {"start": time.monotonic()}

    def end_step(self, step_name: str) -> None:
        if step_name in self._timings:
            self._timings[step_name]["end"] = time.monotonic()

    def get_elapsed(self, step_name: str) -> float:
        t = self._timings.get(step_name, {})
        start = t.get("start", 0)
        end = t.get("end", time.monotonic())
        return end - start

    def add_rejects(self, step_name: str, count: int) -> None:
        self._rejects[step_name] = self._rejects.get(step_name, 0) + count

    @property
    def total_rejects(self) -> int:
        return sum(self._rejects.values())

    def register_temp(self, path: Path) -> None:
        self._temp_files.append(path)

    def close(self) -> None:
        """Clean up connections and temp files."""
        for conn in self._connections.values():
            try:
                conn.close()
            except Exception:
                pass
        self._connections.clear()

        for temp in self._temp_files:
            try:
                if temp.exists():
                    temp.unlink()
            except Exception:
                pass
        self._temp_files.clear()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/pipeline/test_orchestrator.py -v`
Expected: All 4 tests PASS

- [ ] **Step 5: Implement pipeline orchestrator**

`ztract/pipeline/orchestrator.py`:
```python
"""Pipeline orchestrator — executes multi-step YAML jobs.

Iterates steps sequentially, resolves $ref inputs, manages
connectors and writers, and reports overall job status.
"""

import logging
import sys
from pathlib import Path

from ztract.config.loader import load_job_config
from ztract.config.schema import validate_job_config
from ztract.pipeline.step_context import StepContext

logger = logging.getLogger(__name__)


class PipelineOrchestrator:
    """Executes a multi-step YAML pipeline."""

    def __init__(self, config: dict, step_filter: str | None = None,
                 dry_run: bool = False) -> None:
        self.config = config
        self.step_filter = step_filter
        self.dry_run = dry_run
        self.context = StepContext()

    def run(self) -> int:
        """Execute the pipeline. Returns exit code (0, 1, or 2)."""
        steps = self.config.get("steps", [])
        job_name = self.config.get("job", {}).get("name", "unnamed")

        if self.step_filter:
            steps = [s for s in steps if s["name"] == self.step_filter]
            if not steps:
                logger.error("Step '%s' not found in job '%s'",
                             self.step_filter, job_name)
                return 1

        if self.dry_run:
            logger.info("DRY RUN — validating %d steps", len(steps))
            for step in steps:
                logger.info("  Step: %s (action: %s)",
                            step["name"], step["action"])
            return 0

        continue_on_error = self.config.get("job", {}).get(
            "continue_on_error", False
        )
        exit_code = 0

        try:
            for step in steps:
                self.context.start_step(step["name"])
                try:
                    self._execute_step(step)
                except Exception as e:
                    logger.error("Step '%s' failed: %s", step["name"], e)
                    self.context.end_step(step["name"])
                    if continue_on_error:
                        exit_code = max(exit_code, 1)
                        continue
                    return 1
                self.context.end_step(step["name"])
        finally:
            self.context.close()

        if self.context.total_rejects > 0:
            exit_code = max(exit_code, 2)

        return exit_code

    def _execute_step(self, step: dict) -> None:
        """Execute a single pipeline step."""
        action = step["action"]
        step_name = step["name"]
        logger.info("Executing step: %s (action: %s)", step_name, action)

        if action == "convert":
            self._execute_convert(step)
        elif action == "diff":
            self._execute_diff(step)
        elif action == "generate":
            self._execute_generate(step)
        elif action == "upload":
            self._execute_upload(step)
        else:
            raise ValueError(f"Unknown action: {action}")

    def _execute_convert(self, step: dict) -> None:
        """Execute a convert step. Implementation delegates to convert logic."""
        # This will be wired to the full convert pipeline in integration
        logger.info("Convert step: %s", step["name"])

    def _execute_diff(self, step: dict) -> None:
        logger.info("Diff step: %s", step["name"])

    def _execute_generate(self, step: dict) -> None:
        logger.info("Generate step: %s", step["name"])

    def _execute_upload(self, step: dict) -> None:
        logger.info("Upload step: %s", step["name"])
```

- [ ] **Step 6: Implement run CLI command**

`ztract/cli/run.py`:
```python
"""ztract run — execute a YAML pipeline job."""

import logging
import sys

import click

from ztract.config.loader import load_job_config
from ztract.config.schema import validate_job_config, ConfigError
from ztract.pipeline.orchestrator import PipelineOrchestrator

logger = logging.getLogger(__name__)


@click.command()
@click.argument("job_file", type=click.Path(exists=True))
@click.option("--dry-run", is_flag=True, help="Validate and show plan without executing.")
@click.option("--step", "step_name", default=None, help="Run only this named step.")
@click.pass_context
def run(ctx: click.Context, job_file: str, dry_run: bool, step_name: str | None) -> None:
    """Execute a multi-step YAML pipeline job."""
    from pathlib import Path

    try:
        config = load_job_config(Path(job_file))
        validate_job_config(config)
    except (ConfigError, ValueError) as e:
        click.echo(f"Config error: {e}", err=True)
        sys.exit(1)

    orchestrator = PipelineOrchestrator(
        config=config,
        step_filter=step_name,
        dry_run=dry_run,
    )

    exit_code = orchestrator.run()

    if exit_code == 0:
        click.echo("Job completed successfully.")
    elif exit_code == 2:
        click.echo("Job completed with rejects.")
    else:
        click.echo("Job failed.", err=True)

    sys.exit(exit_code)
```

- [ ] **Step 7: Register run command in root**

Add to `ztract/cli/root.py`:
```python
from ztract.cli.run import run

cli.add_command(run)
```

- [ ] **Step 8: Commit**

```bash
git add ztract/pipeline/step_context.py ztract/pipeline/orchestrator.py \
       ztract/cli/run.py ztract/cli/root.py tests/pipeline/test_orchestrator.py
git commit -m "feat: add pipeline orchestrator, step context, and run CLI"
```

---

## Phase 9: Diff

### Task 9.1: Differ using daff

**Files:**
- Create: `ztract/diff/differ.py`
- Create: `tests/diff/test_differ.py`

- [ ] **Step 1: Write failing tests**

`tests/diff/test_differ.py`:
```python
"""Tests for the daff-based differ."""

import json
from pathlib import Path

import pytest

from ztract.diff.differ import Differ, DiffResult


class TestDiffer:
    def test_identical_files(self, tmp_path: Path):
        before = tmp_path / "before.jsonl"
        after = tmp_path / "after.jsonl"
        records = [
            json.dumps({"CUST-ID": "001", "CUST-NAME": "Test"}),
            json.dumps({"CUST-ID": "002", "CUST-NAME": "Other"}),
        ]
        before.write_text("\n".join(records) + "\n")
        after.write_text("\n".join(records) + "\n")

        differ = Differ(key_fields=["CUST-ID"])
        result = differ.diff_jsonl(before, after)
        assert result.added == 0
        assert result.deleted == 0
        assert result.changed == 0

    def test_added_record(self, tmp_path: Path):
        before = tmp_path / "before.jsonl"
        after = tmp_path / "after.jsonl"
        before.write_text(json.dumps({"ID": "1", "NAME": "A"}) + "\n")
        after.write_text(
            json.dumps({"ID": "1", "NAME": "A"}) + "\n" +
            json.dumps({"ID": "2", "NAME": "B"}) + "\n"
        )

        differ = Differ(key_fields=["ID"])
        result = differ.diff_jsonl(before, after)
        assert result.added == 1
        assert result.deleted == 0

    def test_deleted_record(self, tmp_path: Path):
        before = tmp_path / "before.jsonl"
        after = tmp_path / "after.jsonl"
        before.write_text(
            json.dumps({"ID": "1", "NAME": "A"}) + "\n" +
            json.dumps({"ID": "2", "NAME": "B"}) + "\n"
        )
        after.write_text(json.dumps({"ID": "1", "NAME": "A"}) + "\n")

        differ = Differ(key_fields=["ID"])
        result = differ.diff_jsonl(before, after)
        assert result.added == 0
        assert result.deleted == 1

    def test_changed_record(self, tmp_path: Path):
        before = tmp_path / "before.jsonl"
        after = tmp_path / "after.jsonl"
        before.write_text(json.dumps({"ID": "1", "NAME": "A"}) + "\n")
        after.write_text(json.dumps({"ID": "1", "NAME": "B"}) + "\n")

        differ = Differ(key_fields=["ID"])
        result = differ.diff_jsonl(before, after)
        assert result.changed == 1
        assert len(result.changes) == 1
        assert result.changes[0]["ID"] == "1"

    def test_no_key_ordinal_match(self, tmp_path: Path):
        before = tmp_path / "before.jsonl"
        after = tmp_path / "after.jsonl"
        before.write_text(json.dumps({"NAME": "A"}) + "\n")
        after.write_text(json.dumps({"NAME": "B"}) + "\n")

        differ = Differ(key_fields=[])
        result = differ.diff_jsonl(before, after)
        assert result.changed == 1
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/diff/test_differ.py -v`
Expected: FAIL

- [ ] **Step 3: Implement differ**

`ztract/diff/differ.py`:
```python
"""Field-level diff using daff for tabular comparison.

Decodes two EBCDIC files to JSONL, then uses daff to compare
them field-by-field with key-based record matching.
"""

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass
class DiffResult:
    """Result of a field-level diff."""

    added: int = 0
    deleted: int = 0
    changed: int = 0
    unchanged: int = 0
    total_before: int = 0
    total_after: int = 0
    changes: list[dict] = field(default_factory=list)
    additions: list[dict] = field(default_factory=list)
    deletions: list[dict] = field(default_factory=list)


class Differ:
    """Compares two JSONL files field-by-field.

    Uses key fields for record matching. Falls back to ordinal
    position if no keys specified.
    """

    def __init__(
        self,
        key_fields: list[str] | None = None,
        show_unchanged: bool = False,
    ) -> None:
        self.key_fields = key_fields or []
        self.show_unchanged = show_unchanged

    def diff_jsonl(self, before_path: Path, after_path: Path) -> DiffResult:
        """Diff two JSONL files.

        Args:
            before_path: Path to before JSONL file.
            after_path: Path to after JSONL file.

        Returns:
            DiffResult with counts and change details.
        """
        before_records = self._load_jsonl(before_path)
        after_records = self._load_jsonl(after_path)

        result = DiffResult(
            total_before=len(before_records),
            total_after=len(after_records),
        )

        if self.key_fields:
            self._diff_by_key(before_records, after_records, result)
        else:
            if len(before_records) != len(after_records):
                logger.warning(
                    "Files have different record counts "
                    "(before: %d, after: %d). Using ordinal matching — "
                    "extra records shown as ADDED/DELETED.",
                    len(before_records), len(after_records),
                )
            self._diff_by_ordinal(before_records, after_records, result)

        return result

    def _diff_by_key(
        self,
        before: list[dict],
        after: list[dict],
        result: DiffResult,
    ) -> None:
        """Diff records using key fields for matching."""
        before_map = {self._key(r): r for r in before}
        after_map = {self._key(r): r for r in after}

        before_keys = set(before_map.keys())
        after_keys = set(after_map.keys())

        # Deleted: in before but not after
        for key in before_keys - after_keys:
            result.deleted += 1
            result.deletions.append(before_map[key])

        # Added: in after but not before
        for key in after_keys - before_keys:
            result.added += 1
            result.additions.append(after_map[key])

        # Changed or unchanged: in both
        for key in before_keys & after_keys:
            b_rec = before_map[key]
            a_rec = after_map[key]
            if b_rec != a_rec:
                result.changed += 1
                change = {f: b_rec.get(f) for f in self.key_fields}
                change["_before"] = {
                    k: v for k, v in b_rec.items() if b_rec.get(k) != a_rec.get(k)
                }
                change["_after"] = {
                    k: v for k, v in a_rec.items() if b_rec.get(k) != a_rec.get(k)
                }
                result.changes.append(change)
            else:
                result.unchanged += 1

    def _diff_by_ordinal(
        self,
        before: list[dict],
        after: list[dict],
        result: DiffResult,
    ) -> None:
        """Diff records by ordinal position."""
        min_len = min(len(before), len(after))

        for i in range(min_len):
            if before[i] != after[i]:
                result.changed += 1
                change = {"_record_num": i}
                change["_before"] = {
                    k: v for k, v in before[i].items()
                    if before[i].get(k) != after[i].get(k)
                }
                change["_after"] = {
                    k: v for k, v in after[i].items()
                    if before[i].get(k) != after[i].get(k)
                }
                result.changes.append(change)
            else:
                result.unchanged += 1

        # Extra records
        if len(after) > min_len:
            result.added += len(after) - min_len
            result.additions.extend(after[min_len:])
        if len(before) > min_len:
            result.deleted += len(before) - min_len
            result.deletions.extend(before[min_len:])

    def _key(self, record: dict) -> tuple:
        """Extract key tuple from record."""
        return tuple(record.get(k) for k in self.key_fields)

    @staticmethod
    def _load_jsonl(path: Path) -> list[dict]:
        """Load a JSONL file into a list of dicts."""
        records = []
        with open(path, encoding="utf-8") as f:
            for line in f:
                stripped = line.strip()
                if stripped:
                    records.append(json.loads(stripped))
        return records
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/diff/test_differ.py -v`
Expected: All 5 tests PASS

- [ ] **Step 5: Implement diff CLI command**

`ztract/cli/diff.py`:
```python
"""ztract diff — field-level comparison of two EBCDIC files."""

import sys
from pathlib import Path

import click

from ztract.codepages import resolve_codepage
from ztract.diff.differ import Differ


@click.command()
@click.option("--copybook", required=True, type=click.Path(exists=True))
@click.option("--before", required=True, type=click.Path(exists=True))
@click.option("--after", required=True, type=click.Path(exists=True))
@click.option("--key", "key_fields", multiple=True, help="Key field(s) for matching")
@click.option("--recfm", required=True)
@click.option("--lrecl", type=int, default=None)
@click.option("--codepage", default="cp037")
@click.option("--format", "fmt", default="console",
              type=click.Choice(["console", "csv", "json"]))
@click.pass_context
def diff(ctx, copybook, before, after, key_fields, recfm, lrecl, codepage, fmt):
    """Compare two EBCDIC binary files field-by-field."""
    from ztract.engine.bridge import ZtractBridge, EngineError
    import json
    import tempfile

    resolved_cp = resolve_codepage(codepage)
    jar_path = Path(__file__).parent.parent / "engine" / "ztract-engine.jar"
    bridge = ZtractBridge(jar_path=jar_path)

    try:
        bridge.check_jre()
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)

    # Decode both files to temp JSONL
    with tempfile.TemporaryDirectory() as tmpdir:
        before_jsonl = Path(tmpdir) / "before.jsonl"
        after_jsonl = Path(tmpdir) / "after.jsonl"

        for label, input_path, output_path in [
            ("before", before, before_jsonl),
            ("after", after, after_jsonl),
        ]:
            click.echo(f"Decoding {label} file...")
            try:
                records = bridge.decode(
                    Path(copybook), Path(input_path),
                    recfm, lrecl, resolved_cp,
                )
                with open(output_path, "w", encoding="utf-8") as f:
                    for rec in records:
                        f.write(json.dumps(rec, ensure_ascii=False) + "\n")
            except EngineError as e:
                click.echo(f"Error decoding {label}: {e}", err=True)
                sys.exit(1)

        # Run diff
        differ = Differ(key_fields=list(key_fields))
        result = differ.diff_jsonl(before_jsonl, after_jsonl)

    # Output
    click.echo(
        f"\nDiff complete: {result.added} added - {result.deleted} deleted - "
        f"{result.changed} changed - {result.unchanged} unchanged"
    )
```

Register in `ztract/cli/root.py`:
```python
from ztract.cli.diff import diff

cli.add_command(diff)
```

- [ ] **Step 6: Commit**

```bash
git add ztract/diff/differ.py ztract/cli/diff.py ztract/cli/root.py \
       tests/diff/test_differ.py
git commit -m "feat: add field-level diff with key-based matching"
```

---

## Phase 10: Mock Data Generator + EBCDIC Writer

### Task 10.1: Field patterns for mock data

**Files:**
- Create: `ztract/generate/field_patterns.py`
- Create: `tests/generate/test_generator.py`

- [ ] **Step 1: Write failing tests**

`tests/generate/test_generator.py`:
```python
"""Tests for mock data field pattern matching."""

import pytest
from faker import Faker

from ztract.generate.field_patterns import get_generator, generate_value


class TestGetGenerator:
    def test_name_field(self):
        gen = get_generator("CUST-NAME", "ALPHANUMERIC", 50)
        assert gen is not None

    def test_norwegian_name_field(self):
        gen = get_generator("KUNDE-NAVN", "ALPHANUMERIC", 50)
        assert gen is not None

    def test_amount_field(self):
        gen = get_generator("TRANS-AMT", "NUMERIC", 9)
        assert gen is not None

    def test_date_field(self):
        gen = get_generator("START-DATE", "NUMERIC", 8)
        assert gen is not None

    def test_fallback_alpha(self):
        gen = get_generator("UNKNOWN-FIELD", "ALPHANUMERIC", 20)
        assert gen is not None

    def test_fallback_numeric(self):
        gen = get_generator("UNKNOWN-FIELD", "NUMERIC", 5)
        assert gen is not None


class TestGenerateValue:
    def test_alpha_fits_size(self):
        value = generate_value("CUST-NAME", "ALPHANUMERIC", 10, 0, "en_US", 42)
        assert len(str(value)) <= 10

    def test_numeric_fits_size(self):
        value = generate_value("CUST-ID", "NUMERIC", 5, 0, "en_US", 42)
        assert len(str(value)) <= 5

    def test_norwegian_locale(self):
        # Just verify it doesn't crash with nb_NO
        value = generate_value("KUNDE-NAVN", "ALPHANUMERIC", 30, 0, "nb_NO", 42)
        assert isinstance(value, str)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/generate/test_generator.py -v`
Expected: FAIL

- [ ] **Step 3: Implement field patterns**

`ztract/generate/field_patterns.py`:
```python
"""Field-name-aware data generators for mock EBCDIC data.

Maps COBOL field names to appropriate Faker generators based on
pattern matching. Falls back to type-correct random values.
"""

import random
import re
from collections.abc import Callable
from datetime import datetime, timedelta

from faker import Faker

# Pattern → generator factory
# Each entry: (regex_pattern, generator_function)
_ALPHA_PATTERNS: list[tuple[str, Callable]] = [
    (r"(NAME|NAVN)", lambda f, sz: f.name()[:sz]),
    (r"(ADDR|ADRESSE)", lambda f, sz: f.street_address()[:sz]),
    (r"(CITY|BY)", lambda f, sz: f.city()[:sz]),
    (r"(PHONE|TELEFON|TLF)", lambda f, sz: f.phone_number()[:sz]),
    (r"EMAIL", lambda f, sz: f.email()[:sz]),
    (r"(ZIP|POST)", lambda f, sz: f.postcode()[:sz]),
    (r"(COUNTRY|LAND)", lambda f, sz: f.country()[:sz]),
    (r"(DESC|TEXT|BESKR)", lambda f, sz: f.sentence()[:sz]),
    (r"(CODE|KODE)", lambda f, sz: f.bothify("?" * min(sz, 8))[:sz]),
]

_NUMERIC_PATTERNS: list[tuple[str, Callable]] = [
    (r"(AMT|AMOUNT|BELOP|BELOEP)", lambda _f, sz, sc: round(random.uniform(100, 999999), sc)),
    (r"(DATE|DATO)", lambda _f, sz, _sc: int((datetime.now() - timedelta(days=random.randint(0, 3650))).strftime("%Y%m%d"))),
    (r"(ID|NR|NUM)", lambda _f, sz, _sc: random.randint(1, 10 ** min(sz, 9) - 1)),
]


def get_generator(
    field_name: str,
    field_type: str,
    size: int,
) -> Callable | None:
    """Find a matching generator for a field.

    Returns a callable or None if no pattern matches (uses fallback).
    """
    upper_name = field_name.upper()

    if field_type == "ALPHANUMERIC":
        for pattern, gen in _ALPHA_PATTERNS:
            if re.search(pattern, upper_name):
                return gen
    else:
        for pattern, gen in _NUMERIC_PATTERNS:
            if re.search(pattern, upper_name):
                return gen

    return None


def generate_value(
    field_name: str,
    field_type: str,
    size: int,
    scale: int,
    locale: str,
    seed: int | None,
) -> str | int | float:
    """Generate a single value for a field.

    Uses field-name pattern matching, falls back to type-correct random.

    Args:
        field_name: COBOL field name.
        field_type: Field type (ALPHANUMERIC, NUMERIC, PACKED_DECIMAL).
        size: Field size in bytes/digits.
        scale: Decimal scale.
        locale: Faker locale (e.g., "nb_NO").
        seed: Random seed (None for non-deterministic).

    Returns:
        Generated value, truncated/padded to fit field size.
    """
    fake = Faker(locale)
    if seed is not None:
        Faker.seed(seed)
        random.seed(seed)

    gen = get_generator(field_name, field_type, size)

    if field_type == "ALPHANUMERIC":
        if gen:
            value = gen(fake, size)
        else:
            value = fake.lexify("?" * size)
        # Pad or truncate to exact size
        return value.ljust(size)[:size]

    else:  # NUMERIC, PACKED_DECIMAL
        if gen:
            value = gen(fake, size, scale)
        else:
            max_val = 10 ** size - 1
            if scale > 0:
                value = round(random.uniform(0, max_val / (10 ** scale)), scale)
            else:
                value = random.randint(0, max_val)

        # Ensure numeric value fits in PIC size
        str_val = str(abs(int(value))) if scale == 0 else str(value)
        if len(str_val.replace(".", "").replace("-", "")) > size + scale:
            if scale > 0:
                value = round(value % (10 ** size), scale)
            else:
                value = int(value) % (10 ** size)

        return value
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/generate/test_generator.py -v`
Expected: All 9 tests PASS

- [ ] **Step 5: Commit**

```bash
git add ztract/generate/field_patterns.py tests/generate/test_generator.py
git commit -m "feat: add field-name-aware mock data patterns"
```

### Task 10.2: Generator + EBCDIC writer + generate CLI

**Files:**
- Create: `ztract/generate/generator.py`
- Create: `ztract/writers/ebcdic.py`
- Create: `ztract/cli/generate.py`
- Modify: `ztract/cli/root.py`

- [ ] **Step 1: Implement EBCDIC writer**

`ztract/writers/ebcdic.py`:
```python
"""EBCDIC writer — reverse path through Java encode mode.

Accepts record dicts and streams them as JSON Lines to the
Java engine's encode mode, which writes raw EBCDIC binary.
"""

import time
from collections.abc import Iterator
from pathlib import Path

from ztract.engine.bridge import ZtractBridge
from ztract.writers.base import Writer, WriterStats


class EBCDICWriter(Writer):
    """Writes records to EBCDIC binary via Java encode mode.

    Python never touches EBCDIC bytes — all encoding is done
    by the Java engine (Cobrix).
    """

    def __init__(
        self,
        output_path: Path,
        bridge: ZtractBridge,
        copybook: Path,
        recfm: str,
        lrecl: int | None,
        codepage: str,
    ) -> None:
        self.output_path = Path(output_path)
        self.bridge = bridge
        self.copybook = copybook
        self.recfm = recfm
        self.lrecl = lrecl
        self.codepage = codepage
        self._records: list[dict] = []
        self._count = 0
        self._start_time = 0.0

    @property
    def name(self) -> str:
        return f"EBCDIC → {self.output_path.name}"

    def open(self, schema: dict) -> None:
        self._start_time = time.monotonic()

    def write_batch(self, records: list[dict]) -> int:
        self._records.extend(records)
        return len(records)

    def close(self) -> WriterStats:
        # Encode all records via Java
        self._count = self.bridge.encode(
            self.copybook,
            self.output_path,
            self.recfm,
            self.lrecl,
            self.codepage,
            iter(self._records),
        )
        elapsed = time.monotonic() - self._start_time
        self._records = []
        return WriterStats(records_written=self._count, elapsed_sec=elapsed)
```

- [ ] **Step 2: Implement generator**

`ztract/generate/generator.py`:
```python
"""Mock data generator — creates synthetic EBCDIC test data.

Reads copybook schema, generates field-name-aware random data
using Faker, streams to Java encode mode for EBCDIC output.
"""

import logging
import random
from collections.abc import Iterator
from pathlib import Path

from faker import Faker

from ztract.engine.bridge import ZtractBridge
from ztract.generate.field_patterns import generate_value

logger = logging.getLogger(__name__)

# Codepage to Faker locale mapping
_CODEPAGE_LOCALES = {
    "cp277": "nb_NO",
    "cp273": "de_DE",
    "cp037": "en_US",
    "cp875": "el_GR",
    "cp870": "pl_PL",
    "cp1047": "en_US",
    "cp838": "th_TH",
    "cp1025": "ru_RU",
}


def generate_records(
    schema: dict,
    count: int,
    codepage: str = "cp037",
    seed: int | None = None,
) -> Iterator[dict]:
    """Generate synthetic records from a copybook schema.

    Args:
        schema: Parsed schema dict from bridge.get_schema().
        count: Number of records to generate.
        codepage: EBCDIC codepage for locale selection.
        seed: Random seed for reproducibility.

    Yields:
        Record dicts with generated field values.
    """
    locale = _CODEPAGE_LOCALES.get(codepage, "en_US")

    if seed is not None:
        Faker.seed(seed)
        random.seed(seed)

    fields = [f for f in schema.get("fields", []) if not f["name"].startswith("FILLER")]

    for i in range(count):
        record = {}
        # Use per-record seed offset for reproducibility with variety
        record_seed = (seed + i) if seed is not None else None

        for field_def in fields:
            if field_def.get("occurs"):
                # Generate array for OCCURS fields
                occurs_count = field_def["occurs"]
                array = []
                for _ in range(occurs_count):
                    # For simplicity, generate a simple value per element
                    val = generate_value(
                        field_def["name"],
                        field_def.get("type", "ALPHANUMERIC"),
                        field_def.get("size", 10),
                        field_def.get("scale", 0),
                        locale,
                        record_seed,
                    )
                    array.append(val)
                record[field_def["name"]] = array
            else:
                record[field_def["name"]] = generate_value(
                    field_def["name"],
                    field_def.get("type", "ALPHANUMERIC"),
                    field_def.get("size", 10),
                    field_def.get("scale", 0),
                    locale,
                    record_seed,
                )
        yield record
```

- [ ] **Step 3: Implement generate CLI**

`ztract/cli/generate.py`:
```python
"""ztract generate — create synthetic EBCDIC test data."""

import sys
from pathlib import Path

import click

from ztract.codepages import resolve_codepage


@click.command()
@click.option("--copybook", required=True, type=click.Path(exists=True))
@click.option("--records", required=True, type=int, help="Number of records")
@click.option("--output", required=True, type=click.Path())
@click.option("--codepage", default="cp037")
@click.option("--recfm", required=True)
@click.option("--lrecl", type=int, default=None)
@click.option("--seed", type=int, default=None, help="Random seed for reproducibility")
@click.pass_context
def generate(ctx, copybook, records, output, codepage, recfm, lrecl, seed):
    """Generate synthetic EBCDIC test data from a copybook."""
    from ztract.engine.bridge import ZtractBridge
    from ztract.generate.generator import generate_records
    from ztract.observability.progress import ProgressTracker

    resolved_cp = resolve_codepage(codepage)
    quiet = ctx.obj.get("quiet", False)

    jar_path = Path(__file__).parent.parent / "engine" / "ztract-engine.jar"
    bridge = ZtractBridge(jar_path=jar_path)

    try:
        bridge.check_jre()
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)

    schema = bridge.get_schema(Path(copybook), recfm=recfm, lrecl=lrecl)

    progress = ProgressTracker(quiet=quiet)
    task_id = progress.add_step("generate", total=records)

    def tracked_records():
        for rec in generate_records(schema, records, resolved_cp, seed):
            progress.update(task_id, advance=1)
            yield rec

    count = bridge.encode(
        Path(copybook), Path(output), recfm, lrecl, resolved_cp,
        tracked_records(),
    )

    progress.finish()
    click.echo(f"\nDone. {count:,} records written to {output}")
```

Register in `ztract/cli/root.py`:
```python
from ztract.cli.generate import generate

cli.add_command(generate)
```

- [ ] **Step 4: Commit**

```bash
git add ztract/writers/ebcdic.py ztract/generate/generator.py \
       ztract/cli/generate.py ztract/cli/root.py
git commit -m "feat: add mock data generator with EBCDIC writer"
```

---

## Phase 11: CLI Extras

### Task 11.1: Inspect command

**Files:**
- Create: `ztract/cli/inspect.py`
- Modify: `ztract/cli/root.py`

- [ ] **Step 1: Implement inspect command**

`ztract/cli/inspect.py`:
```python
"""ztract inspect — display copybook layout as a formatted table."""

import sys
from pathlib import Path

import click
from rich.console import Console
from rich.table import Table


@click.command()
@click.option("--copybook", required=True, type=click.Path(exists=True))
@click.pass_context
def inspect(ctx, copybook):
    """Display copybook field layout as a formatted table."""
    from ztract.engine.bridge import ZtractBridge

    jar_path = Path(__file__).parent.parent / "engine" / "ztract-engine.jar"
    bridge = ZtractBridge(jar_path=jar_path)

    try:
        bridge.check_jre()
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)

    schema = bridge.get_schema(Path(copybook))
    console = Console()

    table = Table(title=f"Copybook: {Path(copybook).name}")
    table.add_column("Field", style="bold cyan")
    table.add_column("Level")
    table.add_column("PIC")
    table.add_column("Offset", justify="right")
    table.add_column("Size", justify="right")
    table.add_column("Usage")

    for field in schema.get("fields", []):
        usage = field.get("usage", "DISPLAY")
        size_str = str(field.get("size", ""))
        if usage != "DISPLAY":
            size_str += f" ({usage})"

        table.add_row(
            field["name"],
            field.get("level", ""),
            field.get("pic", ""),
            str(field.get("offset", "")),
            size_str,
            usage,
        )

    console.print(table)
    console.print(
        f"\nTotal record length: {schema.get('record_length', 'unknown')} bytes"
    )
```

Register in `ztract/cli/root.py`:
```python
from ztract.cli.inspect import inspect

cli.add_command(inspect)
```

- [ ] **Step 2: Commit**

```bash
git add ztract/cli/inspect.py ztract/cli/root.py
git commit -m "feat: add ztract inspect command for copybook visualization"
```

### Task 11.2: Validate command

**Files:**
- Create: `ztract/cli/validate.py`
- Modify: `ztract/cli/root.py`

- [ ] **Step 1: Implement validate command**

`ztract/cli/validate.py`:
```python
"""ztract validate — pre-flight check by decoding sample records."""

import sys
from pathlib import Path

import click
from rich.console import Console


@click.command()
@click.option("--copybook", required=True, type=click.Path(exists=True))
@click.option("--input", "input_path", required=True, type=click.Path(exists=True))
@click.option("--recfm", required=True)
@click.option("--lrecl", type=int, default=None)
@click.option("--codepage", default="cp037")
@click.option("--sample", type=int, default=1000,
              help="Number of records to sample")
@click.pass_context
def validate(ctx, copybook, input_path, recfm, lrecl, codepage, sample):
    """Pre-flight check: decode sample records and report stats."""
    from ztract.codepages import resolve_codepage
    from ztract.engine.bridge import ZtractBridge

    resolved_cp = resolve_codepage(codepage)
    jar_path = Path(__file__).parent.parent / "engine" / "ztract-engine.jar"
    bridge = ZtractBridge(jar_path=jar_path)

    try:
        bridge.check_jre()
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)

    report = bridge.validate(
        Path(copybook), Path(input_path),
        recfm, lrecl, resolved_cp, sample,
    )

    console = Console()
    console.print(f"\nValidation complete ({sample} sample records)")
    console.print(f"  [green]✓ Decoded:[/green]   {report.records_decoded}")
    console.print(f"  [yellow]⚠ Warnings:[/yellow]  {report.records_warnings}")
    console.print(f"  [red]✗ Errors:[/red]    {report.records_errors}")

    if report.field_stats:
        console.print("\nField statistics:")
        for name, stats in report.field_stats.items():
            console.print(f"  {name}: {stats}")
```

Register in `ztract/cli/root.py`:
```python
from ztract.cli.validate import validate

cli.add_command(validate)
```

- [ ] **Step 2: Commit**

```bash
git add ztract/cli/validate.py ztract/cli/root.py
git commit -m "feat: add ztract validate command for pre-flight checks"
```

### Task 11.3: Init command

**Files:**
- Create: `ztract/cli/init.py`
- Modify: `ztract/cli/root.py`

- [ ] **Step 1: Implement init command**

`ztract/cli/init.py`:
```python
"""ztract init — scaffold a new Ztract project directory."""

from pathlib import Path

import click


_DIRS = [
    "copybooks",
    "jobs",
    "output",
    "logs",
    "audit",
    "rejects",
    "testdata",
    ".ztract",
]

_DEFAULT_CONFIG = """\
# Ztract machine-level configuration
# This file is gitignored — contains per-machine settings

engine:
  jvm_max_heap: 512m
  jvm_args: []

logging:
  retention_days: 30

defaults:
  codepage: cp037
  record_format: FB
"""

_GITIGNORE_ENTRIES = """\
# Ztract
.ztract/
.ztract_tmp/
logs/
rejects/
"""


@click.command()
@click.option("--dir", "target_dir", default=".",
              help="Target directory (default: current)")
def init(target_dir):
    """Scaffold a new Ztract project directory."""
    root = Path(target_dir).resolve()

    for d in _DIRS:
        (root / d).mkdir(parents=True, exist_ok=True)
        click.echo(f"  Created: {d}/")

    # Write default config
    config_path = root / ".ztract" / "config.yaml"
    if not config_path.exists():
        config_path.write_text(_DEFAULT_CONFIG)
        click.echo("  Created: .ztract/config.yaml")

    # Append to .gitignore
    gitignore = root / ".gitignore"
    existing = gitignore.read_text() if gitignore.exists() else ""
    if ".ztract_tmp/" not in existing:
        with open(gitignore, "a") as f:
            f.write("\n" + _GITIGNORE_ENTRIES)
        click.echo("  Updated: .gitignore")

    click.echo(f"\nZtract project initialized in {root}")
    click.echo("Next steps:")
    click.echo("  1. Put your .cpy files in copybooks/")
    click.echo("  2. Run: ztract inspect --copybook copybooks/YOUR.cpy")
    click.echo("  3. Run: ztract convert --copybook ... --input ... --output ...")
```

Register in `ztract/cli/root.py`:
```python
from ztract.cli.init import init

cli.add_command(init)
```

- [ ] **Step 2: Commit**

```bash
git add ztract/cli/init.py ztract/cli/root.py
git commit -m "feat: add ztract init command for project scaffolding"
```

### Task 11.4: Status command

**Files:**
- Create: `ztract/cli/status.py`
- Modify: `ztract/cli/root.py`

- [ ] **Step 1: Implement status command**

`ztract/cli/status.py`:
```python
"""ztract status — show recent job history from audit log."""

import json
from pathlib import Path

import click
from rich.console import Console
from rich.table import Table


@click.command()
@click.option("--job", "job_name", default=None, help="Filter by job name")
@click.option("--last", "count", type=int, default=10, help="Show last N runs")
def status(job_name, count):
    """Show recent job execution history from the audit log."""
    audit_file = Path("audit") / "ztract_audit.log"
    console = Console()

    if not audit_file.exists():
        console.print("[yellow]No audit log found.[/yellow] Run a job first.")
        return

    entries = []
    with open(audit_file, encoding="utf-8") as f:
        for line in f:
            stripped = line.strip()
            if stripped:
                try:
                    entry = json.loads(stripped)
                    if job_name and entry.get("job_file", "") != job_name:
                        continue
                    entries.append(entry)
                except json.JSONDecodeError:
                    continue

    entries = entries[-count:]

    if not entries:
        console.print("[yellow]No matching job runs found.[/yellow]")
        return

    table = Table(title="Recent Job Runs")
    table.add_column("Job", style="bold")
    table.add_column("Time")
    table.add_column("Records", justify="right")
    table.add_column("Rejects", justify="right")
    table.add_column("Status")

    for entry in reversed(entries):
        total_records = sum(
            s.get("records_read", 0) for s in entry.get("steps", [])
        )
        total_rejects = sum(
            s.get("records_rejected", 0) for s in entry.get("steps", [])
        )

        status_str = entry.get("overall_status", "UNKNOWN")
        if status_str == "SUCCESS":
            style = "[green]✓ OK[/green]"
        elif status_str == "PARTIAL_SUCCESS":
            style = f"[yellow]⚠ {total_rejects} rej[/yellow]"
        else:
            style = f"[red]✗ {status_str}[/red]"

        table.add_row(
            entry.get("job_file", "?"),
            entry.get("timestamp_end", "?")[:19],
            f"{total_records:,}",
            str(total_rejects),
            style,
        )

    console.print(table)
```

Register in `ztract/cli/root.py`:
```python
from ztract.cli.status import status

cli.add_command(status)
```

- [ ] **Step 2: Verify all CLI commands registered**

Run: `ztract --help`
Expected output shows all 8 commands: convert, diff, generate, init, inspect, run, status, validate

- [ ] **Step 3: Commit**

```bash
git add ztract/cli/status.py ztract/cli/root.py
git commit -m "feat: add ztract status command for audit log display"
```

---

## Phase 12: Final Integration

### Task 12.1: Run full test suite

- [ ] **Step 1: Run all tests**

Run: `pytest tests/ -v --tb=short`
Expected: All tests PASS

- [ ] **Step 2: Run linter**

Run: `ruff check ztract/ tests/`
Expected: No errors (or fix any issues)

- [ ] **Step 3: Run type checker**

Run: `mypy ztract/ --ignore-missing-imports`
Expected: No errors (or fix any issues)

- [ ] **Step 4: Verify all CLI commands**

Run each:
```bash
ztract --version
ztract --help
ztract convert --help
ztract diff --help
ztract generate --help
ztract run --help
ztract inspect --help
ztract validate --help
ztract init --help
ztract status --help
```

- [ ] **Step 5: Commit any fixes**

```bash
git add -A
git commit -m "fix: lint and type check fixes for full test suite"
```

### Task 12.2: Create NOTICE file

- [ ] **Step 1: Write NOTICE**

`NOTICE`:
```
Ztract
Copyright 2026 SRRC-1334

This product includes software developed by:

- Cobrix (https://github.com/AbsaOSS/cobrix)
  Copyright AbsaOSS, Licensed under Apache License 2.0
  Used as the COBOL copybook parsing and EBCDIC decoding engine.

- daff (https://github.com/paulfitz/daff)
  Copyright Paul Fitzpatrick, Licensed under Apache License 2.0
  Used for tabular data diff.

- multidiff (https://github.com/juhakivekas/multidiff)
  Copyright Juha Kivekäs, Licensed under MIT License
  Used for binary hex diff of REDEFINES areas.

- rich (https://github.com/Textualize/rich)
  Copyright Will McGugan, Licensed under MIT License
  Used for console output, progress bars, and tables.

- Click (https://github.com/pallets/click)
  Copyright Pallets Projects, Licensed under BSD-3-Clause
  Used as the CLI framework.

- PyArrow (https://arrow.apache.org/)
  Copyright Apache Software Foundation, Licensed under Apache License 2.0
  Used for Parquet output.

- SQLAlchemy (https://www.sqlalchemy.org/)
  Copyright SQLAlchemy authors, Licensed under MIT License
  Used for database output abstraction.

- Paramiko (https://www.paramiko.org/)
  Copyright Jeff Forcier, Licensed under LGPL 2.1
  Used for SFTP connectivity.

- Faker (https://github.com/joke2k/faker)
  Copyright Daniele Faraglia, Licensed under MIT License
  Used for mock data generation.
```

- [ ] **Step 2: Commit**

```bash
git add NOTICE
git commit -m "docs: add NOTICE file with dependency attribution"
```

---

## Deferred to Subsequent Plans

The following spec items are not covered in this plan and should be implemented in follow-up plans after the core is working end-to-end:

1. **`diff/redefines.py`** — REDEFINES area hex comparison using multidiff. Requires real EBCDIC test data with REDEFINES groups to test properly. Build after the core diff pipeline works.

2. **`engine/download_engine.py`** — JAR auto-download from GitHub Releases. Build after the first GitHub Release is published with the JAR artifact.

3. **Database writer upsert mode** — Phase 2 feature per spec.

4. **PostgreSQL array type for OCCURS** — Phase 2 feature per spec.

5. **Email notifications** — SMTP integration for `on_failure`/`on_success` in YAML jobs.

6. **Docker distribution** — Phase 2 per spec (`FROM eclipse-temurin:17-jre-alpine`).

7. **`ztract cleanup` command** — Remove orphaned `.ztract_tmp/` directories from aborted jobs.

8. **Java engine source (`engine-java/`)** — Maven project with the thin CLI wrapper around cobol-parser. This is a separate Java development task that must be built before integration testing.
