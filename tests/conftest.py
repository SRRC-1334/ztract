"""Shared pytest fixtures for Ztract tests."""
from pathlib import Path

import pytest


@pytest.fixture
def test_data_dir() -> Path:
    """Return the path to the test_data directory."""
    return Path(__file__).parent / "test_data"


@pytest.fixture
def sample_copybook(test_data_dir: Path) -> Path:
    """Return the path to the sample CUSTMAST.cpy copybook."""
    return test_data_dir / "CUSTMAST.cpy"


@pytest.fixture
def complex_redefines_cpy(test_data_dir: Path) -> Path:
    return test_data_dir / "COMPLEX_REDEFINES.cpy"


@pytest.fixture
def complex_occurs_cpy(test_data_dir: Path) -> Path:
    return test_data_dir / "COMPLEX_OCCURS.cpy"


@pytest.fixture
def complex_numeric_cpy(test_data_dir: Path) -> Path:
    return test_data_dir / "COMPLEX_NUMERIC.cpy"


@pytest.fixture
def complex_redefines_dat(test_data_dir: Path) -> Path:
    return test_data_dir / "REDEFINES_SAMPLE.DAT"


@pytest.fixture
def complex_occurs_dat(test_data_dir: Path) -> Path:
    return test_data_dir / "OCCURS_SAMPLE.DAT"


@pytest.fixture
def complex_numeric_dat(test_data_dir: Path) -> Path:
    return test_data_dir / "NUMERIC_SAMPLE.DAT"
