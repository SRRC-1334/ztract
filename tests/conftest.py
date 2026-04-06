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
