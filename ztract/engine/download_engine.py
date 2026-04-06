"""ztract.engine.download_engine — JAR auto-download.

Downloads the ztract-engine JAR from GitHub releases if it is not already
present alongside this module.
"""
from __future__ import annotations

import logging
import urllib.request
from pathlib import Path

from ztract import __version__

logger = logging.getLogger(__name__)

# GitHub release URL pattern
_RELEASE_URL = (
    "https://github.com/SRRC-1334/ztract/releases/download/"
    "v{version}/ztract-engine.jar"
)


def get_jar_path() -> Path:
    """Get the path where the engine JAR should be located."""
    return Path(__file__).parent / "ztract-engine.jar"


def ensure_jar(version: str | None = None) -> Path:
    """Ensure the engine JAR exists, downloading if necessary.

    Args:
        version: Version to download. Defaults to current ztract version.

    Returns:
        Path to the JAR file.

    Raises:
        RuntimeError: If download fails.
    """
    jar_path = get_jar_path()
    if jar_path.exists():
        return jar_path

    version = version or __version__
    url = _RELEASE_URL.format(version=version)

    logger.info("Engine JAR not found. Downloading from %s...", url)

    try:
        urllib.request.urlretrieve(url, str(jar_path))
        logger.info(
            "Downloaded engine JAR to %s (%d bytes)",
            jar_path,
            jar_path.stat().st_size,
        )
    except Exception as e:
        raise RuntimeError(
            f"Failed to download engine JAR from {url}: {e}\n"
            f"You can manually download it from the GitHub releases page:\n"
            f"https://github.com/SRRC-1334/ztract/releases"
        ) from e

    return jar_path
