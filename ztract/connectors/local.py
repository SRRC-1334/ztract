"""Local filesystem connector — passes paths through without network I/O."""
from __future__ import annotations

import shutil
from pathlib import Path

from ztract.connectors.base import Connector


class LocalConnector(Connector):
    """Connector for files that already reside on the local filesystem.

    ``download`` validates the file and returns its Path unchanged.
    ``upload`` copies the file to the destination, creating parent directories
    as needed.
    """

    # ------------------------------------------------------------------
    # Download
    # ------------------------------------------------------------------

    def download(self, source: str, local_path: str) -> Path:
        """Return *source* as a :class:`~pathlib.Path` after validation.

        Parameters
        ----------
        source:
            Path to the local file to use as the dataset input.
        local_path:
            Ignored for ``LocalConnector`` — the source is used directly.

        Returns
        -------
        Path
            ``Path(source)`` resolved as-is.

        Raises
        ------
        FileNotFoundError
            If *source* does not exist.
        ValueError
            If *source* is a zero-byte (empty) file.
        """
        path = Path(source)
        if not path.exists():
            raise FileNotFoundError(f"Dataset not found: {source!r}")
        if path.stat().st_size == 0:
            raise ValueError("empty")
        return path

    # ------------------------------------------------------------------
    # Upload
    # ------------------------------------------------------------------

    def upload(
        self,
        local_path: str,
        destination: str,
        site_commands: dict | None = None,
    ) -> None:
        """Copy *local_path* to *destination*, creating parent directories.

        Parameters
        ----------
        local_path:
            Source file to copy.
        destination:
            Destination file path.
        site_commands:
            Ignored for ``LocalConnector``.
        """
        dest = Path(destination)
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(local_path, dest)

    # ------------------------------------------------------------------
    # Exists
    # ------------------------------------------------------------------

    def exists(self, source: str) -> bool:
        """Return ``True`` if *source* exists on the local filesystem."""
        return Path(source).exists()

    # ------------------------------------------------------------------
    # Close
    # ------------------------------------------------------------------

    def close(self) -> None:
        """No-op — the local connector holds no external resources."""
