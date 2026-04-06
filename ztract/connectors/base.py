"""Abstract base class for all ztract data source connectors."""
from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path


class Connector(ABC):
    """Abstract interface for downloading and uploading dataset files."""

    @abstractmethod
    def download(self, source: str, local_path: str) -> Path:
        """Download *source* to *local_path* and return the resolved local Path."""

    @abstractmethod
    def upload(
        self,
        local_path: str,
        destination: str,
        site_commands: dict | None = None,
    ) -> None:
        """Upload *local_path* to *destination*."""

    def list_datasets(self, pattern: str) -> list[str]:
        """Return dataset names matching *pattern*.

        Raises NotImplementedError for connectors that do not support listing.
        """
        raise NotImplementedError(
            f"{type(self).__name__} does not support dataset listing"
        )

    @abstractmethod
    def exists(self, source: str) -> bool:
        """Return True if *source* exists at the remote (or local) location."""

    @abstractmethod
    def close(self) -> None:
        """Release any resources held by this connector."""
