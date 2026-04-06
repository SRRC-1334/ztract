"""SFTP connector for mainframe dataset transfers via SSH."""
from __future__ import annotations

import logging
import warnings
from pathlib import Path

import paramiko  # type: ignore[import-untyped]

from ztract.connectors.base import Connector

logger = logging.getLogger(__name__)


class SFTPConnector(Connector):
    """Download and upload files over SFTP using paramiko.

    Parameters
    ----------
    host:
        SFTP server hostname or IP address.
    user:
        SSH username.
    password:
        SSH password (used when *key_path* is not supplied).
    key_path:
        Path to a PEM private key file for key-based authentication.
    port:
        SSH port (default 22).
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
        self._sftp = self._connect()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _connect(self) -> paramiko.SFTPClient:
        """Open an SFTP session, authenticating with key or password."""
        transport = paramiko.Transport((self.host, self.port))

        if self.key_path is not None:
            pkey = paramiko.RSAKey.from_private_key_file(self.key_path)
            transport.connect(username=self.user, pkey=pkey)
        else:
            transport.connect(username=self.user, password=self.password)

        self._transport = transport
        sftp = paramiko.SFTPClient.from_transport(transport)
        return sftp

    # ------------------------------------------------------------------
    # Connector interface
    # ------------------------------------------------------------------

    def download(self, source: str, local_path: str) -> Path:
        """Download *source* to *local_path* via SFTP GET.

        Parameters
        ----------
        source:
            Remote path of the file to download.
        local_path:
            Local destination path.

        Returns
        -------
        Path
            The resolved local path.
        """
        dest = Path(local_path)
        dest.parent.mkdir(parents=True, exist_ok=True)
        assert self._sftp is not None
        self._sftp.get(source, str(dest))
        return dest

    def upload(
        self,
        local_path: str,
        destination: str,
        site_commands: dict | None = None,
    ) -> None:
        """Upload *local_path* to *destination* via SFTP PUT.

        SFTP does not support mainframe SITE commands; passing *site_commands*
        will emit a :class:`UserWarning`.

        Parameters
        ----------
        local_path:
            Local file to upload.
        destination:
            Remote destination path.
        site_commands:
            Unsupported for SFTP — triggers a warning if provided.
        """
        if site_commands:
            warnings.warn(
                "SFTPConnector does not support site_commands; "
                "the site_commands argument will be ignored.",
                UserWarning,
                stacklevel=2,
            )
        assert self._sftp is not None
        self._sftp.put(local_path, destination)

    def exists(self, source: str) -> bool:
        """Return ``True`` if *source* exists on the SFTP server.

        Uses ``sftp.stat()`` — any I/O error is treated as "not found".
        """
        try:
            assert self._sftp is not None
            self._sftp.stat(source)
            return True
        except (FileNotFoundError, IOError, OSError):
            return False

    def close(self) -> None:
        """Close both the SFTP client and the underlying Transport."""
        if self._sftp is not None:
            try:
                self._sftp.close()
            except Exception:
                pass
            finally:
                self._sftp = None

        if self._transport is not None:
            try:
                self._transport.close()
            except Exception:
                pass
            finally:
                self._transport = None
