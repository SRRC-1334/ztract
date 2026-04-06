"""FTP connector for mainframe dataset transfers."""
from __future__ import annotations

import ftplib
import logging
import time
from pathlib import Path
from typing import Callable

from ztract.connectors.base import Connector

logger = logging.getLogger(__name__)

# Canonical order in which SITE commands must be issued.
_SITE_CMD_ORDER = [
    "recfm",
    "lrecl",
    "blksize",
    "space_unit",
    "primary",
    "secondary",
    "mgmtclas",
    "storclas",
    "dataclas",
    "unit",
    "volser",
]


class FTPConnector(Connector):
    """Download and upload mainframe datasets over FTP.

    Parameters
    ----------
    host:
        FTP server hostname or IP address.
    user:
        FTP username.
    password:
        FTP password.
    port:
        FTP port (default 21).
    transfer_mode:
        ``"binary"`` (default) or ``"text"``.
    ftp_mode:
        ``"passive"`` (default) or ``"active"``.
    timeout:
        Socket timeout in seconds (default 30).
    retries:
        Number of reconnect attempts with exponential back-off (default 3).
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
        self._ftp: ftplib.FTP | None = self._connect()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _connect(self) -> ftplib.FTP:
        """Open an FTP connection with exponential back-off retry."""
        last_exc: Exception | None = None
        for attempt in range(self.retries):
            try:
                ftp = ftplib.FTP()
                ftp.connect(self.host, self.port, self.timeout)
                ftp.login(self.user, self.password)
                if self.ftp_mode == "passive":
                    ftp.set_pasv(True)
                else:
                    ftp.set_pasv(False)
                # Ping to verify connection is alive on reuse path
                ftp.voidcmd("NOOP")
                return ftp
            except ftplib.all_errors as exc:
                last_exc = exc
                wait = 2 ** attempt
                logger.warning(
                    "FTP connect attempt %d/%d failed: %s — retrying in %ds",
                    attempt + 1,
                    self.retries,
                    exc,
                    wait,
                )
                time.sleep(wait)
        raise ConnectionError(
            f"Failed to connect to {self.host}:{self.port} after {self.retries} attempts"
        ) from last_exc

    def _send_site_commands(self, ftp: ftplib.FTP, commands: dict) -> None:
        """Issue SITE commands in the canonical *_SITE_CMD_ORDER*.

        ``space_unit`` is special: it is sent as ``SITE CYLINDERS`` (no ``=``).
        All other keys are sent as ``SITE KEY=VALUE``.
        """
        normalised = {k.lower(): v for k, v in commands.items() if v is not None}
        for key in _SITE_CMD_ORDER:
            value = normalised.get(key)
            if value is None:
                continue
            if key == "space_unit":
                cmd = f"SITE {value.upper()}"
            else:
                cmd = f"SITE {key.upper()}={value}"
            logger.debug("Sending FTP command: %s", cmd)
            ftp.sendcmd(cmd)

    # ------------------------------------------------------------------
    # Connector interface
    # ------------------------------------------------------------------

    def download(self, source: str, local_path: str) -> Path:
        """Download *source* dataset to *local_path*.

        Uses ``RETR`` in binary mode (``retrbinary``) or text mode
        (``retrlines``) depending on :attr:`transfer_mode`.

        Parameters
        ----------
        source:
            Remote dataset name (e.g. ``HLQ.MY.DATASET``).
        local_path:
            Local path where the file will be written.

        Returns
        -------
        Path
            The resolved local path.
        """
        dest = Path(local_path)
        dest.parent.mkdir(parents=True, exist_ok=True)

        ftp = self._ftp
        if self.transfer_mode == "binary":
            with dest.open("wb") as fh:
                ftp.retrbinary(f"RETR {source}", fh.write)
        else:
            lines: list[str] = []
            ftp.retrlines(f"RETR {source}", lines.append)
            dest.write_text("\n".join(lines))

        return dest

    def upload(
        self,
        local_path: str,
        destination: str,
        site_commands: dict | None = None,
    ) -> None:
        """Upload *local_path* to *destination* dataset.

        SITE commands (if any) are issued before the data transfer in the
        order defined by :data:`_SITE_CMD_ORDER`.

        Parameters
        ----------
        local_path:
            Local file to upload.
        destination:
            Remote dataset name.
        site_commands:
            Optional mapping of SITE command keys to values (e.g.
            ``{"recfm": "FB", "lrecl": "80"}``).
        """
        ftp = self._ftp
        if site_commands:
            self._send_site_commands(ftp, site_commands)

        with open(local_path, "rb") as fh:
            ftp.storbinary(f"STOR {destination}", fh)

    def list_datasets(self, pattern: str) -> list[str]:
        """Return dataset names matching *pattern* using FTP NLST.

        Parameters
        ----------
        pattern:
            Glob-style pattern forwarded to NLST (e.g. ``HLQ.DATA.*``).
        """
        return self._ftp.nlst(pattern)

    def exists(self, source: str) -> bool:
        """Return ``True`` if *source* exists on the FTP server.

        Uses ``SIZE`` command — raises :class:`ftplib.error_perm` for missing
        datasets which is caught and converted to ``False``.
        """
        try:
            self._ftp.size(source)
            return True
        except ftplib.all_errors:
            return False

    def close(self) -> None:
        """Gracefully close the FTP connection."""
        if self._ftp is not None:
            try:
                self._ftp.quit()
            except ftplib.all_errors:
                pass
            finally:
                self._ftp = None
