"""Zowe CLI connector for mainframe dataset operations.

Supports two backends:
- **zosmf** (default): uses ``zos-files`` commands, requires z/OSMF.
- **zftp**: uses ``zos-ftp`` commands via the ``@zowe/zos-ftp-for-zowe-cli``
  plugin. Does not require z/OSMF — only FTP access to z/OS.

Transfer modes:
- ``binary``: raw EBCDIC bytes (default for Ztract).
- ``text``: server-side EBCDIC-to-ASCII conversion.
- ``encoding``: conversion to a specific codepage.
- ``record``: VB files with RDW headers preserved (zftp only).
"""
from __future__ import annotations

import logging
import re
import subprocess
from pathlib import Path

from ztract.connectors.base import Connector

logger = logging.getLogger(__name__)


class ZoweError(RuntimeError):
    """Raised when the Zowe CLI is unavailable, too old, or returns an error."""


class ZoweConnector(Connector):
    """Run mainframe operations via the Zowe CLI (v2+).

    Parameters
    ----------
    profile:
        Zowe profile name passed via ``--zosmf-profile`` or ``--zftp-profile``.
    backend:
        ``"zosmf"`` (default) or ``"zftp"``.
    transfer_mode:
        ``"binary"`` (default), ``"text"``, ``"encoding"``, or ``"record"``.
    encoding:
        Codepage name used when *transfer_mode* is ``"encoding"``
        (e.g. ``"cp277"``).
    """

    def __init__(
        self,
        profile: str,
        backend: str = "zosmf",
        transfer_mode: str = "binary",
        encoding: str | None = None,
    ) -> None:
        self.profile = profile
        self.backend = backend
        self.transfer_mode = transfer_mode
        self.encoding = encoding
        self._zowe_version: str | None = None
        self.check_zowe()

    # ------------------------------------------------------------------
    # Zowe version + backend guard
    # ------------------------------------------------------------------

    def check_zowe(self) -> dict:
        """Verify Zowe CLI is installed (v2+) and backend is available.

        Returns
        -------
        dict
            ``{"zowe_version": "3", "backend": "zosmf"}`` (or ``"zftp"``).

        Raises
        ------
        ZoweError
            If the CLI is missing, too old, or the zftp plugin is not installed.
        """
        try:
            result = subprocess.run(
                ["zowe", "--version"],
                capture_output=True,
                text=True,
            )
        except FileNotFoundError as exc:
            raise ZoweError(
                "Zowe CLI not found — install it with 'npm install -g @zowe/cli'"
            ) from exc

        output = result.stdout.strip()
        match = re.search(r"(\d+)\.\d+\.\d+", output)
        if not match:
            raise ZoweError(f"Could not parse Zowe version from: {output!r}")

        major = int(match.group(1))
        if major < 2:
            raise ZoweError(
                f"Zowe CLI v2 or later is required; found v{major}.x. "
                "Upgrade with 'npm install -g @zowe/cli'."
            )

        self._zowe_version = str(major)
        info: dict = {"zowe_version": str(major), "backend": self.backend}

        if self.backend == "zftp":
            self._check_zftp_plugin()
            info["zftp_plugin"] = "installed"

        return info

    def _check_zftp_plugin(self) -> None:
        """Verify the zos-ftp plugin is installed."""
        try:
            result = subprocess.run(
                ["zowe", "plugins", "list"],
                capture_output=True,
                text=True,
            )
        except FileNotFoundError as exc:
            raise ZoweError("Zowe CLI not found on PATH") from exc

        if "zos-ftp" not in result.stdout:
            raise ZoweError(
                "zftp backend requires the zos-ftp plugin. "
                "Install with: zowe plugins install @zowe/zos-ftp-for-zowe-cli@latest"
            )

    # ------------------------------------------------------------------
    # Command builders
    # ------------------------------------------------------------------

    def _cmd_group(self) -> str:
        """Return the Zowe command group for the active backend."""
        return "zos-ftp" if self.backend == "zftp" else "zos-files"

    def _profile_flag(self) -> list[str]:
        """Return the profile CLI flag pair for the active backend."""
        if not self.profile:
            return []
        if self.backend == "zftp":
            return ["--zftp-profile", self.profile]
        return ["--zosmf-profile", self.profile]

    def _transfer_args(self, direction: str = "download") -> list[str]:
        """Return CLI flags for the configured transfer mode.

        Parameters
        ----------
        direction:
            ``"download"`` or ``"upload"``.

        Raises
        ------
        ValueError
            If *transfer_mode* is ``"record"`` but backend is not ``"zftp"``.
        """
        if self.transfer_mode == "binary":
            return ["--binary"]
        if self.transfer_mode == "text":
            return []  # default mode, no extra flag
        if self.transfer_mode == "encoding":
            if not self.encoding:
                raise ValueError(
                    "transfer_mode='encoding' requires an encoding value "
                    "(e.g. encoding='cp277')"
                )
            return ["--encoding", self.encoding]
        if self.transfer_mode == "record":
            if self.backend != "zftp":
                raise ValueError(
                    "transfer_mode='record' requires backend='zftp'. "
                    "z/OSMF does not support RDW mode."
                )
            return ["--rdw"]
        return []

    def _run(
        self, args: list[str], check_returncode: bool = True
    ) -> subprocess.CompletedProcess:
        """Run a ``zowe`` command and optionally raise on failure."""
        cmd = ["zowe"] + args + self._profile_flag()

        logger.debug("Running: %s", " ".join(cmd))
        result = subprocess.run(cmd, capture_output=True, text=True)

        if check_returncode and result.returncode != 0:
            raise ZoweError(
                f"Zowe command failed (rc={result.returncode}): "
                f"{result.stderr.strip()}"
            )
        return result

    # ------------------------------------------------------------------
    # Connector interface
    # ------------------------------------------------------------------

    def download(self, source: str, local_path: str) -> Path:
        """Download *source* dataset via Zowe CLI.

        Uses the configured backend (``zos-files`` or ``zos-ftp``) and
        transfer mode (``--binary``, ``--encoding``, ``--rdw``, or none).
        """
        dest = Path(local_path)
        dest.parent.mkdir(parents=True, exist_ok=True)

        args = [
            self._cmd_group(), "download", "data-set",
            source,
            "--file", str(dest),
        ] + self._transfer_args("download")

        self._run(args)
        return dest

    def upload(
        self,
        local_path: str,
        destination: str,
        site_commands: dict | None = None,
        dcb: str | None = None,
    ) -> None:
        """Upload *local_path* to *destination* dataset via Zowe CLI.

        Parameters
        ----------
        local_path:
            Local file to upload.
        destination:
            Target mainframe dataset name.
        site_commands:
            Optional allocation attributes (currently unused by Zowe CLI).
        dcb:
            DCB allocation string for zftp backend
            (e.g. ``"RECFM=FB LRECL=500 BLKSIZE=27920"``).
        """
        args = [
            self._cmd_group(), "upload", "file-to-data-set",
            local_path, destination,
        ] + self._transfer_args("upload")

        if self.backend == "zftp" and dcb:
            args += ["--dcb", dcb]

        self._run(args)

    def exists(self, source: str) -> bool:
        """Return ``True`` if *source* dataset exists on the mainframe."""
        result = self._run(
            [self._cmd_group(), "list", "data-set", source],
            check_returncode=False,
        )
        return result.returncode == 0

    def list_datasets(self, pattern: str) -> list[str]:
        """Return dataset names matching *pattern*."""
        result = self._run([self._cmd_group(), "list", "data-set", pattern])
        output = result.stdout.strip()
        if not output:
            return []
        return [line.strip() for line in output.splitlines() if line.strip()]

    def close(self) -> None:
        """No-op — Zowe CLI holds no persistent connection."""
