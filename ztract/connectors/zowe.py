"""Zowe CLI connector for mainframe dataset operations."""
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
        Zowe profile name to pass to every command via ``--zosmf-profile``.
    """

    def __init__(self, profile: str) -> None:
        self.profile = profile
        self.check_zowe()

    # ------------------------------------------------------------------
    # Zowe version guard
    # ------------------------------------------------------------------

    def check_zowe(self) -> str:
        """Verify Zowe CLI is installed and is at least v2.

        Returns
        -------
        str
            The major version string (e.g. ``"3"``).

        Raises
        ------
        ZoweError
            If the ``zowe`` executable is not found, returns a non-zero exit
            code, or is older than v2.
        """
        try:
            result = subprocess.run(
                ["zowe", "--version"],
                capture_output=True,
                text=True,
            )
        except FileNotFoundError as exc:
            raise ZoweError("Zowe CLI not found — install it with 'npm install -g @zowe/cli'") from exc
        except subprocess.CalledProcessError as exc:
            raise ZoweError(f"Zowe CLI error: {exc}") from exc

        output = result.stdout.strip()
        # Output format: "@zowe-cli/core/3.0.0 linux-x64 node-v18.0.0"
        # or just "3.0.0" depending on version
        match = re.search(r"(\d+)\.\d+\.\d+", output)
        if not match:
            raise ZoweError(f"Could not parse Zowe version from: {output!r}")

        major = int(match.group(1))
        if major < 2:
            raise ZoweError(
                f"Zowe CLI v2 or later is required; found v{major}.x. "
                "Upgrade with 'npm install -g @zowe/cli'."
            )

        return str(major)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _run(self, args: list[str], check_returncode: bool = True) -> subprocess.CompletedProcess:
        """Run a ``zowe`` command and optionally raise on failure."""
        cmd = ["zowe"] + args
        if self.profile:
            cmd += ["--zosmf-profile", self.profile]

        logger.debug("Running: %s", " ".join(cmd))
        result = subprocess.run(cmd, capture_output=True, text=True)

        if check_returncode and result.returncode != 0:
            raise ZoweError(
                f"Zowe command failed (rc={result.returncode}): {result.stderr.strip()}"
            )
        return result

    # ------------------------------------------------------------------
    # Connector interface
    # ------------------------------------------------------------------

    def download(self, source: str, local_path: str) -> Path:
        """Download *source* dataset using ``zowe zos-files download data-set``.

        Always passes ``--binary`` to preserve mainframe byte content.

        Parameters
        ----------
        source:
            Mainframe dataset name (e.g. ``HLQ.MY.DATASET``).
        local_path:
            Local path to write the downloaded file.

        Returns
        -------
        Path
            The resolved local path.
        """
        dest = Path(local_path)
        dest.parent.mkdir(parents=True, exist_ok=True)

        self._run([
            "zos-files", "download", "data-set",
            source,
            "--file", str(dest),
            "--binary",
        ])
        return dest

    def upload(
        self,
        local_path: str,
        destination: str,
        site_commands: dict | None = None,
    ) -> None:
        """Upload *local_path* using ``zowe zos-files upload file-to-data-set``.

        Parameters
        ----------
        local_path:
            Local file to upload.
        destination:
            Target mainframe dataset name.
        site_commands:
            Optional dict of allocation attributes forwarded as CLI flags
            (e.g. ``{"recfm": "FB"}`` → ``--record-format FB``).
            Currently stored for future extension; not all attributes map to
            Zowe CLI flags for every version.
        """
        args = ["zos-files", "upload", "file-to-data-set", local_path, destination]
        self._run(args)

    def exists(self, source: str) -> bool:
        """Return ``True`` if *source* dataset exists on the mainframe."""
        result = self._run(
            ["zos-files", "list", "data-set", source],
            check_returncode=False,
        )
        return result.returncode == 0

    def list_datasets(self, pattern: str) -> list[str]:
        """Return dataset names matching *pattern*.

        Parameters
        ----------
        pattern:
            Dataset name pattern (e.g. ``HLQ.DATA.*``).
        """
        result = self._run(["zos-files", "list", "data-set", pattern])
        output = result.stdout.strip()
        if not output:
            return []
        return [line.strip() for line in output.splitlines() if line.strip()]

    def close(self) -> None:
        """No-op — Zowe CLI holds no persistent connection."""
