"""ZtractBridge - Python wrapper around the Java COBOL engine subprocess.

Manages launching, communicating with, and shutting down the Java engine JAR.
No dependencies on other ztract modules — only stdlib + json.
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterator


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class JREError(RuntimeError):
    """Raised when the JRE is missing, not on PATH, or too old."""


class EngineError(RuntimeError):
    """Raised when the Java engine exits with a non-zero return code."""


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class ValidationReport:
    records_decoded: int = 0
    records_warnings: int = 0
    records_errors: int = 0
    field_stats: dict = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Bridge
# ---------------------------------------------------------------------------

class ZtractBridge:
    """Manages the Java subprocess for COBOL binary operations."""

    def __init__(
        self,
        jar_path: Path,
        jvm_max_heap: str = "512m",
        jvm_args: list[str] | None = None,
    ) -> None:
        self.jar_path = Path(jar_path)
        self.jvm_max_heap = jvm_max_heap
        self.jvm_args: list[str] = jvm_args or []
        self._cached_jre_version: str | None = None
        self._active_proc: subprocess.Popen | None = None  # type: ignore[type-arg]

    # ------------------------------------------------------------------
    # JRE detection
    # ------------------------------------------------------------------

    def check_jre(self) -> str:
        """Return the major Java version string (e.g. "17").

        Raises JREError if Java is not on PATH or version < 11.
        Result is cached after the first successful call.
        """
        if self._cached_jre_version is not None:
            return self._cached_jre_version

        try:
            result = subprocess.run(
                ["java", "-version"],
                capture_output=True,
                text=True,
            )
        except FileNotFoundError:
            raise JREError(
                "Java not found on PATH. "
                "Install Java 11 or later from https://adoptium.net"
            )

        # 'java -version' writes to stderr
        output = result.stderr or result.stdout

        # New format: "17.0.2"  Old format: "1.8.0_301"
        match = re.search(r'"(\d+)(?:\.(\d+))?[\d._]*"', output)
        if not match:
            raise JREError(
                f"Could not parse Java version from: {output!r}. "
                "Install Java 11 or later from https://adoptium.net"
            )

        first, second = match.group(1), match.group(2)
        if first == "1":
            # Old 1.x format — major version is the second component
            major = int(second) if second else 0
        else:
            major = int(first)

        if major < 11:
            raise JREError(
                f"Java {major} detected. Java 11 or later is required. "
                "Download from https://adoptium.net"
            )

        version_str = str(major)
        self._cached_jre_version = version_str
        return version_str

    # ------------------------------------------------------------------
    # Command building
    # ------------------------------------------------------------------

    def _base_cmd(self) -> list[str]:
        """Build the base Java command list.

        Includes -Dstdout.encoding=UTF-8 only when JRE >= 17.
        """
        cmd = ["java", f"-Xmx{self.jvm_max_heap}", "-Dfile.encoding=UTF-8"]

        try:
            major = int(self._cached_jre_version or "0")
        except ValueError:
            major = 0

        if major >= 17:
            cmd.append("-Dstdout.encoding=UTF-8")

        cmd.extend(self.jvm_args)
        cmd.extend(["-jar", str(self.jar_path)])
        return cmd

    # ------------------------------------------------------------------
    # Stderr classification
    # ------------------------------------------------------------------

    def _classify_stderr(self, line: str) -> str:
        """Classify a single JVM stderr line.

        Returns one of: "fatal", "warning", "ignore".
        """
        if not line:
            return "ignore"
        if (
            "Exception in thread" in line
            or "OutOfMemoryError" in line
            or line.startswith("ERROR:")
        ):
            return "fatal"
        if line.startswith("WARN:"):
            return "warning"
        return "ignore"

    # ------------------------------------------------------------------
    # Schema
    # ------------------------------------------------------------------

    def get_schema(
        self,
        copybook: Path,
        recfm: str | None = None,
        lrecl: int | None = None,
    ) -> dict:
        """Return the parsed schema JSON dict for a copybook.

        Raises EngineError on non-zero exit code.
        """
        cmd = self._base_cmd() + ["--schema-only", "--copybook", str(copybook)]
        if recfm is not None:
            cmd += ["--recfm", recfm]
        if lrecl is not None:
            cmd += ["--lrecl", str(lrecl)]

        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            raise EngineError(
                f"Engine exited {result.returncode}: {result.stderr.strip()}"
            )
        return json.loads(result.stdout)

    # ------------------------------------------------------------------
    # Decode (EBCDIC → JSON Lines stream)
    # ------------------------------------------------------------------

    def decode(
        self,
        copybook: Path,
        input_path: Path,
        recfm: str,
        lrecl: int,
        codepage: str,
        encoding: str = "ebcdic",
    ) -> Iterator[dict]:
        """Stream records decoded from a binary COBOL file.

        Yields one dict per record, reading stdout line-by-line.
        """
        cmd = self._base_cmd() + [
            "--mode", "decode",
            "--copybook", str(copybook),
            "--input", str(input_path),
            "--recfm", recfm,
            "--lrecl", str(lrecl),
            "--codepage", codepage,
            "--encoding", encoding,
        ]

        with subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        ) as proc:
            self._active_proc = proc
            try:
                for raw in proc.stdout:  # type: ignore[union-attr]
                    line = raw.decode("utf-8", errors="replace").rstrip("\n")
                    if line:
                        yield json.loads(line)
            finally:
                proc.wait()
                self._active_proc = None

    # ------------------------------------------------------------------
    # Encode (JSON Lines → binary COBOL)
    # ------------------------------------------------------------------

    def encode(
        self,
        copybook: Path,
        output_path: Path,
        recfm: str,
        lrecl: int,
        codepage: str,
        records: Iterator[dict],
        encoding: str = "ebcdic",
    ) -> int:
        """Write records to a binary COBOL file via the engine.

        Pipes JSON Lines to stdin. Returns the number of records written.
        """
        cmd = self._base_cmd() + [
            "--mode", "encode",
            "--copybook", str(copybook),
            "--output", str(output_path),
            "--recfm", recfm,
            "--lrecl", str(lrecl),
            "--codepage", codepage,
            "--encoding", encoding,
        ]

        count = 0
        with subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        ) as proc:
            self._active_proc = proc
            try:
                for record in records:
                    line = json.dumps(record) + "\n"
                    proc.stdin.write(line.encode("utf-8"))  # type: ignore[union-attr]
                    count += 1
                proc.stdin.close()  # type: ignore[union-attr]
                proc.wait()
            finally:
                self._active_proc = None

        return count

    # ------------------------------------------------------------------
    # Validate
    # ------------------------------------------------------------------

    def validate(
        self,
        copybook: Path,
        input_path: Path,
        recfm: str,
        lrecl: int,
        codepage: str,
        sample: int = 1000,
    ) -> ValidationReport:
        """Run the engine in validate mode and return a ValidationReport.

        Raises EngineError on non-zero exit code.
        """
        cmd = self._base_cmd() + [
            "--mode", "validate",
            "--copybook", str(copybook),
            "--input", str(input_path),
            "--recfm", recfm,
            "--lrecl", str(lrecl),
            "--codepage", codepage,
            "--sample", str(sample),
        ]

        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            raise EngineError(
                f"Engine exited {result.returncode}: {result.stderr.strip()}"
            )

        data = json.loads(result.stdout)
        return ValidationReport(
            records_decoded=data.get("records_decoded", 0),
            records_warnings=data.get("records_warnings", 0),
            records_errors=data.get("records_errors", 0),
            field_stats=data.get("field_stats", {}),
        )

    # ------------------------------------------------------------------
    # Shutdown
    # ------------------------------------------------------------------

    def shutdown(self) -> None:
        """Terminate any active subprocess.

        Sends SIGTERM (terminate() on Windows), waits 5 s, then SIGKILL.
        """
        proc = self._active_proc
        if proc is None:
            return

        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            if sys.platform != "win32":
                import signal
                proc.send_signal(signal.SIGKILL)
            else:
                proc.kill()
            proc.wait()
