"""StepContext — shared state bag for a pipeline run."""
from __future__ import annotations

import time
from pathlib import Path
from typing import Any

__all__ = ["StepContext"]

_REF_PREFIX = "$ref:"


class StepContext:
    """Carries shared state across all steps in a single pipeline run.

    Responsibilities:

    * **Output registry** — steps expose named outputs; later steps resolve
      ``$ref:name.type`` references to concrete :class:`~pathlib.Path` objects.
    * **Connection pool** — connectors are created on demand and cached so
      that multiple steps sharing the same URI reuse a single connection.
    * **Timing** — per-step wall-clock elapsed times.
    * **Reject accounting** — aggregate reject counts across all steps.
    * **Temp-file registry** — paths registered here are deleted on
      :meth:`close`.
    """

    def __init__(self) -> None:
        # name → {"type": output_type, "path": Path}
        self._outputs: dict[str, dict[str, Any]] = {}
        # uri → Connector
        self._connections: dict[str, Any] = {}
        # step name → start time (monotonic)
        self._start_times: dict[str, float] = {}
        # step name → elapsed seconds
        self._elapsed: dict[str, float] = {}
        # step name → reject count
        self._rejects: dict[str, int] = {}
        # temp paths to clean up on close()
        self._temp_paths: list[Path] = []

    # ------------------------------------------------------------------
    # Output registry
    # ------------------------------------------------------------------

    def expose(self, name: str, output_type: str, path: Path) -> None:
        """Register a named output produced by a step.

        Parameters
        ----------
        name:
            Logical output name (e.g. ``"customers"``).
        output_type:
            Format tag (e.g. ``"csv"``, ``"parquet"``, ``"dataset"``).
        path:
            Filesystem path to the output.
        """
        self._outputs[name] = {"type": output_type, "path": Path(path)}

    def resolve_ref(self, ref: str) -> Path:
        """Resolve a ``$ref:name.type`` reference to a :class:`~pathlib.Path`.

        Parameters
        ----------
        ref:
            Reference string in the form ``"$ref:name.type"`` where *name* is
            the logical output name and *type* is the expected format tag.  The
            type portion is used only for lookup — it is not re-validated here.

        Returns
        -------
        Path
            The path previously registered via :meth:`expose`.

        Raises
        ------
        KeyError
            If *ref* does not match any registered output.
        ValueError
            If *ref* is not a valid ``$ref:…`` string.
        """
        if not ref.startswith(_REF_PREFIX):
            raise ValueError(
                f"Invalid reference {ref!r}: must start with {_REF_PREFIX!r}."
            )
        key = ref[len(_REF_PREFIX):]
        # key is "name.type" — split on last dot to get name
        if "." in key:
            name = key.rsplit(".", 1)[0]
        else:
            name = key
        if name not in self._outputs:
            raise KeyError(
                f"Unknown output reference {ref!r}. "
                f"Available outputs: {list(self._outputs.keys())}"
            )
        return self._outputs[name]["path"]

    # ------------------------------------------------------------------
    # Connection pool
    # ------------------------------------------------------------------

    def get_connector(self, uri: str, factory: Any) -> Any:
        """Return a cached connector for *uri*, creating it via *factory* if needed.

        Parameters
        ----------
        uri:
            Connection URI (used as cache key).
        factory:
            Callable ``factory(uri) -> Connector`` invoked on cache miss.

        Returns
        -------
        Connector
            A live connector instance.
        """
        if uri not in self._connections:
            self._connections[uri] = factory(uri)
        return self._connections[uri]

    # ------------------------------------------------------------------
    # Timing
    # ------------------------------------------------------------------

    def start_step(self, name: str) -> None:
        """Record the start time for step *name*."""
        self._start_times[name] = time.monotonic()

    def end_step(self, name: str) -> None:
        """Record the end time for step *name* and compute elapsed seconds."""
        start = self._start_times.get(name)
        if start is None:
            raise KeyError(f"start_step({name!r}) was never called.")
        self._elapsed[name] = time.monotonic() - start

    def get_elapsed(self, name: str) -> float:
        """Return elapsed seconds for a completed step.

        Raises
        ------
        KeyError
            If the step was never ended (or never started).
        """
        if name not in self._elapsed:
            raise KeyError(f"No elapsed time recorded for step {name!r}.")
        return self._elapsed[name]

    # ------------------------------------------------------------------
    # Reject accounting
    # ------------------------------------------------------------------

    def add_rejects(self, name: str, count: int) -> None:
        """Add *count* rejects for step *name*."""
        self._rejects[name] = self._rejects.get(name, 0) + count

    @property
    def total_rejects(self) -> int:
        """Total number of rejected records across all steps."""
        return sum(self._rejects.values())

    # ------------------------------------------------------------------
    # Temp file registry
    # ------------------------------------------------------------------

    def register_temp(self, path: Path) -> None:
        """Register *path* for deletion when :meth:`close` is called."""
        self._temp_paths.append(Path(path))

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def close(self) -> None:
        """Release connectors and delete registered temp files."""
        for connector in self._connections.values():
            try:
                connector.close()
            except Exception:
                pass
        self._connections.clear()

        for tmp in self._temp_paths:
            try:
                if tmp.exists():
                    tmp.unlink()
            except Exception:
                pass
        self._temp_paths.clear()
