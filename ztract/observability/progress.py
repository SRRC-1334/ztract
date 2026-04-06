"""Progress tracking — thin wrapper around rich.progress with a quiet fallback."""
from __future__ import annotations

import sys
from typing import Any

# TaskID is an int alias in rich; define our own compatible type alias.
TaskID = int


class ProgressTracker:
    """Track processing progress across multiple named steps.

    When *quiet* is ``True`` **or** stdout is not a TTY, rich is not used and
    only internal counters are maintained.  The public API is identical in both
    modes so callers never need to branch.
    """

    def __init__(self, quiet: bool = False) -> None:
        self._quiet = quiet
        self._counts: dict[TaskID, int] = {}
        self._next_id: int = 0
        self._progress: Any = None  # rich Progress object or None

        use_rich = not quiet and sys.stdout.isatty()
        if use_rich:
            try:
                from rich.progress import (
                    BarColumn,
                    MofNCompleteColumn,
                    Progress,
                    SpinnerColumn,
                    TextColumn,
                    TimeElapsedColumn,
                )

                self._progress = Progress(
                    SpinnerColumn(),
                    TextColumn("[progress.description]{task.description}"),
                    BarColumn(),
                    MofNCompleteColumn(),
                    TimeElapsedColumn(),
                )
                self._progress.start()
            except Exception:
                self._progress = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def add_step(self, name: str, total: int | None = None) -> TaskID:
        """Register a new step and return its :data:`TaskID`."""
        task_id: TaskID = self._next_id
        self._next_id += 1
        self._counts[task_id] = 0

        if self._progress is not None:
            rich_id = self._progress.add_task(name, total=total)
            # Map our sequential TaskID to the rich task id.  Rich also uses
            # sequential integers so they coincide, but store the mapping
            # explicitly to be safe.
            self._rich_ids: dict[TaskID, Any]
            if not hasattr(self, "_rich_ids"):
                self._rich_ids = {}
            self._rich_ids[task_id] = rich_id

        return task_id

    def update(self, task_id: TaskID, advance: int = 1) -> None:
        """Advance *task_id* by *advance* units."""
        self._counts[task_id] = self._counts.get(task_id, 0) + advance

        if self._progress is not None and hasattr(self, "_rich_ids"):
            rich_id = self._rich_ids.get(task_id)
            if rich_id is not None:
                self._progress.advance(rich_id, advance)

    def get_count(self, task_id: TaskID) -> int:
        """Return the total units advanced for *task_id* so far."""
        return self._counts.get(task_id, 0)

    def finish(self) -> None:
        """Stop the progress display (no-op if already finished or quiet)."""
        if self._progress is not None:
            try:
                self._progress.stop()
            except Exception:
                pass
            self._progress = None
