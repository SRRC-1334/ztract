"""Fan-out queue for broadcasting records to multiple writers."""
from __future__ import annotations

import queue
import threading
from typing import Iterator

from ztract.writers.base import Writer, WriterStats

_POISON = object()  # Sentinel to stop worker threads


class FanOut:
    """Distribute records from a single iterator to multiple writers.

    For a single writer, records are batched and written directly (no threads).
    For multiple writers, a per-writer queue + worker thread is used so that
    all writers receive every record (broadcast).
    """

    def __init__(
        self,
        writers: list[Writer],
        schema: dict,
        batch_size: int = 1000,
        queue_size: int = 5000,
    ) -> None:
        self.writers = writers
        self.schema = schema
        self.batch_size = batch_size
        self.queue_size = queue_size

    def run(self, records: Iterator[dict]) -> int:
        """Open all writers, distribute records, close writers.

        Returns total number of records consumed from the iterator.
        """
        for writer in self.writers:
            writer.open(self.schema)

        try:
            if len(self.writers) == 1:
                total = self._run_single(records)
            else:
                total = self._run_multi(records)
        finally:
            for writer in self.writers:
                writer.close()

        return total

    # ------------------------------------------------------------------
    # Single-writer path (no threads)
    # ------------------------------------------------------------------

    def _run_single(self, records: Iterator[dict]) -> int:
        writer = self.writers[0]
        total = 0
        batch: list[dict] = []
        for record in records:
            batch.append(record)
            total += 1
            if len(batch) >= self.batch_size:
                writer.write_batch(batch)
                batch = []
        if batch:
            writer.write_batch(batch)
        return total

    # ------------------------------------------------------------------
    # Multi-writer path (per-writer queues + threads)
    # ------------------------------------------------------------------

    def _run_multi(self, records: Iterator[dict]) -> int:
        # One queue per writer
        queues: list[queue.Queue] = [
            queue.Queue(maxsize=self.queue_size) for _ in self.writers
        ]

        # Start one consumer thread per writer
        threads: list[threading.Thread] = []
        for writer, q in zip(self.writers, queues):
            t = threading.Thread(
                target=self._writer_worker,
                args=(writer, q),
                daemon=True,
            )
            t.start()
            threads.append(t)

        # Reader: batch records and broadcast to all queues
        total = 0
        batch: list[dict] = []
        for record in records:
            batch.append(record)
            total += 1
            if len(batch) >= self.batch_size:
                for q in queues:
                    q.put(list(batch))  # put a copy to each writer queue
                batch = []

        if batch:
            for q in queues:
                q.put(list(batch))

        # Send poison pills to stop workers
        for q in queues:
            q.put(_POISON)

        # Wait for all writers to finish
        for t in threads:
            t.join()

        return total

    @staticmethod
    def _writer_worker(writer: Writer, q: queue.Queue) -> None:
        """Consumer thread: drain queue and write batches until poison pill."""
        while True:
            item = q.get()
            if item is _POISON:
                break
            writer.write_batch(item)
