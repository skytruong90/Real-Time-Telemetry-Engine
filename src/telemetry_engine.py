#!/usr/bin/env python3
"""Async bounded telemetry engine with validation, persistence, and metrics."""

from __future__ import annotations

import argparse
import asyncio
import json
import math
from collections import defaultdict
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class TelemetryFrame:
    source: str
    sequence: int
    timestamp_ns: int
    position_m: tuple[float, float, float]
    velocity_mps: tuple[float, float, float]
    status: str = "nominal"

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["position_m"] = list(self.position_m)
        data["velocity_mps"] = list(self.velocity_mps)
        return data


def validate_frame(raw: dict[str, Any]) -> TelemetryFrame:
    required = ("source", "sequence", "timestamp_ns", "position_m", "velocity_mps")
    missing = [name for name in required if name not in raw]
    if missing:
        raise ValueError(f"missing fields: {', '.join(missing)}")

    source = str(raw["source"])
    sequence = int(raw["sequence"])
    timestamp_ns = int(raw["timestamp_ns"])
    if not source:
        raise ValueError("source must be non-empty")
    if sequence < 0:
        raise ValueError("sequence must be non-negative")
    if timestamp_ns < 0:
        raise ValueError("timestamp_ns must be non-negative")

    def vector(name: str) -> tuple[float, float, float]:
        value = raw[name]
        if not isinstance(value, (list, tuple)) or len(value) != 3:
            raise ValueError(f"{name} must contain exactly three values")
        result = tuple(float(item) for item in value)
        if not all(math.isfinite(item) for item in result):
            raise ValueError(f"{name} must contain finite values")
        return result  # type: ignore[return-value]

    return TelemetryFrame(
        source=source,
        sequence=sequence,
        timestamp_ns=timestamp_ns,
        position_m=vector("position_m"),
        velocity_mps=vector("velocity_mps"),
        status=str(raw.get("status", "nominal")),
    )


class TelemetryEngine:
    def __init__(self, output_path: Path, queue_size: int = 256) -> None:
        if queue_size < 1:
            raise ValueError("queue_size must be >= 1")
        self.output_path = output_path
        self.queue: asyncio.Queue[TelemetryFrame | None] = asyncio.Queue(maxsize=queue_size)
        self.received = 0
        self.processed = 0
        self.dropped = 0
        self.invalid = 0
        self.sequence_gaps = 0
        self.out_of_order = 0
        self.per_source: dict[str, int] = defaultdict(int)
        self.last_sequence: dict[str, int] = {}
        self.last_timestamp_ns: dict[str, int] = {}
        self._worker: asyncio.Task[None] | None = None

    async def start(self) -> None:
        if self._worker is not None:
            raise RuntimeError("engine already started")
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        self._worker = asyncio.create_task(self._process())

    async def ingest(self, raw: dict[str, Any]) -> bool:
        self.received += 1
        try:
            frame = validate_frame(raw)
        except (ValueError, TypeError, OverflowError):
            self.invalid += 1
            return False

        if self.queue.full():
            try:
                dropped = self.queue.get_nowait()
                self.queue.task_done()
                if dropped is not None:
                    self.dropped += 1
            except asyncio.QueueEmpty:
                pass
        await self.queue.put(frame)
        return True

    async def stop(self) -> None:
        if self._worker is None:
            return
        await self.queue.put(None)
        await self._worker
        self._worker = None

    async def _process(self) -> None:
        with self.output_path.open("w", encoding="utf-8") as handle:
            while True:
                frame = await self.queue.get()
                try:
                    if frame is None:
                        return
                    previous_sequence = self.last_sequence.get(frame.source)
                    previous_timestamp = self.last_timestamp_ns.get(frame.source)
                    if previous_sequence is not None and frame.sequence > previous_sequence + 1:
                        self.sequence_gaps += frame.sequence - previous_sequence - 1
                    if previous_sequence is not None and frame.sequence <= previous_sequence:
                        self.out_of_order += 1
                    if previous_timestamp is not None and frame.timestamp_ns <= previous_timestamp:
                        self.out_of_order += 1

                    self.last_sequence[frame.source] = frame.sequence
                    self.last_timestamp_ns[frame.source] = frame.timestamp_ns
                    self.processed += 1
                    self.per_source[frame.source] += 1
                    handle.write(json.dumps(frame.to_dict(), separators=(",", ":")) + "\n")
                finally:
                    self.queue.task_done()

    def summary(self) -> dict[str, Any]:
        return {
            "received": self.received,
            "processed": self.processed,
            "dropped": self.dropped,
            "invalid": self.invalid,
            "sequence_gaps": self.sequence_gaps,
            "out_of_order": self.out_of_order,
            "per_source": dict(sorted(self.per_source.items())),
        }


async def produce_demo(engine: TelemetryEngine, source: str, frames: int, offset: int) -> None:
    base_ns = 1_700_000_000_000_000_000 + offset * 1_000_000
    for i in range(frames):
        sequence = i
        if source == "vehicle-2" and i >= frames // 2:
            sequence += 1  # one deliberate missing sequence number
        t = i * 0.02
        await engine.ingest({
            "source": source,
            "sequence": sequence,
            "timestamp_ns": base_ns + i * 20_000_000,
            "position_m": [85.0 * t, 10.0 * math.sin(t), 1000.0 + 4.0 * math.sin(0.2 * t)],
            "velocity_mps": [85.0, 10.0 * math.cos(t), 0.8 * math.cos(0.2 * t)],
            "status": "nominal",
        })
        if i % 25 == 0:
            await asyncio.sleep(0)


async def run_demo(output_dir: Path, frames: int, queue_size: int) -> dict[str, Any]:
    engine = TelemetryEngine(output_dir / "telemetry.jsonl", queue_size=queue_size)
    await engine.start()
    await asyncio.gather(
        produce_demo(engine, "vehicle-1", frames, 1),
        produce_demo(engine, "vehicle-2", frames, 2),
        produce_demo(engine, "vehicle-3", frames, 3),
    )
    await engine.stop()
    summary = engine.summary()
    (output_dir / "summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description="Run a bounded asynchronous telemetry engine")
    parser.add_argument("--demo", action="store_true", help="run the deterministic multi-source demo")
    parser.add_argument("--frames", type=int, default=300, help="frames generated per demo source")
    parser.add_argument("--queue-size", type=int, default=256)
    parser.add_argument("--output-dir", type=Path, default=Path("output"))
    args = parser.parse_args()

    if not args.demo:
        parser.error("this reference implementation currently exposes the built-in --demo producer")
    if args.frames < 1:
        parser.error("--frames must be >= 1")

    summary = asyncio.run(run_demo(args.output_dir, args.frames, args.queue_size))
    print(
        f"received={summary['received']} processed={summary['processed']} "
        f"dropped={summary['dropped']} gaps={summary['sequence_gaps']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
