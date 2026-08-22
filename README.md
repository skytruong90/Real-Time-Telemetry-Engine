# Real-Time Telemetry Engine

[![CI](https://github.com/skytruong90/Real-Time-Telemetry-Engine/actions/workflows/ci.yml/badge.svg)](https://github.com/skytruong90/Real-Time-Telemetry-Engine/actions/workflows/ci.yml)

An asynchronous telemetry-ingestion and processing engine for simulation and flight-test style data streams. The project models multiple producers, bounded-queue backpressure, frame validation, sequence-gap detection, real-time aggregation, JSONL persistence, and summary metrics using Python's standard `asyncio` stack.

## What it demonstrates

- asynchronous producer/consumer architecture
- bounded queues and explicit backpressure policy
- telemetry schema validation
- per-source sequence tracking and gap detection
- streaming aggregation without loading the full run into memory
- deterministic multi-source demo generation
- JSON Lines persistence for downstream analytics
- run summaries and data-quality metrics
- unit/integration tests and GitHub Actions CI

## Architecture

```text
 producer A ----\
 producer B -----+--> ingest() --> bounded asyncio.Queue --> processor --> telemetry.jsonl
 producer C ----/                         |                    |
                                          |                    +--> rolling metrics
                                     drop-oldest                    +--> gap counters
                                     on overload                     +--> summary.json
```

## Quick start

Python 3.10+; no third-party dependencies.

```bash
python src/telemetry_engine.py --demo --frames 300 --output-dir output
```

Generated artifacts:

```text
output/
├── telemetry.jsonl
└── summary.json
```

The demo launches three concurrent simulated telemetry sources and intentionally introduces one sequence gap so the data-quality path is exercised.

## Frame schema

Each persisted frame contains:

```json
{
  "source": "vehicle-1",
  "sequence": 42,
  "timestamp_ns": 1700000000000000000,
  "position_m": [120.5, -4.2, 1010.0],
  "velocity_mps": [84.8, 1.1, -0.2],
  "status": "nominal"
}
```

## Data-quality behavior

The engine validates:

- required fields and types
- three-component position/velocity vectors
- non-negative sequence numbers
- monotonically increasing timestamps per source
- sequence continuity per source

Sequence gaps are recorded in the summary rather than treated as fatal. Invalid frames are rejected and counted. If producers outrun the configured queue, the oldest queued frame is dropped and the drop counter is incremented; this keeps ingestion bounded instead of allowing unbounded memory growth.

## Testing

```bash
python -m unittest discover -s tests -v
```

Tests cover schema validation, sequence-gap accounting, overload behavior, and the complete asynchronous record-to-summary pipeline.

## Repository layout

```text
Real-Time-Telemetry-Engine/
├── src/telemetry_engine.py
├── tests/test_telemetry_engine.py
├── .github/workflows/ci.yml
└── README.md
```

## Design notes

The persisted format is JSONL because each frame is independently readable and stream-friendly. A production implementation could replace the writer with Kafka, NATS, ZeroMQ, DDS, gRPC streaming, Apache Arrow, or a binary wire protocol while preserving the same validation and backpressure concepts.

## Next extensions

- UDP/TCP/gRPC adapters
- schema versioning and protobuf serialization
- publish/subscribe fan-out
- Prometheus/OpenTelemetry metrics
- replay with original timing
- Apache Arrow/Parquet archival sink
- clock synchronization and timestamp-quality flags
- fault injection for latency, reordering, duplication, and loss
