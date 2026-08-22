import asyncio
import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import telemetry_engine


class ValidationTests(unittest.TestCase):
    def test_valid_frame(self):
        frame = telemetry_engine.validate_frame({
            "source": "vehicle-1",
            "sequence": 0,
            "timestamp_ns": 100,
            "position_m": [1, 2, 3],
            "velocity_mps": [4, 5, 6],
        })
        self.assertEqual(frame.position_m, (1.0, 2.0, 3.0))

    def test_bad_vector_rejected(self):
        with self.assertRaises(ValueError):
            telemetry_engine.validate_frame({
                "source": "vehicle-1",
                "sequence": 0,
                "timestamp_ns": 100,
                "position_m": [1, 2],
                "velocity_mps": [4, 5, 6],
            })


class EngineTests(unittest.IsolatedAsyncioTestCase):
    async def test_end_to_end_gap_detection(self):
        with tempfile.TemporaryDirectory() as temp:
            output = Path(temp) / "telemetry.jsonl"
            engine = telemetry_engine.TelemetryEngine(output, queue_size=16)
            await engine.start()
            await engine.ingest({"source": "v1", "sequence": 0, "timestamp_ns": 100, "position_m": [0, 0, 0], "velocity_mps": [1, 0, 0]})
            await engine.ingest({"source": "v1", "sequence": 2, "timestamp_ns": 200, "position_m": [1, 0, 0], "velocity_mps": [1, 0, 0]})
            await engine.stop()

            summary = engine.summary()
            self.assertEqual(summary["processed"], 2)
            self.assertEqual(summary["sequence_gaps"], 1)
            self.assertEqual(len(output.read_text(encoding="utf-8").splitlines()), 2)

    async def test_bounded_queue_drops_oldest(self):
        engine = telemetry_engine.TelemetryEngine(Path("unused.jsonl"), queue_size=2)
        for sequence in range(3):
            accepted = await engine.ingest({
                "source": "v1",
                "sequence": sequence,
                "timestamp_ns": 100 + sequence,
                "position_m": [0, 0, 0],
                "velocity_mps": [1, 0, 0],
            })
            self.assertTrue(accepted)
        self.assertEqual(engine.dropped, 1)
        self.assertEqual(engine.queue.qsize(), 2)

    async def test_demo_pipeline_writes_summary(self):
        with tempfile.TemporaryDirectory() as temp:
            summary = await telemetry_engine.run_demo(Path(temp), frames=20, queue_size=128)
            self.assertEqual(summary["received"], 60)
            self.assertEqual(summary["processed"], 60)
            self.assertEqual(summary["sequence_gaps"], 1)
            saved = json.loads((Path(temp) / "summary.json").read_text(encoding="utf-8"))
            self.assertEqual(saved["processed"], 60)


if __name__ == "__main__":
    unittest.main()
