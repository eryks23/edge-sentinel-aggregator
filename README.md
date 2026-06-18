# edge-sentinel-aggregator

> Lightweight edge-node data aggregator with threshold-based alert batching and SHA-256 integrity checksumming for IoT pipelines.

## Description

`edge_aggregator.py` simulates an IoT edge node that continuously polls sensor data, filters readings above a configurable critical threshold, and transmits them to the cloud in batched, checksum-verified payloads. It implements two flush strategies — size-based and timeout-based — to guarantee delivery even under low-traffic conditions. The script is designed for rapid prototyping, educational use, and as a reference implementation for edge-to-cloud data pipelines.

## Table of Contents

- [Key Features](#key-features)
- [Tech Stack](#tech-stack)
- [Requirements](#requirements)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage / Quick Start](#usage--quick-start)
- [API Documentation](#api-documentation)
- [Project Structure](#project-structure)
- [Testing](#testing)
- [Roadmap](#roadmap)
- [Contributing](#contributing)
- [License](#license)
- [Author](#author)

---

## Key Features

- **Threshold-based filtering** — only readings exceeding `critical_threshold` (default 40 °C) are batched, reducing unnecessary cloud traffic
- **Dual-trigger flushing** — batches flush automatically when they reach `batch_size` *or* when `timeout` seconds elapse since the last flush, preventing data staleness under low-alert conditions
- **SHA-256 integrity checksums** — every outbound payload envelope includes a checksum computed over the serialised batch, enabling end-to-end integrity verification
- **Zero external dependencies** — runs on Python standard library only; no `pip install` required
- **Per-node configuration** — `node_id`, `batch_size`, `critical_threshold`, and `timeout` are all tunable at instantiation

---

## Tech Stack

| Component | Detail |
|---|---|
| Language | Python 3.7+ |
| Dependencies | Standard library only (`hashlib`, `json`, `collections`, `typing`, `time`, `random`) |
| Target environment | Edge nodes, local servers, CI pipelines |

---

## Requirements

- Python **3.7** or higher (uses `typing` generics and f-strings)
- No third-party packages

Verify your Python version:

```bash
python3 --version
```

---

## Installation

```bash
# Clone the repository
git clone https://github.com/eryks23/edge-sentinel-aggregator.git
cd edge-sentinel-aggregator

# No dependencies to install — stdlib only
```

---

## Configuration

All configuration is passed to the `EdgeDataAggregator` constructor or set as instance attributes. There are no environment variables or `.env` files.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `node_id` | `str` | — | Unique identifier for this edge node (**required**) |
| `batch_size` | `int` | `5` | Alert entries per batch before automatic flush |
| `critical_threshold` | `float` | `40.0` | Temperature in °C above which a reading triggers an alert |
| `timeout` | `int` | `10` | Seconds after the last flush before a non-empty batch is force-flushed |

To override `critical_threshold` or `timeout`, set them on the instance after construction:

```python
aggregator = EdgeDataAggregator(node_id="EDGE-NODE-002", batch_size=5)
aggregator.critical_threshold = 38.5  # lower the alert threshold
aggregator.timeout = 30               # allow a longer idle window
```

---

## Usage / Quick Start

**Run the built-in demo loop:**

```bash
python3 edge_aggregator.py
```

Example output:

```
Aggregator Started: EDGE-SECURE-001 (Batch Size: 3)
DEBUG: 22.481°C - Normal
DEBUG: 35.902°C - Normal
Alert added to batch. Current size: 1
Alert added to batch. Current size: 2
Alert added to batch. Current size: 3
[SYSTEM] Preparing batch for transmission...

[CLOUD TRANSMISSION]
Node ID: EDGE-SECURE-001
Checksum: 3f8a1c9d...
Payload Size: 92 bytes
Items in batch: 3
------------------------------------------
```

Stop the aggregator with `Ctrl+C`.

**Use as a module in your own code:**

```python
from edge_aggregator import EdgeDataAggregator

aggregator = EdgeDataAggregator(node_id="EDGE-PROD-007", batch_size=10)
aggregator.critical_threshold = 45.0

# Run a single processing cycle
aggregator.process_cycle()

# Manually force a flush at any time
aggregator.flush_batch()
```

---

## API Documentation

### `EdgeDataAggregator`

```python
class EdgeDataAggregator:
    def __init__(self, node_id: str, batch_size: int = 5) -> None
```

Creates a new aggregator instance for a single edge node.

| Parameter | Type | Required | Default | Description |
|---|---|---|---|---|
| `node_id` | `str` | Yes | — | Unique identifier for this node |
| `batch_size` | `int` | No | `5` | Entries per batch before auto-flush |

---

#### `process_cycle() -> None`

Executes one sensor read-evaluate-flush iteration. Intended to be called in a loop.

- Reads a simulated temperature via `get_reading()`
- Appends `{"t": <float>, "ts": <timestamp>}` to `pending_batch` if the reading exceeds `critical_threshold`
- Calls `flush_batch()` if `len(pending_batch) >= batch_size`
- Calls `flush_batch()` if `pending_batch` is non-empty and `timeout` seconds have elapsed since the last flush

---

#### `flush_batch() -> None`

Serialises `pending_batch` to JSON, computes a SHA-256 checksum, assembles the transmission envelope, calls `send_to_cloud()`, and resets the batch and flush timer.

Has no effect if `pending_batch` is empty.

---

#### `send_to_cloud(envelope: Dict[str, Any]) -> None`

Handles outbound transmission of the assembled envelope. In the current implementation, prints transmission metadata to stdout.

> **Note:** Replace this method with real HTTP (`requests`) or MQTT (`paho-mqtt`) logic for production deployments.

| Parameter | Type | Description |
|---|---|---|
| `envelope` | `Dict[str, Any]` | Dict containing `node_id` (str), `data` (list of alert entries), and `checksum` (str) |

---

#### `generate_checksum(data: str) -> str`

Returns the SHA-256 hex digest of the given string.

| Parameter | Type | Description |
|---|---|---|
| `data` | `str` | Serialised payload string to hash |
| **Returns** | `str` | 64-character hexadecimal SHA-256 digest |

```python
agg = EdgeDataAggregator(node_id="X")
digest = agg.generate_checksum('{"t": 45.0, "ts": 1718700000.0}')
# 'a3f1...' (64-char hex string)
```

---

#### `get_reading() -> float`

Returns a simulated sensor reading in the range **[20.0, 50.0] °C**, rounded to 3 decimal places.

> **Note:** Replace with real hardware sensor I/O (e.g., `Adafruit_DHT`, `smbus2`) for production use.

---

## Project Structure

```
edge-sentinel-aggregator/
├── edge_aggregator.py   # Core EdgeDataAggregator class and CLI entry point
├── requirements.txt     # Dependency manifest (stdlib only — no packages)
├── LICENSE              # MIT License
└── README.md            # This file
```

---

## Testing

The project currently has no test suite. Below is a minimal `unittest` setup to get started:

```bash
touch test_edge_aggregator.py
```

```python
# test_edge_aggregator.py
import unittest
from edge_aggregator import EdgeDataAggregator


class TestEdgeDataAggregator(unittest.TestCase):

    def setUp(self):
        self.agg = EdgeDataAggregator(node_id="TEST-001", batch_size=3)

    def test_checksum_is_deterministic(self):
        payload = '{"t": 45.0, "ts": 1000000}'
        self.assertEqual(
            self.agg.generate_checksum(payload),
            self.agg.generate_checksum(payload),
        )

    def test_checksum_length(self):
        digest = self.agg.generate_checksum("test")
        self.assertEqual(len(digest), 64)

    def test_reading_within_range(self):
        for _ in range(200):
            r = self.agg.get_reading()
            self.assertGreaterEqual(r, 20.0)
            self.assertLessEqual(r, 50.0)

    def test_flush_clears_batch(self):
        self.agg.pending_batch = [{"t": 45.0, "ts": 1000000.0}]
        self.agg.flush_batch()
        self.assertEqual(len(self.agg.pending_batch), 0)

    def test_flush_noop_on_empty_batch(self):
        # Should not raise and batch should remain empty
        self.agg.flush_batch()
        self.assertEqual(len(self.agg.pending_batch), 0)

    def test_below_threshold_does_not_add_to_batch(self):
        self.agg.critical_threshold = 100.0  # nothing will exceed this
        initial_len = len(self.agg.pending_batch)
        self.agg.process_cycle()
        self.assertEqual(len(self.agg.pending_batch), initial_len)


if __name__ == "__main__":
    unittest.main()
```

Run the tests:

```bash
python3 -m unittest test_edge_aggregator.py -v
```

---

## Roadmap

- [ ] Replace `send_to_cloud()` stub with real HTTP (`requests`) or MQTT (`paho-mqtt`) transport
- [ ] Add persistent queue (SQLite or file-backed) for offline resilience
- [ ] Expose `node_id`, `batch_size`, `critical_threshold`, and `timeout` as CLI arguments via `argparse`
- [ ] Support multiple sensor channels per node instance
- [ ] Replace `print` statements with structured logging via the `logging` module
- [ ] Implement retry logic with exponential back-off on transmission failure
- [ ] Add a complete unit-test suite with CI integration (GitHub Actions)

---

## Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/your-feature-name`
3. Commit with descriptive messages: `git commit -m "feat: add retry logic to send_to_cloud"`
4. Push to your fork: `git push origin feature/your-feature-name`
5. Open a Pull Request against `main`

Please follow [PEP 8](https://peps.python.org/pep-0008/) style conventions. Add docstrings to any new public method and cover new logic with unit tests before submitting.

---

## License

Distributed under the MIT License. See [`LICENSE`](LICENSE) for the full text.

---

## Author

GitHub: [@eryks23](https://github.com/eryks23)  
Repository: [https://github.com/eryks23/edge-sentinel-aggregator](https://github.com/eryks23/edge-sentinel-aggregator)
