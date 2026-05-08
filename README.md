# Sentinel

Sentinel is a trade event anomaly detection pipeline that identifies unusual rejection rate spikes for a given counterparty by comparing recent activity against a rolling historical baseline.

## How it works

```
data/events.csv
    → aggregations.compute_rejection_rate_metrics()   # 30-min rolling rejection rate per counterparty
    → features.compute_rejection_rate_baseline()       # 2-hr baseline mean/std, z-score delta
    → anomaly.calculate_anomaly_score()                # median L2 distance from historical feature vectors
```

The pipeline operates on a stream of trade events. For each event, it computes a rejection rate over a 30-minute rolling window and compares it against a 2-hour historical baseline (excluding the current window). An anomaly score is then calculated as the L2 distance of the current feature vector from the median of all prior warm-state vectors.

Scores are only produced once both the current window and the baseline window have accumulated enough trades ("warm state"), preventing spurious signals during startup.

## Running

This project uses [uv](https://docs.astral.sh/uv/) for package management (Python 3.14).

```bash
# Install dependencies
uv sync

# Run the pipeline
uv run python -m src.main

# Regenerate synthetic event data
uv run python data/event_generator.py
```

## Project structure

| File | Role |
|---|---|
| `src/main.py` | Entry point; wires the pipeline stages |
| `src/aggregations.py` | 30-min rolling rejection rate and warm-state gating |
| `src/features.py` | 2-hr baseline stats and z-score delta |
| `src/anomaly.py` | Feature standardisation and median-distance anomaly score |
| `src/helpers.py` | Z-score standardisation utility |
| `data/event_generator.py` | Synthetic ASX trade event generator |

## Data

`data/events.csv` contains synthetic ASX trade events for a single counterparty. A short anomaly window is injected with an elevated rejection rate to serve as a test fixture for the detection pipeline.
