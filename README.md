# Sentinel

Sentinel is a trade event anomaly detection pipeline that identifies unusual rejection rate spikes for a given counterparty by comparing recent activity against a rolling historical baseline.

## How it works

The pipeline operates on a stream of trade events. For each event, it computes a rejection rate over a 30-minute rolling window and compares it against a 2-hour historical baseline (excluding the current window). An anomaly score is then calculated as the L2 distance of the current feature vector from the median of all prior warm-state vectors.

Scores are only produced once both the current window and the baseline window have accumulated enough trades ("warm state"), preventing spurious signals during startup.
