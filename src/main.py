import pandas as pd
from pathlib import Path

from src.aggregations import compute_rejection_rate_metrics
from src.anomaly import calculate_anomaly_score
from src.features import compute_rejection_rate_baseline
from src.ingest import load_events

DATA_PATH = Path("data/events.csv")

if __name__ == "__main__":
    df = load_events(DATA_PATH)

    print("Is the dataframe sorted by timestamp_created? ", end="")
    print(df.index.is_monotonic_increasing)

    cp_df = compute_rejection_rate_metrics(df, "CP1")
    print("What is the average rejection rate during the anomaly window? ", end="")
    print(cp_df.loc["2025-12-20 12:15":"2025-12-20 12:25"]["rejection_rate_30m"].mean())

    start_ts = cp_df[cp_df["is_window_warm"]].index[0]

    print("What is the average rejection rate outside the anomaly window? ", end="")
    print(
        cp_df.loc[start_ts : start_ts + pd.Timedelta(minutes=10)][
            "rejection_rate_30m"
        ].mean()
    )

    cp_df = compute_rejection_rate_baseline(cp_df)
    print(
        "What is the maximum rejection rate spike we encountered during the anomaly window? ",
        end="",
    )
    print(
        cp_df.loc["2025-12-20 12:15":"2025-12-20 12:25"][
            "rejection_rate_delta_baseline"
        ].max()
    )

    start_ts = cp_df[cp_df["is_baseline_warm"]].index[0]

    print(
        "What is the maximum rejection rate spike we encountered outside the anomaly window? ",
        end="",
    )
    print(
        cp_df.loc[start_ts : start_ts + pd.Timedelta(minutes=10)][
            "rejection_rate_delta_baseline"
        ].max()
    )

    cp_df = calculate_anomaly_score(cp_df)
    print(
        "What is the maximum anomaly score spike we encountered during the anomaly window? ",
        end="",
    )
    print(cp_df.loc["2025-12-20 12:15":"2025-12-20 12:25"]["anomaly_score"].max())

    start_ts = cp_df[cp_df["is_vector_warm"]].index[0]
    print(
        "What is the maximum anomaly score spike we encountered outside the anomaly window? ",
        end="",
    )
    print(
        cp_df.loc[start_ts : start_ts + pd.Timedelta(minutes=10)]["anomaly_score"].max()
    )
