# TODO: For warm rows, calculate the anomaly_score = | rejection_rate_delta_baseline(T) - median({rejection_rate_delta_baseline(t) for t < T}))
import numpy as np
import pandas as pd


def calculate_anomaly_score(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["timestamp_created"] = pd.to_datetime(df["timestamp_created"], format="mixed")
    df = df.set_index("timestamp_created").sort_index()

    df["baseline_median"] = np.where(
        df["is_baseline_warm"] & df["is_window_warm"],
        df["rejection_rate_delta_baseline"].expanding().median().shift(1),
        np.nan,
    )

    df["anomaly_score"] = abs(
        df["rejection_rate_delta_baseline"] - df["baseline_median"]
    )

    print(
        df.loc[:, ["anomaly_score", "rejection_rate_delta_baseline", "baseline_median"]]
    )

    return df.reset_index()
