import numpy as np
import pandas as pd


def compute_rejection_rate_baseline(
    df: pd.DataFrame, baseline_threshold: int = 50
) -> pd.DataFrame:
    df = df.copy()
    df["timestamp_created"] = pd.to_datetime(df["timestamp_created"], format="mixed")
    df = df.set_index("timestamp_created").sort_index()

    rejected_trades = df["status"].eq("REJECTED")
    rejected_2h = rejected_trades.rolling("2h", min_periods=1).sum()
    rejected_2h_sq = (rejected_trades**2).rolling("2h", min_periods=1).sum()
    total_2h = df["status"].rolling("2h", min_periods=1).count()

    rejected_30m_sq = (rejected_trades**2).rolling("30min", min_periods=1).sum()

    rejected_baseline = rejected_2h - df["rejected_trade_count"]
    rejected_baseline_sq = rejected_2h_sq - rejected_30m_sq
    total_baseline = total_2h - df["total_trade_count"]

    df["baseline_count"] = total_baseline

    df["is_baseline_warm"] = df["baseline_count"] >= baseline_threshold

    df["baseline_mean"] = np.where(
        df["is_baseline_warm"], rejected_baseline / total_baseline, np.nan
    )

    mean_of_squares = rejected_baseline_sq / total_baseline
    square_of_mean = (rejected_baseline / total_baseline) ** 2

    variance = mean_of_squares - square_of_mean
    df["baseline_std"] = np.where(
        df["is_baseline_warm"], np.sqrt(np.maximum(variance, 0)), np.nan
    )

    df["rejection_rate_delta_baseline"] = (
        df["rejection_rate_30m"] - df["baseline_mean"]
    ) / df["baseline_std"]

    return df.reset_index()
