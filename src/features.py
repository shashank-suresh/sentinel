import numpy as np
import pandas as pd

from src.config import (
    SHORT_WINDOW,
    LONG_WINDOW,
    REJECTED_STATUS,
    BASELINE_WARM_THRESHOLD,
)


def compute_rejection_rate_baseline(
    df: pd.DataFrame, baseline_threshold: int = BASELINE_WARM_THRESHOLD
) -> pd.DataFrame:
    df = df.copy()

    rejected_trades = df["status"].eq(REJECTED_STATUS)
    rejected_2h = rejected_trades.rolling(LONG_WINDOW, min_periods=1).sum()
    rejected_2h_sq = (rejected_trades**2).rolling(LONG_WINDOW, min_periods=1).sum()
    total_2h = df["status"].rolling(LONG_WINDOW, min_periods=1).count()

    rejected_30m_sq = (rejected_trades**2).rolling(SHORT_WINDOW, min_periods=1).sum()

    rejected_baseline = rejected_2h - df["rejected_trade_count"]
    rejected_baseline_sq = rejected_2h_sq - rejected_30m_sq
    total_baseline = total_2h - df["total_trade_count"]

    df["baseline_count"] = total_baseline

    # Ensure no NaNs: baseline_count may be 0 early; keep warm flag boolean with no NaNs.
    df["is_baseline_warm"] = (df["baseline_count"] >= baseline_threshold).fillna(False)

    # Avoid division-by-zero / inf, and ensure no NaNs in baseline-derived outputs.
    safe_total_baseline = total_baseline.replace(0, np.nan)

    baseline_mean_raw = rejected_baseline / safe_total_baseline
    df["baseline_mean"] = np.where(df["is_baseline_warm"], baseline_mean_raw, 0.0)
    df["baseline_mean"] = np.nan_to_num(
        df["baseline_mean"], nan=0.0, posinf=0.0, neginf=0.0
    )

    mean_of_squares = rejected_baseline_sq / safe_total_baseline
    square_of_mean = (rejected_baseline / safe_total_baseline) ** 2

    variance = mean_of_squares - square_of_mean
    baseline_std_raw = np.sqrt(np.maximum(variance, 0))
    df["baseline_std"] = np.where(df["is_baseline_warm"], baseline_std_raw, 0.0)
    df["baseline_std"] = np.nan_to_num(
        df["baseline_std"], nan=0.0, posinf=0.0, neginf=0.0
    )

    # If std is 0 (not warm / no variance / insufficient history), define delta as 0.
    denom = df["baseline_std"].replace(0, np.nan)
    df["rejection_rate_delta_baseline"] = (
        df["rejection_rate_30m"] - df["baseline_mean"]
    ) / denom
    df["rejection_rate_delta_baseline"] = np.nan_to_num(
        df["rejection_rate_delta_baseline"], nan=0.0, posinf=0.0, neginf=0.0
    )

    return df
