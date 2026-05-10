import pandas as pd

from src.config import SHORT_WINDOW, REJECTED_STATUS, WINDOW_WARM_THRESHOLD


def compute_rejection_rate_metrics(
    df: pd.DataFrame, counterparty_id: str, warm_threshold: int = WINDOW_WARM_THRESHOLD
) -> pd.DataFrame:
    df = df.copy()
    df = df.loc[df["counterparty_id"] == counterparty_id, ["status"]]

    rejected_trades = df["status"].eq(REJECTED_STATUS)
    df["rejected_trade_count"] = rejected_trades.rolling(
        SHORT_WINDOW, min_periods=1
    ).sum()
    df["total_trade_count"] = df["status"].rolling(SHORT_WINDOW, min_periods=1).count()
    df["rejection_rate_30m"] = df["rejected_trade_count"] / df["total_trade_count"]

    df["is_window_warm"] = df["total_trade_count"] > warm_threshold

    return df
