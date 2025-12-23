import pandas as pd


def compute_rejection_rate_metrics(
    df: pd.DataFrame, counterparty_id: str
) -> pd.DataFrame:
    df = df.copy()
    df["timestamp_created"] = pd.to_datetime(df["timestamp_created"], format="mixed")
    df = df.set_index("timestamp_created").sort_index()
    df = df.loc[df["counterparty_id"] == counterparty_id, ["status"]]

    rejected_trades = df["status"].eq("REJECTED")
    df["rejected_trade_count"] = rejected_trades.rolling("30min", min_periods=1).sum()
    df["total_trade_count"] = df["status"].rolling("30min", min_periods=1).count()
    df["rejection_rate_30m"] = df["rejected_trade_count"] / df["total_trade_count"]

    return df.reset_index()


def add_warmup_indicator(df: pd.DataFrame, warm_threshold: int = 50) -> pd.DataFrame:
    df = df.copy()
    df["is_window_warm"] = df["total_trade_count"] > warm_threshold

    return df
