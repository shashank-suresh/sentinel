import pandas as pd


def load_events(df: pd.DataFrame) -> None:
    df["timestamp_created"] = pd.to_datetime(df["timestamp_created"], format="mixed")
    df.set_index("timestamp_created", inplace=True)
    df.sort_index(inplace=True)
