import pandas as pd
from pathlib import Path


def load_events(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)

    df["timestamp_created"] = pd.to_datetime(df["timestamp_created"], format="mixed")
    df.set_index("timestamp_created", inplace=True)
    df.sort_index(inplace=True)

    return df
