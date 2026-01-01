import pandas as pd

from aggregations import add_warmup_indicator, compute_rejection_rate_metrics
from features import compute_rejection_rate_baseline

if __name__ == "__main__":
    df = pd.read_csv("data/events.csv")

    cp_df = compute_rejection_rate_metrics(df, "CP1")
    cp_df = add_warmup_indicator(cp_df)

    cp_df = compute_rejection_rate_baseline(cp_df)
