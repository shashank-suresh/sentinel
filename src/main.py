import pandas as pd

from aggregations import add_warmup_indicator, compute_rejection_rate_metrics

if __name__ == "__main__":
    df = pd.read_csv("data/events.csv")

    cp_df = compute_rejection_rate_metrics(df, "CP1")
    cp_df = add_warmup_indicator(cp_df)

    print(cp_df["rejection_rate_30m"])
