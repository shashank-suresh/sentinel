import pandas as pd

from src.aggregations import compute_rejection_rate_metrics
from src.anomaly import calculate_anomaly_score
from src.features import compute_rejection_rate_baseline

if __name__ == "__main__":
    df = pd.read_csv("data/events.csv")

    cp_df = compute_rejection_rate_metrics(df, "CP1")
    cp_df = compute_rejection_rate_baseline(cp_df)
    cp_df = calculate_anomaly_score(cp_df)
