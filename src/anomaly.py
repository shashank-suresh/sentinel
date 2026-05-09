import numpy as np
import pandas as pd

from src.helpers import standardize_features


def calculate_anomaly_score(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df, vec_cols = standardize_features(
        df, ["rejection_rate_delta_baseline", "rejection_rate_30m"]
    )

    df["is_vector_warm"] = df["is_baseline_warm"] & df["is_window_warm"]

    warm_indices = np.where(df["is_vector_warm"])[0]
    vec_data = df[vec_cols].values

    anomaly_scores = np.full(len(df), np.nan)

    for i, pos in enumerate(warm_indices):
        prev_warm_indices = warm_indices[:i]

        if len(prev_warm_indices) == 0:
            continue

        current_vec = vec_data[pos]
        reference_vecs = vec_data[prev_warm_indices]

        distances = np.linalg.norm(reference_vecs - current_vec, axis=1)

        anomaly_scores[pos] = np.median(distances)

    df["anomaly_score"] = anomaly_scores

    return df
