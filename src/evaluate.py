import pandas as pd
import datetime as dt
import numpy as np

from src.config import EVALUATION_THRESHOLD_STEPS


def evaluate(
    df: pd.DataFrame, anomaly_start: dt.datetime, anomaly_end: dt.datetime
) -> list[tuple[float, float, float, float]]:
    eval_df = df.copy()
    eval_df = eval_df[eval_df["anomaly_score"].notna()]

    eval_df["is_anomaly"] = (eval_df.index >= anomaly_start) & (
        eval_df.index <= anomaly_end
    )

    eval_results = []
    for threshold in np.linspace(
        eval_df["anomaly_score"].min(),
        eval_df["anomaly_score"].max(),
        EVALUATION_THRESHOLD_STEPS,
    ):
        predicted = eval_df["anomaly_score"] >= threshold

        true_pos = sum(predicted & eval_df["is_anomaly"])
        false_pos = sum(predicted & ~eval_df["is_anomaly"])
        false_neg = sum((~predicted) & eval_df["is_anomaly"])

        total_predicted_pos = true_pos + false_pos
        total_actual_pos = true_pos + false_neg
        precision = true_pos / total_predicted_pos if total_predicted_pos > 0 else 0
        recall = true_pos / total_actual_pos if total_actual_pos > 0 else 0
        f1 = (
            2 * ((precision * recall) / (precision + recall))
            if (precision + recall) > 0
            else 0
        )

        eval_results.append((threshold, precision, recall, f1))

    return eval_results
