import pandas as pd

from src.aggregations import compute_rejection_rate_metrics
from src.anomaly import calculate_anomaly_score
from src.features import compute_rejection_rate_baseline
from src.ingest import load_events
from src.config import DATA_PATH, TEST_DATA_PATH
from src.evaluate import evaluate
from data.event_generator import (
    ANOMALY_START,
    ANOMALY_END,
    TEST_ANOMALY_START,
    TEST_ANOMALY_END,
)

if __name__ == "__main__":
    df = load_events(DATA_PATH)

    cp_df = compute_rejection_rate_metrics(df, "CP1")
    cp_df = compute_rejection_rate_baseline(cp_df)
    cp_df = calculate_anomaly_score(cp_df)
    eval_results = evaluate(cp_df, ANOMALY_START, ANOMALY_END)
    best_eval = max(eval_results, key=lambda x: x[3])
    print(best_eval)
    threshold = best_eval[0]

    test_df = load_events(TEST_DATA_PATH)
    test_cp_df = compute_rejection_rate_metrics(test_df, "CP1")
    test_cp_df = compute_rejection_rate_baseline(test_cp_df)
    test_cp_df = calculate_anomaly_score(test_cp_df)

    test_anomaly_df = test_cp_df.copy()
    test_anomaly_df = test_anomaly_df[test_anomaly_df["anomaly_score"].notna()]
    test_anomaly_df["is_anomaly"] = (test_anomaly_df.index >= TEST_ANOMALY_START) & (
        test_anomaly_df.index <= TEST_ANOMALY_END
    )

    predicted = test_anomaly_df["anomaly_score"] >= threshold

    true_pos = (predicted & test_anomaly_df["is_anomaly"]).sum()
    false_pos = (predicted & ~test_anomaly_df["is_anomaly"]).sum()
    false_neg = ((~predicted) & test_anomaly_df["is_anomaly"]).sum()

    total_predicted_pos = true_pos + false_pos
    total_actual_pos = true_pos + false_neg
    precision = true_pos / total_predicted_pos if total_predicted_pos > 0 else 0
    recall = true_pos / total_actual_pos if total_actual_pos > 0 else 0

    print(true_pos, false_pos, false_neg, precision, recall, sep=" ")

    anomalous_events = test_anomaly_df[predicted]
    print(anomalous_events)
