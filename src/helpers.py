from collections.abc import Sequence

import numpy as np
import pandas as pd


def standardize_features(
    df_in: pd.DataFrame,
    features: str | Sequence[str],
    *,
    output_cols: str | Sequence[str] | None = None,
    mean: float | None = None,
    std: float | None = None,
) -> tuple[pd.DataFrame, list[str]]:
    if isinstance(features, str):
        features = [features]

    if output_cols is not None and len(output_cols) != len(features):
        raise ValueError(
            "output_cols must match the number of features and should be passed in the same order"
        )

    df_out = df_in.copy()
    cols_out = []
    for i, feature in enumerate(features):
        output_col = output_cols[i] if output_cols else f"{feature}_std"
        cols_out.append(output_col)

        series = pd.Series(pd.to_numeric(df_out[feature], errors="coerce"))
        mu = float(series.mean()) if mean is None else float(mean)
        sigma = float(series.std(ddof=0)) if std is None else float(std)

        if sigma == 0 or np.isnan(sigma):
            df_out[output_col] = np.nan
        else:
            df_out[output_col] = (series - mu) / sigma

    return (df_out, cols_out)
