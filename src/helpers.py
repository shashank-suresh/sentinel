from collections.abc import Sequence

import numpy as np
import pandas as pd


def standardize_features(
    df_in: pd.DataFrame, features: str | Sequence[str]
) -> tuple[pd.DataFrame, list[str]]:
    if isinstance(features, str):
        features = [features]

    df_out = df_in.copy()
    cols_out = []
    for feature in features:
        output_col = f"{feature}_std"
        cols_out.append(output_col)

        series = pd.Series(pd.to_numeric(df_out[feature], errors="coerce"))
        mu = float(series.mean())
        sigma = float(series.std(ddof=0))

        if sigma == 0 or np.isnan(sigma):
            df_out[output_col] = np.nan
        else:
            df_out[output_col] = (series - mu) / sigma

    return (df_out, cols_out)
