
import pandas as pd
import numpy as np


def detect_percentile_outliers(series: pd.Series, lower: int = 5, upper: int = 95):
    s = pd.to_numeric(series, errors="coerce")
    if s.dropna().empty:
        return pd.Series(False, index=series.index), np.nan, np.nan

    p_low = s.quantile(lower / 100)
    p_high = s.quantile(upper / 100)
    mask = (s < p_low) | (s > p_high)

    return mask, p_low, p_high


def detect_anomalies(df: pd.DataFrame, anomalies_cfg: dict) -> pd.DataFrame:
    if not anomalies_cfg:
        return pd.DataFrame(columns=["row_index", "column", "value", "reason"])

    records = []

    for rule in anomalies_cfg.get("numeric", []):
        col = rule.get("column")
        if not col or col not in df.columns:
            continue

        method = rule.get("method", "percentile")

        if method == "percentile":
            lower = rule.get("lower_percentile", 5)
            upper = rule.get("upper_percentile", 95)
            mask, p_low, p_high = detect_percentile_outliers(df[col], lower, upper)
            reason = f"percentile_outlier[{p_low:.2f},{p_high:.2f}]"
        else:
            # Fallback: no anomalies for unknown methods
            continue

        for idx in df.index[mask]:
            records.append({
                "row_index": int(idx),
                "column": col,
                "value": df.at[idx, col],
                "reason": reason,
            })

    return pd.DataFrame.from_records(records)
