
import pandas as pd


def apply_schema(df: pd.DataFrame, schema: list) -> pd.DataFrame:
    if not schema:
        return df

    out = pd.DataFrame(index=df.index)
    for col in schema:
        if col in df.columns:
            out[col] = df[col]
        else:
            out[col] = pd.NA

    return out
