
import re
import pandas as pd


def clean_column_name(name: str) -> str:
    name = str(name).strip()
    name = name.lower()
    name = re.sub(r"[ \-]+", "_", name)
    name = re.sub(r"[^0-9a-zA-Z_]", "", name)
    return name


def normalize_headers(df: pd.DataFrame) -> pd.DataFrame:
    mapping = {col: clean_column_name(col) for col in df.columns}
    df = df.rename(columns=mapping)
    return df


def apply_rename(df: pd.DataFrame, rename_cfg: dict) -> pd.DataFrame:
    if not rename_cfg:
        return df

    # Normalize both keys and values of rename config
    normalized = {clean_column_name(k): clean_column_name(v) for k, v in rename_cfg.items()}
    df = df.rename(columns=normalized)
    return df


def apply_office_flags(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
    input_cols = rule.get("input_columns", [])
    labels = rule.get("labels", [])
    output_col = rule.get("output_column", "ufficio")

    if not input_cols or not labels or len(input_cols) != len(labels):
        print("Invalid office_flags rule, skipping.")
        return df

    existing = [c for c in input_cols if c in df.columns]
    if not existing:
        print("No office flag columns found, skipping office_flags rule.")
        return df

    def row_to_office(row):
        for col, label in zip(input_cols, labels):
            if col in row.index and row[col] == 1:
                return label
        return None

    df[output_col] = df.apply(row_to_office, axis=1)
    return df
