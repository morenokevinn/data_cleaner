
import pandas as pd


def drop_exact_duplicates(df: pd.DataFrame):
    dup_mask = df.duplicated(keep="first")
    df_dups = df[dup_mask].copy()
    df_unique = df[~dup_mask].copy()
    return df_unique, df_dups
