
import argparse
from pathlib import Path

import pandas as pd

from modules.normalization import normalize_headers, apply_rename
from modules.typing_module import apply_types
from modules.anomalies import detect_anomalies
from modules.duplicates import drop_exact_duplicates
from modules.schema_module import apply_schema
from modules.config_loader import load_config, load_schema


def preview_schema(df, types_cfg: dict, schema_list: list | None):
    if schema_list:
        columns = schema_list
    else:
        columns = list(df.columns)

    def get_type(col: str) -> str:
        if col in (types_cfg.get("date", []) or []):
            return "date"
        if col in (types_cfg.get("int", []) or []):
            return "int"
        if col in (types_cfg.get("float", []) or []):
            return "float"
        if col in (types_cfg.get("string", []) or []):
            return "string"
        if col in df.columns:
            return str(df[col].dtype)
        return "missing"

    print("\nProposed structured schema:\n")
    for col in columns:
        col_type = get_type(col)
        present = col in df.columns
        print(f"- {col:<20} ({col_type:<10}) present={present}")
    print()


def clean_dataset(input_path: str, config_path: str, schema_path: str | None = None) -> dict:
    input_path = Path(input_path)
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    config = load_config(config_path)
    schema_cfg = load_schema(schema_path) if schema_path else {}

    df = pd.read_csv(input_path)

    # 1) Normalize headers
    df = normalize_headers(df)

    # 2) Rename columns from config
    df = apply_rename(df, config.get("rename_columns", {}))

    # 2b) PREVIEW SCHEMA DINAMICO
    types_cfg = config.get("types", {}) or {}
    raw_schema_list = schema_cfg.get("schema") or []

    if raw_schema_list and any(col in df.columns for col in raw_schema_list):
        schema_list = raw_schema_list
    else:
        print("No compatible schema found, using dataset columns as structured schema.\n")
        schema_list = list(df.columns)

    preview_schema(df, types_cfg, schema_list)

    # 3) Custom rule: office flags (optional)
    office_rule = config.get("office_flags")
    if office_rule:
        from modules.normalization import apply_office_flags
        df = apply_office_flags(df, office_rule)

    # 4) Types
    df = apply_types(df, types_cfg)

    # 5) Anomalies
    anomalies_cfg = config.get("anomalies", {})
    anomalies_df = detect_anomalies(df, anomalies_cfg)
    anomalies_path = None
    if not anomalies_df.empty:
        anomalies_path = input_path.with_name(input_path.stem + "_anomalies.csv")
        anomalies_df.to_csv(anomalies_path, index=False)
        print(f"Anomalies found: {len(anomalies_df)} (see {anomalies_path})")
    else:
        print("No anomalies detected.")

    # 6) Duplicates
    df, df_dups = drop_exact_duplicates(df)
    duplicates_path = None
    if not df_dups.empty:
        duplicates_path = input_path.with_name(input_path.stem + "_duplicates.csv")
        df_dups.to_csv(duplicates_path, index=False)
        print(f"Duplicate rows removed: {len(df_dups)} (see {duplicates_path})")
    else:
        print("No exact duplicate rows found.")

    # 7) Schema: solo se definito in schema.yaml
    if schema_cfg.get("schema"):
        df = apply_schema(df, schema_list)
        
    # 8) Save cleaned file
    output_path = input_path.with_name(input_path.stem + "_cleaned.csv")
    df.to_csv(output_path, index=False)
    print(f"Cleaned file saved to: {output_path}")

    return {
        "output_path": str(output_path),
        "anomalies_path": str(anomalies_path) if anomalies_path else None,
        "duplicates_path": str(duplicates_path) if duplicates_path else None,
    }


def main():
    parser = argparse.ArgumentParser(description="Standardize and clean a dataset.")
    parser.add_argument("input", help="Input CSV file path")
    parser.add_argument("--config", default="config.yaml", help="YAML config file")
    parser.add_argument("--schema", default="schema.yaml", help="YAML schema file")
    args = parser.parse_args()

    clean_dataset(args.input, args.config, args.schema)


if __name__ == "__main__":
    main()
