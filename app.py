from pathlib import Path
import pandas as pd

from modules.normalization import normalize_headers, apply_rename, apply_office_flags
from modules.typing_module import apply_types, normalize_string_values
from modules.anomalies import detect_anomalies
from modules.duplicates import drop_exact_duplicates
from modules.schema_module import apply_schema
from modules.config_loader import load_config, load_schema


def preview_schema(df: pd.DataFrame, types_cfg: dict, schema_list: list | None):
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

    print("\n=== PROPOSED STRUCTURED SCHEMA ===\n")
    for col in columns:
        col_type = get_type(col)
        present = col in df.columns
        print(f"- {col:<20} ({col_type:<10}) present={present}")
    print("\n=================================\n")


def run_cleaning(input_path: Path, config_path: Path, schema_path: Path | None = None):
    config = load_config(str(config_path))
    schema_cfg = load_schema(str(schema_path)) if schema_path and schema_path.exists() else {}

    print(f"\nUsing config: {config_path}")
    if schema_cfg:
        print(f"Using schema: {schema_path}")
    else:
        print("No schema file or empty schema: output will keep current columns.")

    df = pd.read_csv(input_path)

    # 1) Normalizza intestazioni
    df = normalize_headers(df)

    # 2) Rinomina da config
    df = apply_rename(df, config.get("rename_columns", {}) or {})

    # 3) Decidi lo schema in modo dinamico
    types_cfg = config.get("types", {}) or {}
    raw_schema_list = schema_cfg.get("schema") or []

    # Se lo schema da YAML ha almeno una colonna presente nel dataset, usalo.
    # Altrimenti, usa direttamente le colonne del dataset come schema strutturato.
    if raw_schema_list and any(col in df.columns for col in raw_schema_list):
        schema_list = raw_schema_list
    else:
        print("No compatible schema found, using dataset columns as structured schema.\n")
        schema_list = list(df.columns)

    preview_schema(df, types_cfg, schema_list)

    # 4) Conferma
    choice = input("Proceed with cleaning? [y/N]: ").strip().lower()
    if choice not in ("y", "yes"):
        print("Cleaning aborted by user.")
        return

    # 5) Regole personalizzate (es. office_flags)
    office_rule = config.get("office_flags")
    if office_rule:
        df = apply_office_flags(df, office_rule)

    # 6) Tipi
    df = apply_types(df, types_cfg)

    # 6b) Normalizza valori testuali (minuscolo + underscore)
    df = normalize_string_values(df)

    # 7) Anomalie
    anomalies_cfg = config.get("anomalies", {}) or {}
    anomalies_df = detect_anomalies(df, anomalies_cfg)
    anomalies_path = None
    if not anomalies_df.empty:
        anomalies_path = input_path.with_name(input_path.stem + "_anomalies.csv")
        anomalies_df.to_csv(anomalies_path, index=False)
        print(f"Anomalies found: {len(anomalies_df)} (see {anomalies_path})")
    else:
        print("No anomalies detected based on current config.")

    # 8) Duplicati completi
    df, df_dups = drop_exact_duplicates(df)
    duplicates_path = None
    if not df_dups.empty:
        duplicates_path = input_path.with_name(input_path.stem + "_duplicates.csv")
        df_dups.to_csv(duplicates_path, index=False)
        print(f"Duplicate rows removed: {len(df_dups)} (see {duplicates_path})")
    else:
        print("No exact duplicate rows found.")

    # 9) Applica schema finale SOLO se è definito in schema.yaml
    if schema_cfg.get("schema"):
        df = apply_schema(df, schema_list)

    # 9b) aggiungi ID sequenziale
    df.insert(0, "id_seq", range(1, len(df) + 1))

    # 10) Salva file pulito (NA espliciti)
    output_path = input_path.with_name(input_path.stem + "_cleaned.csv")
    df.to_csv(output_path, index=False, na_rep="NA")
    print(f"\nCleaned file saved to: {output_path}\n")


def main():
    print("=== DATA CLEANER INTERACTIVE APP ===\n")
    raw = input("Enter dataset path (CSV): ").strip()
    if not raw:
        print("No file selected. Exiting.")
        return

    path = Path(raw)
    if not path.exists():
        print(f"File not found: {path}")
        return

    base_dir = Path(__file__).resolve().parent
    config_path = base_dir / "config.yaml"
    schema_path = base_dir / "schema.yaml"

    run_cleaning(path, config_path, schema_path)


if __name__ == "__main__":
    main()
