import re
import pandas as pd


def detect_date_columns(df: pd.DataFrame) -> list[str]:
    """
    Regole:
    1. Il nome colonna deve contenere 'date'
    2. I valori devono contenere elementi da:
       - anno (es. 2014)
       - mese (numero o parola)
       - giorno (1-31)
    """
    candidate_cols = [c for c in df.columns if "date" in c.lower()]
    detected = []

    date_like_pattern = re.compile(
        r"""
        (
            # YYYY-MM-DD / YYYY/MM/DD
            \d{4}[-/]\d{1,2}[-/]\d{1,2}
            |
            # DD-MM-YYYY / MM-DD-YYYY
            \d{1,2}[-/]\d{1,2}[-/]\d{4}
            |
            # Date con mesi scritti (Dec, December, etc.)
            (jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)[a-z]*
            .*?\d{1,2}
            .*?\d{4}
        )
        """,
        re.IGNORECASE | re.VERBOSE,
    )

    for col in candidate_cols:
        series = df[col].astype("string").dropna()
        sample = series.head(50)

        if any(date_like_pattern.search(str(v)) for v in sample):
            detected.append(col)

    return detected

def _normalize_numeric_series(s: pd.Series) -> pd.Series:
    """
    Pulisce una serie che dovrebbe contenere numeri:
    - rimuove simboli non numerici (€, $, spazi, ecc.)
    - interpreta la virgola come separatore decimale
    - rimuove i punti usati come separatori migliaia
      es: '1.234,56' -> '1234.56'
    """
    s = s.astype("string").str.strip()

    # rimuove tutto ciò che NON è cifra, punto, virgola o segno meno
    s = s.str.replace(r"[^0-9,\.\-]", "", regex=True)

    # prima togliamo i punti (tipico separatore migliaia: 1.234,56)
    s = s.str.replace(".", "", regex=False)

    # poi trasformiamo la virgola in punto (decimale)
    s = s.str.replace(",", ".", regex=False)

    return s

def apply_types(df: pd.DataFrame, types_cfg: dict) -> pd.DataFrame:
    # tratta stringhe vuote come NA
    df = df.replace({"": pd.NA, " ": pd.NA})

    # 1) Date: auto + manuali da config
    auto_dates = detect_date_columns(df)
    manual_dates = types_cfg.get("date", []) if types_cfg else []
    date_columns = list(set(auto_dates + manual_dates))

    for col in date_columns:
        if col not in df.columns:
            continue

        # parse con utc=True per evitare problemi di timezone miste
        series = pd.to_datetime(df[col], errors="coerce", utc=True)

        # rimuovi il timezone (portiamo tutto a naive)
        series = series.dt.tz_convert(None)

        # formato standard: MM-DD-YYYY
        df[col] = series.dt.strftime("%m-%d-%Y")

    # 2) Integers
    for col in (types_cfg.get("int", []) if types_cfg else []):
        if col in df.columns:
            s = _normalize_numeric_series(df[col])
            df[col] = pd.to_numeric(s, errors="coerce").astype("Int64")

    # 3) Floats
    for col in (types_cfg.get("float", []) if types_cfg else []):
        if col in df.columns:
            s = _normalize_numeric_series(df[col])
            df[col] = pd.to_numeric(s, errors="coerce")

    # 4) Strings (solo cast, la normalizzazione è sotto)
    for col in (types_cfg.get("string", []) if types_cfg else []):
        if col in df.columns:
            df[col] = df[col].astype("string")

    return df

def normalize_string_values(df: pd.DataFrame, string_cols: list[str] | None = None) -> pd.DataFrame:
    """
    Porta tutte le colonne string:
    - in minuscolo
    - senza spazi (sostituiti da underscore)
    - strip spazi iniziali/finali
    """
    if string_cols is None:
        string_cols = [
            c for c in df.columns
            if df[c].dtype == "string" or df[c].dtype == "object"
        ]

    for col in string_cols:
        df[col] = df[col].astype("string")
        df[col] = df[col].str.strip()
        df[col] = df[col].str.lower()
        df[col] = df[col].str.replace(r"\s+", "_", regex=True)

    return df
