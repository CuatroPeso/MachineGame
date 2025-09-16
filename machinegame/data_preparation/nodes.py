import pandas as pd
import numpy as np

def clean_dataset(
    df: pd.DataFrame,
    drop_duplicates: bool = True,
    drop_all_null_cols: bool = True,
    fillna_strategy: str = "zero",
) -> pd.DataFrame:
    """Limpieza básica respetando tipos para evitar columnas mixtas (parquet)."""
    df = df.copy()

    if drop_duplicates:
        df = df.drop_duplicates()

    if drop_all_null_cols:
        df = df.dropna(axis=1, how="all")

    # Detectar por tipo
    num_cols  = df.select_dtypes(include=["number"]).columns
    bool_cols = df.select_dtypes(include=["bool"]).columns
    txt_cols  = df.select_dtypes(include=["object", "string", "category"]).columns
    # No tocamos fechas

    # Relleno por tipo
    if fillna_strategy == "median":
        if len(num_cols) > 0:
            df[num_cols] = df[num_cols].fillna(df[num_cols].median(numeric_only=True))
    else:  # "zero"
        if len(num_cols) > 0:
            df[num_cols] = df[num_cols].fillna(0)

    if len(bool_cols) > 0:
        df[bool_cols] = df[bool_cols].fillna(False)

    if len(txt_cols) > 0:
        df[txt_cols] = df[txt_cols].astype("string").fillna("")

    return df


def add_basic_features(df: pd.DataFrame) -> pd.DataFrame:
    """Feature engineering sencillo: conteo y ratio de nulos por fila."""
    if df.empty:
        return df
    df = df.copy()
    na_count = df.isna().sum(axis=1)
    df["row_na_count"] = na_count
    df["row_na_ratio"] = (na_count / df.shape[1]).round(4)
    return df
