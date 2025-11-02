from __future__ import annotations
import pandas as pd


def summarize_datasets(games_raw: pd.DataFrame, steam_raw: pd.DataFrame, vg_sales_raw: pd.DataFrame) -> pd.DataFrame:
    """Resumen global: filas, columnas y nulos totales."""
    summaries = []
    for name, df in [
        ("games", games_raw),
        ("steam", steam_raw),
        ("vg_sales", vg_sales_raw),
    ]:
        summaries.append(
            {
                "dataset": name,
                "filas": int(df.shape[0]),
                "columnas": int(df.shape[1]),
                "nulos_totales": int(df.isna().sum().sum()),
            }
        )
    return pd.DataFrame(summaries)



def describe_dataset(df: pd.DataFrame, dataset_name: str) -> pd.DataFrame:
    """Describe include='all' compatible con pandas antiguos."""
    try:
        # En pandas nuevos existe datetime_is_numeric
        desc = df.describe(include="all", datetime_is_numeric=True).transpose()
    except TypeError:
        # Fallback para pandas viejos
        desc = df.describe(include="all").transpose()

    desc.reset_index(inplace=True)
    desc.rename(columns={"index": "columna"}, inplace=True)
    desc.insert(0, "dataset", dataset_name)
    # Evitar objetos no serializables al guardar CSV
    return desc.astype("object").where(pd.notnull(desc), None)

