from __future__ import annotations
import os
from pathlib import Path
import pandas as pd


def build_business_report(objectives: list, questions: list, success_metrics: list) -> pd.DataFrame:
    """Construye un reporte de Comprensión del Negocio en forma tabular."""
    rows = []
    for x in objectives:
        rows.append({"seccion": "Objetivos", "item": x})
    for x in questions:
        rows.append({"seccion": "Preguntas de Negocio", "item": x})
    for x in success_metrics:
        rows.append({"seccion": "Métricas de Éxito", "item": x})
    return pd.DataFrame(rows)


def inventory_raw_datasets(games_path: str, steam_path: str, vg_path: str) -> pd.DataFrame:
    """Inventario de datasets RAW: existencia, tamaño y columnas de muestra."""
    records = []
    for name, p in [
        ("games_raw", games_path),
        ("steam_raw", steam_path),
        ("vg_sales_raw", vg_path),
    ]:
        path = Path(p)
        exists = path.exists()
        size = path.stat().st_size if exists else 0
        sample_cols = ""
        if exists:
            try:
                sample_cols = ",".join(pd.read_csv(path, nrows=0).columns[:12])
            except Exception as e:  # noqa: BLE001 (Kedro suele preferir excepciones explícitas, pero aquí sirve rápido)
                sample_cols = f"error_leyendo_header: {e}"
        records.append(
            {
                "dataset": name,
                "filepath": str(path),
                "exists": bool(exists),
                "size_bytes": int(size),
                "sample_columns": sample_cols,
            }
        )
    return pd.DataFrame(records)
