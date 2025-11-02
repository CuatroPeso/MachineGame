from __future__ import annotations
from typing import Any, Dict, List, Tuple

import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.model_selection import train_test_split, GridSearchCV, KFold
from sklearn.pipeline import Pipeline as SkPipeline
from sklearn.metrics import r2_score, mean_absolute_error, mean_squared_error

# Modelos base de regresión (agrega/quita si quieres)
from sklearn.linear_model import LinearRegression, Ridge, Lasso
from sklearn.ensemble import RandomForestRegressor


# =============================
# Utilidades compartidas
# =============================
def _split_feature_target(df: pd.DataFrame, target_col: str) -> Tuple[pd.DataFrame, pd.Series]:
    if target_col not in df.columns:
        raise KeyError(
            f"Target '{target_col}' no existe en dataframe. "
            f"Columnas: {list(df.columns)}"
        )
    df = df.copy()
    df = df.dropna(subset=[target_col])
    y = df[target_col]
    X = df.drop(columns=[target_col])
    return X, y


def _make_preprocessor(X: pd.DataFrame) -> ColumnTransformer:
    # Detecta columnas numéricas y categóricas automáticamente
    numeric_features = X.select_dtypes(include=["number", "bool"]).columns.tolist()
    categorical_features = X.select_dtypes(exclude=["number", "bool"]).columns.tolist()

    numeric_transformer = StandardScaler()
    categorical_transformer = OneHotEncoder(handle_unknown="ignore", sparse_output=False)

    pre = ColumnTransformer(
        transformers=[
            ("num", numeric_transformer, numeric_features),
            ("cat", categorical_transformer, categorical_features),
        ],
        remainder="drop",
        verbose_feature_names_out=False,
    )
    return pre


# =============================
# Nodos del pipeline
# =============================
def split_data_reg(
    df: pd.DataFrame,
    params: Dict[str, Any],
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.Series, pd.Series]:
    """
    Inputs esperados en pipeline:
      - df: model_input_regression (ParquetDataset)
      - params: params:model (MemoryDataset de parámetros)
    """
    target = params.get("target_reg", "Critic_Score")
    test_size = float(params.get("test_size", 0.2))
    random_state = int(params.get("random_state", 42))

    X, y = _split_feature_target(df, target)
    # En regresión NO se usa stratify
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, random_state=random_state
    )
    return X_train, X_test, y_train, y_test


def train_regressors(
    X_train: pd.DataFrame,
    y_train: pd.Series,
    grid_cfg: Dict[str, Any],
    cv_cfg: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Inputs esperados en pipeline:
      - X_train_reg, y_train_reg
      - params:reg.grid (dict)
      - params:reg.cv (dict)
    Salida:
      - reg_models (dict serializable en memoria)
    """
    # CV
    n_splits = int(cv_cfg.get("n_splits", 3))
    shuffle = bool(cv_cfg.get("shuffle", True))
    random_state = int(cv_cfg.get("random_state", 42))
    cv = KFold(n_splits=n_splits, shuffle=shuffle, random_state=random_state)

    pre = _make_preprocessor(X_train)

    # Model zoo
    model_space: Dict[str, SkPipeline] = {
        "linreg": SkPipeline(steps=[("pre", pre), ("model", LinearRegression())]),
        "ridge": SkPipeline(steps=[("pre", pre), ("model", Ridge())]),
        "lasso": SkPipeline(steps=[("pre", pre), ("model", Lasso(max_iter=2000))]),
        "rf": SkPipeline(steps=[("pre", pre), ("model", RandomForestRegressor(random_state=random_state))]),
    }

    # Grids por modelo: si grid_cfg está vacío, ponemos defaults pequeñitos
    default_grids: Dict[str, Dict[str, List[Any]]] = {
        "linreg": {},  # sin hiperparámetros
        "ridge": {"model__alpha": [0.1, 1.0, 10.0]},
        "lasso": {"model__alpha": [0.0001, 0.001, 0.01]},
        "rf": {
            "model__n_estimators": [100, 300],
            "model__max_depth": [None, 8, 16],
        },
    }

    # Si el usuario trajo grillas, se fusionan; si no, usamos las default
    full_grids: Dict[str, Dict[str, List[Any]]] = {}
    for name in model_space.keys():
        user_grid = grid_cfg.get(name, {})
        if user_grid:
            full_grids[name] = user_grid
        else:
            full_grids[name] = default_grids.get(name, {})

    results: Dict[str, Any] = {}
    for name, pipe in model_space.items():
        grid = full_grids.get(name, {})
        gs = GridSearchCV(
            estimator=pipe,
            param_grid=grid if grid else [{}],  # si vacío, que ejecute una sola combinación
            cv=cv,
            n_jobs=-1,
            scoring="r2",
            refit=True,
            error_score="raise",
        )
        gs.fit(X_train, y_train)
        results[name] = {
            "best_estimator": gs.best_estimator_,
            "best_params": gs.best_params_,
            "best_score_cv": float(gs.best_score_) if hasattr(gs, "best_score_") else None,
        }
    return results


def evaluate_regressors(
    models: Dict[str, Any],
    X_test: pd.DataFrame,
    y_test: pd.Series,
) -> pd.DataFrame:
    """
    Inputs:
      - reg_models
      - X_test_reg, y_test_reg
    Output:
      - reg_report (DataFrame con métricas)
    """
    rows = []
    for name, info in models.items():
        model = info["best_estimator"]
        y_pred = model.predict(X_test)
        r2 = r2_score(y_test, y_pred)
        mae = mean_absolute_error(y_test, y_pred)
        rmse = mean_squared_error(y_test, y_pred, squared=False)
        rows.append(
            {
                "model": name,
                "r2": float(r2),
                "mae": float(mae),
                "rmse": float(rmse),
                "cv_best": float(info.get("best_score_cv")) if info.get("best_score_cv") is not None else None,
                "best_params": info.get("best_params"),
            }
        )
    report = pd.DataFrame(rows).sort_values(by="r2", ascending=False)
    return report
