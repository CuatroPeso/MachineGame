from __future__ import annotations
from typing import Any, Dict, List, Tuple

import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.model_selection import train_test_split, GridSearchCV, StratifiedKFold
from sklearn.pipeline import Pipeline as SkPipeline
from sklearn.metrics import f1_score, accuracy_score

# Modelos de clasificación
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.svm import SVC


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
def split_data_cls(
    df: pd.DataFrame,
    params: Dict[str, Any],
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.Series, pd.Series]:
    """
    Inputs:
      - df: model_input_classification
      - params: params:model
    """
    target = params.get("target_cls", "Rating")
    test_size = float(params.get("test_size", 0.2))
    random_state = int(params.get("random_state", 42))
    use_stratify = bool(params.get("stratify", True))

    X, y = _split_feature_target(df, target)

    # Seguridad: si alguna clase tiene 1 instancia, estratificar rompe. Forzamos mínimo.
    stratify_vec = y if use_stratify and y.value_counts().min() >= 2 else None

    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=test_size,
        random_state=random_state,
        stratify=stratify_vec,
    )
    return X_train, X_test, y_train, y_test


def train_classifiers(
    X_train: pd.DataFrame,
    y_train: pd.Series,
    grid_cfg: Dict[str, Any],
    cv_cfg: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Inputs:
      - X_train_cls, y_train_cls
      - params:cls.grid
      - params:cls.cv
    Output:
      - cls_models
    """
    n_splits = int(cv_cfg.get("n_splits", 3))
    shuffle = bool(cv_cfg.get("shuffle", True))
    random_state = int(cv_cfg.get("random_state", 42))
    cv = StratifiedKFold(n_splits=n_splits, shuffle=shuffle, random_state=random_state)

    pre = _make_preprocessor(X_train)

    model_space: Dict[str, SkPipeline] = {
        "logreg": SkPipeline(steps=[("pre", pre), ("model", LogisticRegression(max_iter=2000))]),
        "rf": SkPipeline(steps=[("pre", pre), ("model", RandomForestClassifier(random_state=random_state))]),
        "svc": SkPipeline(steps=[("pre", pre), ("model", SVC())]),
    }

    default_grids: Dict[str, Dict[str, List[Any]]] = {
        "logreg": {"model__C": [0.1, 1.0, 10.0]},
        "rf": {
            "model__n_estimators": [100, 300],
            "model__max_depth": [None, 8, 16],
        },
        "svc": {"model__C": [0.1, 1.0, 10.0], "model__kernel": ["rbf", "linear"]},
    }

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
            param_grid=grid if grid else [{}],
            cv=cv,
            n_jobs=-1,
            scoring="f1_macro",
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


def evaluate_classifiers(
    models: Dict[str, Any],
    X_test: pd.DataFrame,
    y_test: pd.Series,
) -> pd.DataFrame:
    """
    Inputs:
      - cls_models
      - X_test_cls, y_test_cls
    Output:
      - cls_report
    """
    rows = []
    for name, info in models.items():
        model = info["best_estimator"]
        y_pred = model.predict(X_test)
        f1_macro = f1_score(y_test, y_pred, average="macro")
        acc = accuracy_score(y_test, y_pred)
        rows.append(
            {
                "model": name,
                "f1_macro": float(f1_macro),
                "accuracy": float(acc),
                "cv_best": float(info.get("best_score_cv")) if info.get("best_score_cv") is not None else None,
                "best_params": info.get("best_params"),
            }
        )
    report = pd.DataFrame(rows).sort_values(by="f1_macro", ascending=False)
    return report
