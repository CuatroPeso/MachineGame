from __future__ import annotations
from typing import Dict
from kedro.pipeline import Pipeline

# Importa los paquetes de pipelines como módulos (para usar .create_pipeline())
from .pipelines import business_understanding as bu
from .pipelines import data_understanding as du
from .pipelines import data_preparation as dp

# Importa directamente las factories de los pipelines de modelado
from machinegame.pipelines.modeling_regression import create_pipeline as mreg_create
from machinegame.pipelines.modeling_classification import create_pipeline as mcls_create


def register_pipelines() -> Dict[str, Pipeline]:
    p_business = bu.create_pipeline()
    p_eda = du.create_pipeline()
    p_prep = dp.create_pipeline()
    p_cls = mcls_create()
    p_reg = mreg_create()

    default = p_cls + p_reg + p_business + p_eda + p_prep
    return {
        "business_understanding": p_business,
        "data_understanding": p_eda,
        "data_preparation": p_prep,
        "modeling_classification": p_cls,
        "modeling_regression": p_reg,
        "__default__": default,
    }
