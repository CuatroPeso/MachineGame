from __future__ import annotations
from kedro.pipeline import Pipeline
from .pipelines import business_understanding as bu
from .pipelines import data_understanding as du
from .pipelines import data_preparation as dp

def register_pipelines() -> dict[str, Pipeline]:
    p_business = bu.create_pipeline()
    p_eda = du.create_pipeline()
    p_prep = dp.create_pipeline()
    default = p_business + p_eda + p_prep
    return {
        "business_understanding": p_business,
        "data_understanding": p_eda,
        "data_preparation": p_prep,
        "__default__": default,
    }
