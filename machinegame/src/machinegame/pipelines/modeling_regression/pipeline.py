from __future__ import annotations
from kedro.pipeline import node, pipeline
from .nodes import (
    split_data_reg,
    train_regressors,
    evaluate_regressors,
)


def create_pipeline():
    return pipeline(
        [
            node(
                func=split_data_reg,
                inputs=dict(
                    df="model_input_regression",
                    params="params:model",
                ),
                outputs=["X_train_reg", "X_test_reg", "y_train_reg", "y_test_reg"],
                name="split_reg_node",
            ),
            node(
                func=train_regressors,
                inputs=[
                    "X_train_reg",
                    "y_train_reg",
                    "params:reg.grid",
                    "params:reg.cv",
                ],
                outputs="reg_models",
                name="train_regressors_node",
            ),
            node(
                func=evaluate_regressors,
                inputs=["reg_models", "X_test_reg", "y_test_reg"],
                outputs="reg_report",
                name="evaluate_regressors_node",
            ),
        ]
    )
