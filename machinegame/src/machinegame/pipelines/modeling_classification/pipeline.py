from __future__ import annotations
from kedro.pipeline import node, pipeline
from .nodes import (
    split_data_cls,
    train_classifiers,
    evaluate_classifiers,
)


def create_pipeline():
    return pipeline(
        [
            node(
                func=split_data_cls,
                inputs=dict(
                    df="model_input_classification",
                    params="params:model",
                ),
                outputs=["X_train_cls", "X_test_cls", "y_train_cls", "y_test_cls"],
                name="split_cls_node",
            ),
            node(
                func=train_classifiers,
                inputs=[
                    "X_train_cls",
                    "y_train_cls",
                    "params:cls.grid",
                    "params:cls.cv",
                ],
                outputs="cls_models",
                name="train_classifiers_node",
            ),
            node(
                func=evaluate_classifiers,
                inputs=["cls_models", "X_test_cls", "y_test_cls"],
                outputs="cls_report",
                name="evaluate_classifiers_node",
            ),
        ]
    )
