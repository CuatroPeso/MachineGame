from kedro.pipeline import node, pipeline
from .nodes import summarize_datasets, describe_dataset


def create_pipeline(**kwargs):
    return pipeline([
        node(
            func=summarize_datasets,
            inputs=["games_raw", "steam_raw", "vg_sales_raw"],
            outputs="eda_summary",
            name="eda_summary_node",
            tags=["eda", "crispdm", "fase2"],
        ),
        node(
            func=describe_dataset,
            inputs=dict(df="games_raw", dataset_name="params:paths.games_raw"),
            outputs="games_describe",
            name="games_describe_node",
            tags=["eda"],
        ),
        node(
            func=describe_dataset,
            inputs=dict(df="steam_raw", dataset_name="params:paths.steam_raw"),
            outputs="steam_describe",
            name="steam_describe_node",
            tags=["eda"],
        ),
        node(
            func=describe_dataset,
            inputs=dict(df="vg_sales_raw", dataset_name="params:paths.vg_sales_raw"),
            outputs="vg_sales_describe",
            name="vg_sales_describe_node",
            tags=["eda"],
        ),
    ])
