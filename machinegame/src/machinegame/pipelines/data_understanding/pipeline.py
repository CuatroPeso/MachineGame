from kedro.pipeline import node, pipeline
from .nodes import build_business_report, inventory_raw_datasets


def create_pipeline(**kwargs):
    return pipeline([
        node(
            func=build_business_report,
            inputs=[
                "params:business.objectives",
                "params:business.questions",
                "params:business.success_metrics",
            ],
            outputs="business_report",
            name="business_report_node",
            tags=["business", "crispdm", "fase1"],
        ),
        node(
            func=inventory_raw_datasets,
            inputs=[
                "params:paths.games_raw",
                "params:paths.steam_raw",
                "params:paths.vg_sales_raw",
            ],
            outputs="dataset_inventory",
            name="dataset_inventory_node",
            tags=["business", "inventario"],
        ),
    ])
