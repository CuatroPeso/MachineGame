from kedro.pipeline import node, pipeline
from .nodes import clean_dataset, add_basic_features


def create_pipeline(**kwargs):
    return pipeline([
        # Games
        node(
            func=clean_dataset,
            inputs=dict(
                df="games_raw",
                drop_duplicates="params:prep.drop_duplicates",
                drop_all_null_cols="params:prep.drop_all_null_cols",
                fillna_strategy="params:prep.fillna_strategy",
            ),
            outputs="games_clean",
            name="clean_games",
            tags=["prep", "crispdm", "fase3"],
        ),
        node(
            func=add_basic_features,
            inputs="games_clean",
            outputs="games_prepared",     # <- NUEVO nombre
            name="features_games",
        ),

        # Steam
        node(
            func=clean_dataset,
            inputs=dict(
                df="steam_raw",
                drop_duplicates="params:prep.drop_duplicates",
                drop_all_null_cols="params:prep.drop_all_null_cols",
                fillna_strategy="params:prep.fillna_strategy",
            ),
            outputs="steam_clean",
            name="clean_steam",
        ),
        node(
            func=add_basic_features,
            inputs="steam_clean",
            outputs="steam_prepared",     # <- NUEVO nombre
            name="features_steam",
        ),

        # VG Sales
        node(
            func=clean_dataset,
            inputs=dict(
                df="vg_sales_raw",
                drop_duplicates="params:prep.drop_duplicates",
                drop_all_null_cols="params:prep.drop_all_null_cols",
                fillna_strategy="params:prep.fillna_strategy",
            ),
            outputs="vg_sales_clean",
            name="clean_vg_sales",
        ),
        node(
            func=add_basic_features,
            inputs="vg_sales_clean",
            outputs="vg_sales_prepared",  # <- NUEVO nombre
            name="features_vg_sales",
        ),
    ])
