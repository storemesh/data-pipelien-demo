from kedro.pipeline import Pipeline, node, pipeline

from .nodes import (
    landing_func,
    create_catalog
)
def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=create_catalog,
                inputs="params:landing_chinook_catalog",
                outputs=None,
                name="create_func_node",
            ),
            node(
                func=landing_func,
                inputs=["params:postgres_key",
                        "params:lakefs_key",
                        "params:landing_chinook_catalog",
                        "params:select_table"
                       ],
                outputs=None,
                name="landing_func_node",
            )
        ]
    )
