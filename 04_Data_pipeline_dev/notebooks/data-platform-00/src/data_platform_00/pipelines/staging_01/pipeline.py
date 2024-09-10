from kedro.pipeline import Pipeline, node, pipeline

from .nodes import (
    show
)
def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=show,
                inputs="landing_lakefs_customer",
                outputs=None,
                name="show_node",
            )
        ]
    )