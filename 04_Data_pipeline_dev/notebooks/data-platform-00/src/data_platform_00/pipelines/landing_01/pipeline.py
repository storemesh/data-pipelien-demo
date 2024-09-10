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
                inputs="params:landing_catalog",
                outputs=None,
                name="create_landing_node",
            ),
            node(
                func=create_catalog,
                inputs="params:integration_catalog",
                outputs=None,
                name="create_integrate_node",
            ),
            node(
                func=landing_func,
                inputs=["params:source_01",
                        "params:lakefs_key",
                        "params:landing_catalog"
                       ],
                outputs=None,
                name="landing_source1_node",
            ),
            node(
                func=landing_func,
                inputs=["params:source_02",
                        "params:lakefs_key",
                        "params:landing_catalog"
                       ],
                outputs=None,
                name="landing_source2_node",
            )
        ]
    )
