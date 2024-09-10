from kedro.pipeline import Pipeline, node, pipeline

from .nodes import (
    staging01,
    show
)
def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=staging01,
                inputs=[
                        "landing_app01_db1_customer",
                        "landing_app01_db2_customers"
                       ],
                outputs="integrate_customer",
                name="customer_integrate_node",
            ),
            node(
                func=show,
                inputs="integrate_customer",
                outputs=None,
                name="show_customer_integrate_node",
            )
        ]
    )