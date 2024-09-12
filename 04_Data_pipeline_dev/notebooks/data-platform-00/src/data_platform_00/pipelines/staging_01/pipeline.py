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
                        "l.app01.db1.customer",
                        "l.app01.db2.customers"
                       ],
                outputs="i.customer",
                name="customer_integrate_node",
            ),
            node(
                func=show,
                inputs="i.customer",
                outputs=None,
                name="show_customer_integrate_node",
            )
        ]
    )