"""Project pipelines."""
from typing import Dict

import sys
import os
sys.path.append(os.getcwd())


from kedro.framework.project import find_pipelines
from kedro.pipeline import Pipeline

from src.data_platform_00.pipelines import landing_01 as landing
from src.data_platform_00.pipelines import staging_01 as staging



def register_pipelines() -> Dict[str, Pipeline]:
    """Register the project's pipelines.

    Returns:
        A mapping from pipeline names to ``Pipeline`` objects.
    """
    pipelines = find_pipelines()
    pipelines["__default__"] = sum(pipelines.values())
    
    landing_obj = landing.create_pipeline()
    staging_obj = staging.create_pipeline()

    
    
    return {
        "landing" : landing_obj,
        "staging" : staging_obj
    }
