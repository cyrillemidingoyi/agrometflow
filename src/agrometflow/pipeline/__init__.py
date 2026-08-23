"""
Module pipeline - Orchestration des étapes de calcul.
"""

from .pipeline import run_pipeline, run_pipeline_from_yaml

__all__ = [
    'run_pipeline',
    'run_pipeline_from_yaml',
]