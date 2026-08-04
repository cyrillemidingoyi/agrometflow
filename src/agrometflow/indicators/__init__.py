"""
Indicateurs agroclimatiques
"""

from .base import BaseIndicator
from .precipitation import (
    CumulativeRainfall,
    NumberOfRainyDays,
    ConsecutiveDryDays
)

__all__ = [
    'BaseIndicator',
    'CumulativeRainfall',
    'NumberOfRainyDays',
    'ConsecutiveDryDays',
]