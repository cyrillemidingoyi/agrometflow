"""
Indicateurs agroclimatiques
"""

from .base import BaseIndicator

# Précipitation
from .precipitation import (
    CumulativeRainfall,
    RainyDays,
    DryDays,
    DrySpell,
    NumberOfRainyDays,
    ConsecutiveDryDays,
    ConsecutiveWetDays,
    SDII,
    R95p,
)

# Température - Import depuis le dossier temperature/
from .temperature import (
    MeanTemperature,
    GrowingDegreeDays,
)

__all__ = [
    'BaseIndicator',
    # Précipitation
    'CumulativeRainfall',
    'RainyDays',
    'DryDays',
    'DrySpell',
    'NumberOfRainyDays',
    'ConsecutiveDryDays',
    'ConsecutiveWetDays',
    'SDII',
    'R95p',
    # Température
    'MeanTemperature',
    'GrowingDegreeDays',
]
