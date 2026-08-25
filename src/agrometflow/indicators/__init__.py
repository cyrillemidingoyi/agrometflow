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

# Registre central des indicateurs
from .registry import (
    INDICATOR_REGISTRY,
    get_indicator,
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

     # Registry
    "INDICATOR_REGISTRY",
    "get_indicator",
]
