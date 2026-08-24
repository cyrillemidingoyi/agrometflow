"""
Indicateurs de précipitation
"""

from .cumulative import CumulativeRainfall
from .rainy_days import RainyDays
from .dry_days import DryDays
from .dry_spell import DrySpell
from .SDII import SDII
from .RG95P import R95p

# Alias pour compatibilité ascendante
NumberOfRainyDays = RainyDays
ConsecutiveDryDays = DrySpell
ConsecutiveWetDays = DrySpell

__all__ = [
    'CumulativeRainfall',
    'RainyDays',
    'DryDays',
    'DrySpell',
    'NumberOfRainyDays',
    'ConsecutiveDryDays',
    'ConsecutiveWetDays',
    'SDII',
    'R95p',
]
