"""
Registre central des indicateurs AgroMetFlow.
"""

from agrometflow.indicators.precipitation import (
    CumulativeRainfall,
    RainyDays,
    DryDays,
    DrySpell,
    SDII,
    R95p,
)

from agrometflow.indicators.temperature import (
    MeanTemperature,
    GrowingDegreeDays,
)


INDICATOR_REGISTRY = {
    "cumulative_precipitation": CumulativeRainfall,
    "rainy_days": RainyDays,
    "dry_days": DryDays,
    "dry_spell": DrySpell,
    "sdii": SDII,
    "r95p": R95p,
    "mean_temperature": MeanTemperature,
    "growing_degree_days": GrowingDegreeDays,
}


def get_indicator(name):
    """
    Retourne la classe correspondant à un indicateur.

    Parameters
    ----------
    name : str
        Nom de l'indicateur utilisé dans la configuration.

    Returns
    -------
    class
        Classe de l'indicateur demandé.

    Raises
    ------
    ValueError
        Si l'indicateur n'est pas enregistré.
    """

    if name not in INDICATOR_REGISTRY:
        available = ", ".join(
            sorted(INDICATOR_REGISTRY.keys())
        )

        raise ValueError(
            f"Indicateur inconnu : {name}. "
            f"Indicateurs disponibles : {available}"
        )

    return INDICATOR_REGISTRY[name]