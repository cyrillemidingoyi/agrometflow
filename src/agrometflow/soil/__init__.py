from agrometflow.soil.soilgrids import SoilGridsFetcher


def get_soil_source(source_name, **kwargs):
    """
    Retourne l'instance du téléchargeur pédologique correspondant au nom de la source.

    Parameters
    ----------
    source_name : str
        Nom de la source pédologique (ex. "soilgruds" etc.)
    **kwargs : any
        Paramètres additionnels (log_file, verbose...)

    Returns
    -------
    ClimateSource
        Instance du téléchargeur compatible avec run_pipeline
    """
    source_name = source_name.lower()

    if source_name == "soilgrids":
        return SoilGridsFetcher(**kwargs)
    else:
        raise ValueError(f"Unknown soil source: '{source_name}'")