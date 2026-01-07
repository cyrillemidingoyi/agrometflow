from agrometflow.projections.cmip6 import CMIP6Downloader

def get_projection_source(source_name, **kwargs):
    """
    Retourne l'instance du téléchargeur des données de projection correspondant au nom de la source.

    Parameters
    ----------
    source_name : str
        Nom de la source  (ex. "cmip6", etc.)
    **kwargs : any
        Paramètres additionnels (log_file, verbose...)

    Returns
    -------
    ProjectionSource
        Instance du téléchargeur compatible avec run_pipeline
    """
    source_name = source_name.lower()

    if source_name == "cmip6":
        return CMIP6Downloader(**kwargs)
    else:
        raise ValueError(f"Unknown projection source: '{source_name}'")
