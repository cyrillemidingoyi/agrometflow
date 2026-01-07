from agrometflow.climate.power import PowerDownloader
from agrometflow.climate.cds import CDSDownloader
from agrometflow.climate.chirps import ChirpsDownloader  # si besoin
from agrometflow.climate.tamsat import TamsatDownloader
from agrometflow.climate.arc2 import Arc2Downloader
from agrometflow.climate.persiann import PersiannDownloader
from agrometflow.climate.cmorphv1 import Cmorphv1Downloader
from agrometflow.climate.rfe2 import Rfe2Downloader

def get_climate_source(source_name, **kwargs):
    """
    Retourne l'instance du téléchargeur climatique correspondant au nom de la source.

    Parameters
    ----------
    source_name : str
        Nom de la source climatique (ex. "nasapower", "era5", etc.)
    **kwargs : any
        Paramètres additionnels (log_file, verbose...)

    Returns
    -------
    ClimateSource
        Instance du téléchargeur compatible avec run_pipeline
    """
    source_name = source_name.lower()

    if source_name == "power":
        return PowerDownloader(**kwargs)
    elif source_name == "cds":
        return CDSDownloader(**kwargs)
    elif source_name == "chirps":
        return ChirpsDownloader(**kwargs)
    elif source_name == "tamsat":
        return TamsatDownloader(**kwargs)
    elif source_name == "arc2":
        return Arc2Downloader(**kwargs)
    elif source_name == "persiann":
        return PersiannDownloader(**kwargs)
    elif source_name == "cmorphv1":
        return Cmorphv1Downloader(**kwargs)
    elif source_name == "rfe2":
        return Rfe2Downloader(**kwargs)
    else:
        raise ValueError(f"Unknown climate source: '{source_name}'")
