def __getattr__(name):
    """Lazy-import downloaders so heavy deps (xarray, rioxarray…) are only
    loaded when actually accessed."""
    _map = {
        "PowerDownloader": "agrometflow.climate.power",
        "GHCNDDownloader": "agrometflow.climate.ghcnd",
        "CDSDownloader": "agrometflow.climate.cds",
        "LSASAFDownloader": "agrometflow.climate.lsasaf",
        "ChirpsDownloader": "agrometflow.climate.chirps",
        "TamsatDownloader": "agrometflow.climate.tamsat",
        "Arc2Downloader": "agrometflow.climate.arc2",
        "PersiannDownloader": "agrometflow.climate.persiann",
        "Cmorphv1Downloader": "agrometflow.climate.cmorphv1",
        "Rfe2Downloader": "agrometflow.climate.rfe2",
    }
    if name in _map:
        import importlib
        mod = importlib.import_module(_map[name])
        return getattr(mod, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


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

    _factories = {
        "power": ("agrometflow.climate.power", "PowerDownloader"),
        "cds": ("agrometflow.climate.cds", "CDSDownloader"),
        "lsasaf": ("agrometflow.climate.lsasaf", "LSASAFDownloader"),
        "lsasaf_http": ("agrometflow.climate.lsasaf", "LSASAFDownloader"),
        "chirps": ("agrometflow.climate.chirps", "ChirpsDownloader"),
        "tamsat": ("agrometflow.climate.tamsat", "TamsatDownloader"),
        "arc2": ("agrometflow.climate.arc2", "Arc2Downloader"),
        "persiann": ("agrometflow.climate.persiann", "PersiannDownloader"),
        "cmorphv1": ("agrometflow.climate.cmorphv1", "Cmorphv1Downloader"),
        "rfe2": ("agrometflow.climate.rfe2", "Rfe2Downloader"),
        "ghcnd": ("agrometflow.climate.ghcnd", "GHCNDDownloader"),
    }

    if source_name not in _factories:
        raise ValueError(f"Unknown climate source: '{source_name}'")

    import importlib
    mod_path, cls_name = _factories[source_name]
    mod = importlib.import_module(mod_path)
    return getattr(mod, cls_name)(**kwargs)
