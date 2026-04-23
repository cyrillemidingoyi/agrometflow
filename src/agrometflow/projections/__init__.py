def __getattr__(name):
    _map = {
        "CMIP6Downloader": "agrometflow.projections.cmip6",
    }
    if name in _map:
        import importlib

        mod = importlib.import_module(_map[name])
        return getattr(mod, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def get_projection_source(source_name, **kwargs):
    """
    Retourne l'instance du téléchargeur de projections correspondant au nom de la source.
    """
    source_name = source_name.lower()

    _factories = {
        "cmip6": ("agrometflow.projections.cmip6", "CMIP6Downloader"),
    }

    if source_name not in _factories:
        raise ValueError(f"Unknown projection source: '{source_name}'")

    import importlib

    mod_path, cls_name = _factories[source_name]
    mod = importlib.import_module(mod_path)
    return getattr(mod, cls_name)(**kwargs)
