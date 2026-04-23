def __getattr__(name):
    _map = {
        "SoilGridsFetcher": "agrometflow.soil.soilgrids",
    }
    if name in _map:
        import importlib

        mod = importlib.import_module(_map[name])
        return getattr(mod, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def get_soil_source(source_name, **kwargs):
    """
    Retourne l'instance du téléchargeur pédologique correspondant au nom de la source.
    """
    source_name = source_name.lower()

    _factories = {
        "soilgrids": ("agrometflow.soil.soilgrids", "SoilGridsFetcher"),
    }

    if source_name not in _factories:
        raise ValueError(f"Unknown soil source: '{source_name}'")

    import importlib

    mod_path, cls_name = _factories[source_name]
    mod = importlib.import_module(mod_path)
    return getattr(mod, cls_name)(**kwargs)
