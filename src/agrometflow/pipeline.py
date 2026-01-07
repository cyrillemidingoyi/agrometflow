from agrometflow.climate import get_climate_source
from agrometflow.utils import get_logger
from agrometflow.soil import get_soil_source
from agrometflow.config_loader import load_config
from agrometflow.utils import resolve_variables
from agrometflow.utils import write_cdsapirc_from_config
from agrometflow.projections import get_projection_source



def run_pipeline(config):
    """
    Run the full or partial data processing pipeline based on user configuration.

    Parameters
    ----------
    config : dict
        Dictionary with keys like:
        {
            "climate": {
                "source": "power",
                "bbox": (...),
                "start_date": "...",
                "end_date": "...",
                "variables": [...],
                "output_dir": "...",
                "resolution": 0.1
            },
            "soil": {
                "source": "soilgrids",
                "bbox": (...),
                "variables": [...],
                "depth": "0-5cm",
                "output_dir": "...",
                "resolution": 250
            }
        }
    """

    results = {}

    # Récupération des options globales
    global_cfg = config.get("global", {})
    log_file = global_cfg.get("log_file")
    verbose = global_cfg.get("verbose", False)
    project_name = global_cfg.get("project_name", "agrometflow_project")

    logger = get_logger("agrometflow", log_file, verbose)
    logger.info(f" Starting pipeline for project: {project_name}")

    # Process climate block if present
    if "climate" in config:
        climate_cfg = config["climate"]
        source = climate_cfg.get("source", "cds")
        product = climate_cfg.get("product", "AgERA5")
        logger.info(f"Climate source: {source}")
        if source == "cds":
            cdsapi_config = config.get("cdsapi", {})
            climate_cfg.update(cdsapi_config)
            write_cdsapirc_from_config(cdsapi_config, logger=logger)
        try:
            climate_cfg["variables"] = resolve_variables(source, product, climate_cfg["variables"], logger)
            logger.info(f"Resolved variables: {climate_cfg['variables']}")
        except ValueError:
            logger.error("Pipeline aborted due to invalid variables.")
            return {}
    
        downloader = get_climate_source(source, log_file=log_file, verbose=verbose)
        logger.info(f"climate_cfg: {climate_cfg}")
        downloader.download(**climate_cfg)
        if "points" in climate_cfg: results["climate"] = downloader.extract()
        logger.info("Climate data retrieved and processed.")

    # Process soil block if present
    if "soil" in config:
        soil_cfg = config["soil"]
        source = soil_cfg.get("source", "soilgrids")
        logger.info(f"Soil source: {source}")
        downloader = get_soil_source(source, log_file=log_file, verbose=verbose)
        downloader.download(**soil_cfg)
        results["soil"] = downloader.extract()
        logger.info("Soil data retrieved and processed.")
    
    if "projections" in config:
        projections_cfg = config["projections"]
        esgf_config = config.get("esgf", {})
        projections_cfg.update(esgf_config)
        source = projections_cfg.get("source", "CMIP6")
        logger.info(f"Projections source: {projections_cfg.get('source', 'default')}")
        downloader = get_projection_source(source, log_file=log_file, verbose=verbose)
        downloader.download(**projections_cfg)

    return results



def run_pipeline_from_yaml(path_to_yaml):
    """
    Charge un fichier YAML de configuration et exécute le pipeline complet.

    Parameters
    ----------
    path_to_yaml : str
        Chemin vers le fichier de configuration .yaml

    Returns
    -------
    dict
        Résultats du pipeline, structurés par source
    """
    config = load_config(path_to_yaml)
    return run_pipeline(config)