from pathlib import Path

import pandas as pd
import xarray as xr

from agrometflow.climate import get_climate_source
from agrometflow.utils import get_logger
from agrometflow.soil import get_soil_source
from agrometflow.config_loader import load_config
from agrometflow.utils import resolve_variables
from agrometflow.utils import write_cdsapirc_from_config
from agrometflow.projections import get_projection_source

from agrometflow.indicators import get_indicator

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
    
        downloader = get_climate_source(source, product=product,log_file=log_file, verbose=verbose)
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
    
    # Process indicators block if present
    if "indicators" in config:
        indicators_cfg = config["indicators"]

        logger.info("Starting indicators computation.")

        # 1. RÉCUPÉRATION DES DONNÉES D'ENTRÉE

        climate_data = None
        input_file = indicators_cfg.get("input")

        # Fichier fourni directement par l'utilisateur

        if input_file:
            input_path = Path(input_file)

            logger.info(
                f"Loading custom input data: {input_path}"
            )

            if not input_path.exists():
                raise FileNotFoundError(
                    f"Input file not found: {input_path}"
                )

            suffix = input_path.suffix.lower()

            # NetCDF : données spatiales
            if suffix in {".nc", ".nc4"}:
                climate_data = xr.open_dataset(input_path)

                logger.info(
                    "Custom NetCDF data loaded."
                )

            # CSV : données ponctuelles
            elif suffix == ".csv":
                climate_data = pd.read_csv(input_path)

                if "time" in climate_data.columns:
                    climate_data["time"] = pd.to_datetime(
                        climate_data["time"]
                    )

                logger.info(
                    "Custom CSV data loaded."
                )

            else:
                raise ValueError(
                    f"Unsupported input format: {suffix}. "
                    f"Supported formats are .nc, .nc4 and .csv."
                )

        # Sinon : données produites par le bloc climate

        else:
            climate_data = results.get("climate")
            print(f"climate_data from pipeline: {climate_data}")

            if climate_data is not None:
                logger.info(
                    "Using pipeline climate data for indicators."
                )
                
            else:
                # get output_dir from climate block if present
                climate_output_dir = climate_cfg.get("output_dir")
                print(f"climate_output_dir: {climate_output_dir}")
                # get netcdf files in the output_dir
                nc_files = list(Path(climate_output_dir).glob("*.nc"))
                print(f"nc_files: {nc_files}")  
                # read first netcdf file as xarray dataset
                climate_data = xr.open_dataset(nc_files[0])
                print(f"climate_data: {climate_data}")
                    
            

        # 2. VÉRIFICATION DES DONNÉES

        if climate_data is None:
            logger.error(
                "No climate data available for indicators."
            )

            results["indicators"] = {
                "data": {},
                "metadata": {
                    "input": (
                        str(input_file)
                        if input_file
                        else "pipeline"
                    ),
                    "n_indicators": 0,
                },
            }

        else:
            # 3. CONFIGURATION

            functions = indicators_cfg.get(
                "functions",
                {}
            )

            indicators_results = {}

            output_dir = Path(
                indicators_cfg.get(
                    "output_dir",
                    "data/indicators"
                )
            )

            output_dir.mkdir(
                parents=True,
                exist_ok=True
            )

            if not functions:
                logger.warning(
                    "No indicator functions specified."
                )

            # 4. CALCUL DES INDICATEURS

            for indicator_name, params in functions.items():

                # Copie des paramètres pour ne pas modifier config
                params = params.copy() if params else {}

                logger.info(
                    f"Computing indicator: "
                    f"{indicator_name} "
                    f"(params: {params})"
                )

                try:
                    # Récupération de la classe depuis le registre
                    indicator_class = get_indicator(
                        indicator_name
                    )

                    # Création de l'indicateur
                    indicator = indicator_class(
                        **params
                    )
                    
                    print(f"indicator {indicator_name} created with params {params}")

                    # Calcul
                    result = indicator.compute(
                        climate_data
                    )
                    print(f"result for {indicator_name} computed: {result}")

                    # Stockage en mémoire
                    indicators_results[
                        indicator_name
                    ] = result

                    # 5. SAUVEGARDE DU RÉSULTAT

                    try:
                        # Résultat spatial
                        if isinstance(
                            result,
                            (xr.Dataset, xr.DataArray)
                        ):
                            output_path = (
                                output_dir
                                / f"{indicator_name}.nc"
                            )

                            result.to_netcdf(
                                output_path
                            )

                            logger.info(
                                f"Indicator saved: "
                                f"{output_path}"
                            )

                        # Résultat ponctuel
                        elif isinstance(
                            result,
                            pd.DataFrame
                        ):
                            output_path = (
                                output_dir
                                / f"{indicator_name}.csv"
                            )

                            result.to_csv(
                                output_path,
                                index=False
                            )

                            logger.info(
                                f"Indicator saved: "
                                f"{output_path}"
                            )

                    except Exception as error:
                        logger.warning(
                            f"Could not save "
                            f"{indicator_name}: "
                            f"{error}"
                        )
                # get_indicator() lève ValueError
                # si le nom n'existe pas dans le registre
                except ValueError as error:
                    logger.warning(
                        f"{error} Indicator skipped."
                    )
                    
                except Exception as error:
                    logger.error(
                        f"Error computing "
                        f"{indicator_name}: "
                        f"{error}"
                    )

            # 6. RÉSULTATS DU PIPELINE

            results["indicators"] = {
                "data": indicators_results,
                "metadata": {
                    "input": (
                        str(input_file)
                        if input_file
                        else "pipeline"
                    ),
                    "n_indicators": len(
                        indicators_results
                    ),
                    "output_dir": str(
                        output_dir
                    ),
                },
            }

            logger.info(
                f"{len(indicators_results)} "
                f"indicator(s) computed."
            )
        

    if "metrics" in config:
            pass
        
    if "optimal_source" in config:
            pass

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