"""
Fonctions utilitaires pour le traitement des indicateurs agroclimatiques.
"""

from pathlib import Path

import pandas as pd
import xarray as xr

from agrometflow.metadata import get_native_variable_name

from .registry import get_indicator

# STANDARDISATION DES VARIABLES

def standardize_variables(
    data,
    variables,
    product,
    source,
    logger=None
):
    """
    Renomme les variables natives avec les noms standards AgroMetFlow.

    Exemple :
        PERSIANN : precip -> PR
        POWER    : PRECTOTCORR -> PR

    Les correspondances sont récupérées depuis metadata.py.
    """

    if not variables or not product or not source:
        return data

    rename_map = {}

    for standard_name in variables:

        native_name = get_native_variable_name(
            variable=standard_name,
            product=product,
            source=source
        )

        # Cas xarray
        if isinstance(data, (xr.Dataset, xr.DataArray)):

            if (
                native_name in data
                and native_name != standard_name
            ):
                rename_map[native_name] = standard_name

        # Cas pandas
        elif isinstance(data, pd.DataFrame):

            if (
                native_name in data.columns
                and native_name != standard_name
            ):
                rename_map[native_name] = standard_name

    if rename_map:
        data = data.rename(rename_map)

        if logger:
            logger.info(
                f"Standardized variables: {rename_map}"
            )

    return data


# CHARGEMENT DES DONNÉES

def load_indicator_input(
    input_dir,
    variables=None,
    product=None,
    source=None,
    logger=None
):
    """
    Charge les données d'entrée pour le calcul des indicateurs.

    Le dossier peut contenir :
        - un ou plusieurs fichiers CSV
        - un ou plusieurs fichiers NetCDF

    La recherche est récursive dans les sous-dossiers.

    Priorité :
        1. CSV
        2. NetCDF

    Les variables sont ensuite standardisées en utilisant metadata.py.

    Parameters
    ----------
    input_dir : str or Path
        Dossier contenant les données climatiques.

    variables : list, optional
        Noms standards AgroMetFlow des variables.
        Exemple : ["PR", "T2M"]

    product : str, optional
        Produit climatique.
        Exemple : "persiann"

    source : str, optional
        Source climatique.
        Exemple : "chrs_ftp"

    logger : logging.Logger, optional
        Logger du pipeline.

    Returns
    -------
    pandas.DataFrame or xarray.Dataset
        Données climatiques chargées et standardisées.
    """

    input_dir = Path(input_dir)

    # Vérifier que le chemin existe
    if not input_dir.exists():
        raise FileNotFoundError(
            f"Input directory not found: {input_dir}"
        )

    # Vérifier qu'il s'agit bien d'un dossier
    if not input_dir.is_dir():
        raise ValueError(
            f"Indicator input must be a directory: {input_dir}"
        )

    # 1. RECHERCHE DES CSV

    csv_files = sorted(
        list(input_dir.glob("*.csv"))
    )

    if not csv_files:
        csv_files = sorted(
            [
                file
                for file in input_dir.rglob("*.csv")
                if not any(
                    part.lower() == "tmp"
                    for part in file.parts
                )
            ]
        )

    if csv_files:

        if logger:
            logger.info(
                f"Loading {len(csv_files)} CSV file(s) "
                f"from {input_dir}"
            )

        dataframes = []

        for file in csv_files:
            df = pd.read_csv(file)

            if "time" in df.columns:
                df["time"] = pd.to_datetime(df["time"])

            dataframes.append(df)

        data = pd.concat(
            dataframes,
            ignore_index=True
        )

        data = standardize_variables(
            data=data,
            variables=variables,
            product=product,
            source=source,
            logger=logger
        )

        return data


    # 2. RECHERCHE DES NETCDF

    # Chercher d'abord uniquement les fichiers directement
    # présents dans le dossier principal.
    nc_files = sorted(
        list(input_dir.glob("*.nc"))
        + list(input_dir.glob("*.nc4"))
    )

    # Si aucun NetCDF n'est directement dans le dossier,
    # chercher dans les sous-dossiers en excluant les dossiers temporaires.
    if not nc_files:
        nc_files = sorted(
            [
                file
                for file in (
                    list(input_dir.rglob("*.nc"))
                    + list(input_dir.rglob("*.nc4"))
                )
                if not any(
                    part.lower() == "tmp"
                    for part in file.parts
                )
            ]
        )

    if nc_files:

        if logger:
            logger.info(
                f"Loading {len(nc_files)} NetCDF file(s) "
                f"from {input_dir}"
            )

        if len(nc_files) == 1:
            data = xr.open_dataset(
                nc_files[0]
            )

        else:
            data = xr.open_mfdataset(
                nc_files,
                combine="by_coords"
            )

        data = standardize_variables(
            data=data,
            variables=variables,
            product=product,
            source=source,
            logger=logger
        )

        return data


    # Aucun fichier exploitable
    raise FileNotFoundError(
        f"No CSV or NetCDF file found in: {input_dir}"
    )

# SAUVEGARDE DES RÉSULTATS

def save_indicator_result(
    result,
    indicator_name,
    output_dir,
    logger=None
):
    """
    Sauvegarde le résultat d'un indicateur.

    - xarray.Dataset / DataArray -> NetCDF
    - pandas.DataFrame -> CSV
    """

    output_dir = Path(output_dir)

    output_dir.mkdir(
        parents=True,
        exist_ok=True
    )

    # XARRAY -> NETCDF
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

    # PANDAS -> CSV
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

    else:
        raise TypeError(
            f"Unsupported indicator result type: "
            f"{type(result).__name__}"
        )

    if logger:
        logger.info(
            f"Indicator saved: {output_path}"
        )

# CALCUL DES INDICATEURS


def compute_indicators(
    climate_data,
    functions,
    output_dir,
    logger=None
):
    """
    Calcule les indicateurs demandés dans la configuration.
    """

    results = {}

    for indicator_name, params in functions.items():

        # Copie pour ne pas modifier la configuration originale
        params = (
            params.copy()
            if params
            else {}
        )

        if logger:
            logger.info(
                f"Computing indicator: "
                f"{indicator_name} "
                f"(params: {params})"
            )

        try:
            # Récupération de la classe depuis registry.py
            indicator_class = get_indicator(
                indicator_name
            )

            # Création de l'objet indicateur
            indicator = indicator_class(
                **params
            )

            # Calcul
            result = indicator.compute(
                climate_data
            )

            # Stockage en mémoire
            results[
                indicator_name
            ] = result

            # Sauvegarde sur disque
            save_indicator_result(
                result=result,
                indicator_name=indicator_name,
                output_dir=output_dir,
                logger=logger
            )

        except ValueError as error:

            if logger:
                logger.warning(
                    f"{error} Indicator skipped."
                )

        except Exception as error:

            if logger:
                logger.error(
                    f"Error computing "
                    f"{indicator_name}: "
                    f"{error}"
                )

    return results

# TRAITEMENT GLOBAL DES INDICATEURS


def process_indicators(
    indicators_cfg,
    climate_cfg=None,
    logger=None
):
    """
    Traite complètement le bloc 'indicators'.

    Priorité pour le dossier d'entrée :

        1. indicators.input
        2. climate.output_dir

    La fonction :
        1. détermine le dossier d'entrée ;
        2. récupère les informations de la source climatique ;
        3. charge et standardise les données ;
        4. calcule les indicateurs ;
        5. sauvegarde les résultats ;
        6. retourne les résultats et métadonnées.
    """

    # 1. DOSSIER D'ENTRÉE

    input_dir = indicators_cfg.get(
        "input"
    )

    # Si aucun input explicite :
    # utiliser climate.output_dir
    if not input_dir and climate_cfg:
        input_dir = climate_cfg.get(
            "output_dir"
        )

    if not input_dir:
        raise ValueError(
            "No input directory available for indicators."
        )

    # 2. DOSSIER DE SORTIE

    output_dir = indicators_cfg.get(
        "output_dir",
        "data/indicators"
    )

    # 3. INDICATEURS DEMANDÉS

    functions = indicators_cfg.get(
        "functions",
        {}
    )

    if not functions:

        if logger:
            logger.warning(
                "No indicator functions specified."
            )

        return {
            "data": {},
            "metadata": {
                "input": str(input_dir),
                "n_indicators": 0,
                "output_dir": str(output_dir),
            },
        }

    # 4. INFORMATIONS CLIMATIQUES

    source = indicators_cfg.get("source")
    product = indicators_cfg.get("product")
    variables = indicators_cfg.get("variables", [])

    # Si ces informations ne sont pas fournies
    # dans indicators, utiliser celles du bloc climate.
    if climate_cfg:

        if not source:
            source = climate_cfg.get("source")

        if not product:
            product = climate_cfg.get("product")

        if not variables:

            for variable in climate_cfg.get(
                "variables",
                []
            ):

                if isinstance(variable, dict):
                    variables.extend(
                        variable.keys()
                    )

                else:
                    variables.append(
                        variable
                    )

    # 5. CHARGEMENT ET STANDARDISATION

    climate_data = load_indicator_input(
        input_dir=input_dir,
        variables=variables,
        product=product,
        source=source,
        logger=logger
    )

    # 6. CALCUL

    indicators_results = compute_indicators(
        climate_data=climate_data,
        functions=functions,
        output_dir=output_dir,
        logger=logger
    )

    # 7. RETOUR DES RÉSULTATS


    return {
        "data": indicators_results,
        "metadata": {
            "input": str(input_dir),
            "n_indicators": len(
                indicators_results
            ),
            "output_dir": str(
                output_dir
            ),
        },
    }