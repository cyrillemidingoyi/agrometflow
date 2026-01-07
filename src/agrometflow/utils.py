import logging
from pathlib import Path
import xarray as xr
import pandas as pd
from agrometflow.metadata import metadata
import re


def get_logger(name="agrometflow", log_file=None, verbose=False):
    logger = logging.getLogger(name)

    if getattr(logger, "_configured", False):
        return logger

    if not logger.handlers:
        logger.setLevel(logging.DEBUG if verbose else logging.INFO)

        formatter = logging.Formatter("[%(levelname)s] %(asctime)s — %(message)s", "%Y-%m-%d %H:%M:%S")

        # Console handler
        ch = logging.StreamHandler()
        ch.setFormatter(formatter)
        logger.addHandler(ch)

        # Optional file handler
        if log_file:
            Path(log_file).parent.mkdir(parents=True, exist_ok=True)
            fh = logging.FileHandler(log_file)
            fh.setFormatter(formatter)
            logger.addHandler(fh)
    
    logger.propagate = False

    # Marque comme configuré
    logger._configured = True

    return logger

def guess_variable_type(variable):
    """
    Devine le type (climate, soil...) auquel appartient une variable.
    """
    for var_type in metadata.keys():
        if variable in metadata[var_type]:
            return var_type
    return None

def resolve_variables(source, product, variables, logger=None):
    resolved = []
    var_type = guess_variable_type(variables[0])
    print(f"var_type: {var_type}", flush=True)
    if var_type is None:
        msg = f"Unknown variable category for '{variables[0]}'"
        if logger:
            logger.error(msg)
        raise ValueError(msg)

    for var in variables:
        if product not in metadata[var_type][var]["products"] or source not in metadata[var_type][var]["products"][product]["sources"]:
            msg = f"Variable '{var}' not available for source '{source}' and product '{product}'"
            if logger:
                logger.error(msg)
            raise ValueError(msg)

        resolved.append([metadata[var_type][var]["products"][product]["sources"][source]["name"], var])
    return resolved

def split_yearly(ncfile, output_dir=None):


    # Load your full dataset
    ds = xr.open_dataset(ncfile)

    # Make sure the time coordinate is of datetime type
    ds['time'] = pd.to_datetime(ds['time'])

    # Output folder
    output_dir.mkdir(exist_ok=True)

    # Group by year and save each year as a separate NetCDF
    for year, ds_year in ds.groupby('time.year'):
        out_file = output_dir / f"data_{year}.nc"
        ds_year.to_netcdf(out_file)
        print(f"✅ Saved {out_file}")


from pathlib import Path

def write_cdsapirc_from_config(cdsapi_config, logger=None):
    """
    Génère le fichier .cdsapirc à partir du bloc de config.

    Parameters
    ----------
    cdsapi_config : dict
        Doit contenir : url, key, uid (et optionnellement verify)
    target_path : str or None
        Chemin vers le fichier .cdsapirc (par défaut : ~/.cdsapirc)
    """
    if not {"url", "key", "verify"} <= set(cdsapi_config.keys()):
        raise ValueError("cdsapi config must contain 'url', 'key', and 'verify'")

    content = f"""url: {cdsapi_config['url']}
key: {cdsapi_config['uid']}:{cdsapi_config['key']}
verify: {cdsapi_config.get('verify', 1)}
"""

    path = Path(Path.home() / ".cdsapirc")
    if not path.exists():
        path.write_text(content)
        logger.info(f"Created {path} with CDS API credentials.")
    else:
        logger.info(f"File {path} already exists. Not overwriting.")
    return path


import xarray as xr
import geopandas as gpd
import rioxarray
from pathlib import Path

def clip_with_shapefile(nc_path, shp_path, ds=None, output_path=None):
    # Charger NetCDF (il faut que lon/lat soient des coords)
    if ds is None: ds = xr.open_dataset(nc_path)
    # Charger shapefile
    gdf = gpd.read_file(shp_path)
    # S'assurer que lon/lat sont géographiques
    ds = ds.rio.write_crs("EPSG:4326", inplace=True)
    # Reprojeter shapefile vers même CRS que NetCDF
    gdf = gdf.to_crs(ds.rio.crs)
    # Clipping spatial
    ds_clipped = ds.rio.clip(gdf.geometry, gdf.crs, drop=True)
    # Sauvegarde
    if output_path:
        ds_clipped.to_netcdf(output_path)
        return Path(output_path)
    else:
        return ds_clipped

def clipwithbbox(nc_path, lat_min, lat_max, lon_min, lon_max, ds=None, output_path=None):
    if ds is None: ds = xr.open_dataset(nc_path)
    ds_clipped = ds.sel(
        lat=slice(lat_min, lat_max),
        lon=slice(lon_min, lon_max)
        ) 
    if output_path:
        ds_clipped.to_netcdf(output_path)
        return Path(output_path)
    else:
        return ds_clipped  
    

import xarray as xr
import numpy as np
def extract_points_from_tuples(ds: xr.Dataset, points):
    """
    points: list[(lon, lat)]
    Output: Dataset dims ('time', 'point'), avec coords req_lon/req_lat
    """
    lons = np.array([p[0] for p in points], dtype=float)
    lats = np.array([p[1] for p in points], dtype=float)

    lon_da = xr.DataArray(lons, dims="point")
    lat_da = xr.DataArray(lats, dims="point")

    out = ds.sel(lon=lon_da, lat=lat_da, method="nearest")

    # garder les coords demandées (celles de l'utilisateur)
    out = out.assign_coords(lon=("point", lons), lat=("point", lats))
    return out


def dataset_points_to_dataframe(ds_pts: xr.Dataset):
    """
    Retourne un DF avec colonnes: time, point, lon, lat, + variables (en colonnes)
    """
    # on force un dataset "plat"
    df = ds_pts.to_dataframe().reset_index()

    # mettre lon/lat demandés dans des colonnes lon/lat
    # (df a 'req_lon'/'req_lat' répétés par time)
    if "req_lon" in df.columns:
        df = df.rename(columns={"req_lon": "lon"})
    if "req_lat" in df.columns:
        df = df.rename(columns={"req_lat": "lat"})

    # garder uniquement ce qui t'intéresse
    # time, lon, lat, + variables
    keep = []
    if "time" in df.columns:
        keep.append("time")
    keep += [c for c in ["lon", "lat"] if c in df.columns]

    # variables = toutes les colonnes numériques qui ne sont pas index/coords classiques
    drop_like = {"point", "lat", "lon", "time", "bnds", "bounds"}
    var_cols = [c for c in df.columns if c not in drop_like and not c.endswith("_bnds")]
    keep += var_cols

    return df[keep]