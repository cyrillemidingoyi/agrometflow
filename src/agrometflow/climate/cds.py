from .base import ClimateSource
import cdsapi
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from agrometflow.utils import get_logger
import zipfile
import xarray as xr
import os
import tempfile
import numpy as np
import shutil
from pathlib import Path

# Disable tqdm completely to avoid issues in Binder/remote environments
os.environ["TQDM_DISABLE"] = "1"
os.environ["TQDM_NOTEBOOK_DISABLE"] = "1"

def _is_notebook_environment():
    """Check if running in a Jupyter notebook or Binder environment."""
    try:
        from IPython import get_ipython
        shell = get_ipython().__class__.__name__
        if shell == 'ZMQInteractiveShell':
            return True  # Jupyter notebook or qtconsole
        elif shell == 'TerminalInteractiveShell':
            return False  # Terminal running IPython
        else:
            return False
    except (NameError, AttributeError):
        return False

class CDSDownloader(ClimateSource):
    def __init__(self, product=None, log_file=None, verbose=False):
        self.logger = get_logger(__name__, log_file=log_file, verbose=verbose)
        self.data = None
        self.output_dir = None

    def download(self, **kwargs):

        try:
            start_date = kwargs["start_date"]
            end_date = kwargs["end_date"]
            variables = kwargs["variables"]
            output_dir = kwargs["output_dir"]

            bbox = kwargs.get("bbox")
            product = kwargs.get("product", "era5")
            points = kwargs.get("points")

        except KeyError as e:
            raise ValueError(f"Missing required argument: {e}")

        self.output_dir = Path(output_dir)

        self.output_dir.mkdir(
            parents=True,
            exist_ok=True
        )


        # Dataset
         

        dataset = kwargs.get(
            "dataset",
            "sis-agrometeorological-indicators"
        )

        if dataset == "sis-agrometeorological-indicators":

            self.logger.info(
                "Using AgERA5 dataset"
            )

        else:

            self.logger.info(
                "Using ERA5 dataset"
            )

        self.logger.info(
            f"Downloading {variables} from {start_date} to {end_date}"
        )

        self.logger.info(
            f"BBOX: {bbox}, Dataset: {dataset}"
        )


        # CDS client
   

        cds_url = kwargs.get(
            "url",
            os.environ.get(
                "CDS_URL",
                "https://cds.climate.copernicus.eu/api"
            )
        )

        cds_key = kwargs.get("key", os.environ.get("CDS_KEY"))
        cds_url = kwargs.get("url", os.environ.get("CDS_URL", "https://cds.climate.copernicus.eu/api"))

        if not cds_key:
            raise ValueError("❌ CDS_KEY manquant")

        self.client = cdsapi.Client(
            cds_url,
            cds_key,
            quiet=True
        )

       
        # Nombre de workers
     

        is_notebook = _is_notebook_environment()

        max_workers = (
            1
            if is_notebook
            else kwargs.get(
                "max_workers",
                4
            )
        )

        self.logger.info(
            f"Max workers: {max_workers}"
        )

      
        # Construction des requêtes
      

        if dataset == "sis-agrometeorological-indicators":

            requests = build_requests(
                variables=variables,
                start_date=start_date,
                end_date=end_date,
                output_dir=output_dir,
                bbox=bbox,
                dataset=dataset,
                logger=self.logger
            )

        else:

            years = list(
                range(
                    pd.to_datetime(start_date).year,
                    pd.to_datetime(end_date).year + 1
                )
            )

            requests = build_requests(
                variables=variables,
                years=years,
                output_dir=output_dir,
                bbox=bbox,
                dataset=dataset,
                logger=self.logger
            )

        self.logger.info(
            f"Requests: {len(requests)}"
        )

        # Tous les fichiers existent déjà
      

        if len(requests) == 0:

            self.logger.info(
                "📂 Tous les NetCDF existent déjà."
            )

            if points is None:
                return None

            nc_files = sorted(
                self.output_dir.glob("**/*.nc")
            )

            if not nc_files:

                self.logger.error(
                    "❌ Aucun fichier NetCDF trouvé."
                )

                return None

            with xr.open_dataset(nc_files[0]) as ds:

                target_var = list(ds.data_vars)[0]

                result = _extract_points(
                    ds,
                    points,
                    target_var,
                    self.logger,
                    start_date,
                    end_date
                )

            self.data = result

            return result

        
        # Téléchargement
        

        result = None

        for req in requests:

            result = fetch_and_merge(
                req,
                self.client,
                self.logger,
                dataset,
                points=points,
                start_date=start_date,
                end_date=end_date
            )

       
        # Résultat
        

        if points is not None and result is not None:

            self.data = result

            return result

        return None

    def to_csv(self, output_csv=None, points=None):
        """Extrait les données en CSV pour des points spécifiques."""
        if points is None:
            self.logger.error("❌ Le CSV nécessite une liste de points.")
            return None

        nc_files = list(self.output_dir.glob("**/*.nc"))
        if not nc_files:
            self.logger.error("❌ Aucun fichier NetCDF trouvé.")
            return None

        ds = xr.open_dataset(nc_files[0])
        
        # Renommer si nécessaire
        var_name = list(ds.data_vars)[0] if len(ds.data_vars) == 1 else None
        if var_name and var_name != 'PR':
            ds = ds.rename({var_name: 'PR'})
            self.logger.info(f"📝 Variable renommée : {var_name} → PR")

        records = []
        for point in points:
            lat = point.get("lat") if isinstance(point, dict) else point[1]
            lon = point.get("lon") if isinstance(point, dict) else point[0]
            
            da = ds.PR.sel(lat=lat, lon=lon, method='nearest')
            df_point = da.to_dataframe().reset_index()
            df_point["point"] = f"({lat}, {lon})"
            records.append(df_point)

        if not records:
            self.logger.error("❌ Aucun point valide.")
            return None

        df = pd.concat(records, ignore_index=True)
        df["time"] = pd.to_datetime(df["time"])
        
        if output_csv is None:
            output_csv = self.output_dir / "era5_points.csv"
        
        df.to_csv(output_csv, index=False)
        self.logger.info(f"💾 CSV sauvegardé : {output_csv}")
        return df

    def extract(self, variables=None, start_date=None, end_date=None, as_long=False, **kwargs):
        if self.data is None:
            raise ValueError("No point data available. Run download(points=...) first.")

        df = self.data.copy()
        if "time" in df.columns:
            df["time"] = pd.to_datetime(df["time"])
            if start_date:
                df = df[df["time"] >= pd.to_datetime(start_date)]
            if end_date:
                df = df[df["time"] <= pd.to_datetime(end_date)]

        if variables:
            keep = [c for c in ["time", "lon", "lat"] if c in df.columns]
            keep += [v for v in variables if v in df.columns]
            df = df[keep]

        if as_long:
            id_vars = [c for c in ["time", "lon", "lat"] if c in df.columns]
            value_vars = [c for c in df.columns if c not in id_vars]
            df = df.melt(
                id_vars=id_vars,
                value_vars=value_vars,
                var_name="variable",
                value_name="value",
            )

        return df

def fetch_and_merge(
    req,
    client,
    logger,
    dataset,
    points=None,
    start_date=None,
    end_date=None
):

    request, zip_path, output_file, target_var = req

    logger.info(f"⬇️ Downloading to {zip_path}")

    # Téléchargement
   
    try:
        client.retrieve(dataset, request).download(zip_path)
    except Exception as e:
        logger.error(f"❌ Échec du téléchargement : {e}")
        return None

     
    # Détection ZIP
    
    with open(zip_path, "rb") as f:
        is_zip = (f.read(4) == b"PK\x03\x04")

    ds = None

  
    # Cas ERA5 : NetCDF direct
 
    if not is_zip:

        logger.info("📄 NetCDF direct détecté")

        try:
            ds = xr.open_dataset(
                zip_path,
                chunks={
                    "time": 30,
                    "latitude": 100,
                    "longitude": 100
                },
                engine="netcdf4"
            )
        except Exception as e:
            logger.error(f"❌ Erreur d'ouverture du NetCDF : {e}")
            return None

        ds = _process_dataset(ds, target_var, logger)

 
    # Cas AgERA5 : ZIP contenant plusieurs NetCDF
   
    else:

        with tempfile.TemporaryDirectory() as tmp_dir:

            tmp_dir = Path(tmp_dir)

            logger.info(f"📦 Extraction vers {tmp_dir}")

            try:
                with zipfile.ZipFile(zip_path, "r") as z:
                    z.extractall(tmp_dir)
            except Exception as e:
                logger.error(f"❌ Erreur d'extraction du ZIP : {e}")
                return None

            nc_files = sorted(tmp_dir.glob("*.nc"))

            if len(nc_files) == 0:
                logger.error("❌ Aucun fichier NetCDF trouvé dans le ZIP")
                return None

            logger.info(f"📂 {len(nc_files)} fichiers NetCDF trouvés")

            # ✅ Version robuste (fichier par fichier)
            try:
                datasets = []
                for f in nc_files:
                    with xr.open_dataset(f, engine="netcdf4") as d:
                        datasets.append(d.load())
                ds = xr.concat(datasets, dim="time")
                ds = ds.sortby("time")
            except Exception as e:
                logger.error(f"❌ Erreur de fusion des NetCDF : {e}")
                return None

            ds = _process_dataset(ds, target_var, logger)

            # ✅ Le Dataset est maintenant chargé en mémoire
            # → Les fichiers temporaires seront supprimés à la sortie du bloc with

   
    # Vérification de la variable
    
    if target_var not in ds.data_vars:
        logger.error(f"❌ Variable '{target_var}' non trouvée dans le Dataset")
        return None

    # Sauvegarde NetCDF final
 
    try:
        encoding = {
            target_var: {
                "zlib": True,
                "complevel": 4,
                "chunksizes": (
                    1,
                    min(100, ds.sizes["lat"]),
                    min(100, ds.sizes["lon"])
                )
            }
        }

        ds.to_netcdf(output_file, encoding=encoding)
        logger.info(f"💾 Saved : {output_file}")
    except Exception as e:
        logger.error(f"❌ Erreur de sauvegarde : {e}")
        return None

    ds.close()

 
    # Nettoyage ZIP
    
    try:
        os.remove(zip_path)
        logger.info("🗑️ ZIP supprimé")
    except Exception:
        pass

    # =====================================================
    # Extraction des points
    # =====================================================
    if points is not None:

        try:
            with xr.open_dataset(
                output_file,
                chunks={"time": 30, "lat": 100, "lon": 100}
            ) as ds_out:

                if start_date and end_date and "time" in ds_out.coords:

                    start = pd.to_datetime(start_date)
                    end = pd.to_datetime(end_date) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
                    ds_out = ds_out.sel(time=slice(start, end))
                    logger.info(f"📅 Filtrage temporel : {start} → {end}")

                return _extract_points(
                    ds_out,
                    points,
                    target_var,
                    logger
                )
        except Exception as e:
            logger.error(f"❌ Erreur d'extraction des points : {e}")
            return None

    return None

def _extract_points(ds, points, target_var, logger, start_date=None, end_date=None):
    """Extrait les points d'un Dataset avec filtrage temporel."""
    records = []
    
    # ✅ Filtrage temporel
    if start_date and end_date:
        start = pd.to_datetime(start_date)
        end = pd.to_datetime(end_date)
        if 'time' in ds.coords:
            ds = ds.sel(time=slice(start, end))
            logger.info(f"📅 Filtrage temporel : {start} → {end}")
        elif 'valid_time' in ds.coords:
            ds = ds.sel(valid_time=slice(start, end))
            logger.info(f"📅 Filtrage temporel : {start} → {end}")
    
    for p in points:
        if isinstance(p, dict):
            lat = p["lat"]
            lon = p["lon"]
        else:
            lon, lat = p

        # Conversion de la longitude
        if 'lon' in ds.coords and ds.lon.min() >= 0:
            lon_360 = lon % 360
        else:
            lon_360 = lon

        logger.info(f"📍 Extraction pour ({lat}, {lon}) → lon_360={lon_360}")

        da = ds[target_var].sel(lat=lat, lon=lon_360, method="nearest")
        df = da.compute().to_dataframe().reset_index()
        df["point"] = f"({lat}, {lon})"
        records.append(df)

    if records:
        return pd.concat(records, ignore_index=True)
    return None

def build_requests(
    variables,
    output_dir,
    bbox=None,
    dataset="sis-agrometeorological-indicators",
    logger=None,
    start_date=None,
    end_date=None,
    years=None
):
    """
    Construit les requêtes CDS pour AgERA5 en ne demandant
    que les dates réellement nécessaires.
    """

    if logger is None:
        import logging
        logger = logging.getLogger(__name__)

    requests = []

    if start_date is None or end_date is None:
        raise ValueError("AgERA5 nécessite start_date et end_date")

    # Dates demandées
    dates = pd.date_range(start=start_date, end=end_date, freq="D")
    print(f"variables = {variables}, dates = {dates}")

    for var in variables:

        #var_name = var[0]["variable"]
        target_var = list(var.keys())[0]
        var_name = var[target_var]["variable"]
        print(f"target_var = {target_var}, var_name = {var_name}")

        var_dir = Path(output_dir) / target_var
        var_dir.mkdir(parents=True, exist_ok=True)

        year_label = f"{dates[0]:%Y}_{dates[-1]:%Y}"

        zip_path = var_dir / f"agera5_{target_var}_{year_label}.zip"
        nc_path = var_dir / f"agera5_{target_var}_{year_label}.nc"

        if nc_path.exists():
            logger.info(f"⏩ Fichier existant ignoré : {nc_path}")
            continue

        request = {
            "variable": var_name,
            "year": sorted(dates.strftime("%Y").unique().tolist()),
            "month": sorted(dates.strftime("%m").unique().tolist()),
            "day": sorted(dates.strftime("%d").unique().tolist()),
            "version": "2_0"
        }

        if bbox:
            west, south, east, north = bbox
            request["area"] = [north, west, south, east]

        logger.info(f"🗓️ REQUEST = {request}")

        requests.append(
            (request, str(zip_path), str(nc_path), target_var)
        )

    return requests

def _process_dataset(ds, target_var, logger):
    """
    Standardise ERA5 / AgERA5 Dataset.

    - Harmonise les coordonnées
    - Supprime les variables auxiliaires
    - Détecte la variable météorologique principale
    - Renomme vers target_var
    - Corrige l'ordre des dimensions
    - Ajoute les métadonnées CF
    """

    # =====================================================
    # 1. Normalisation des coordonnées
    # =====================================================

    rename_coords = {}

    mapping = {
        "latitude": "lat",
        "longitude": "lon",
        "valid_time": "time"
    }

    for old, new in mapping.items():

        if old in ds.coords:
            rename_coords[old] = new


    if rename_coords:

        ds = ds.rename(rename_coords)

        logger.info(
            f"📝 Coordonnées renommées : {rename_coords}"
        )


    # =====================================================
    # 2. Suppression variables auxiliaires
    # =====================================================

    auxiliary = [
        "crs",
        "spatial_ref"
    ]


    for var in auxiliary:

        if var in ds.data_vars:

            ds = ds.drop_vars(var)

            logger.info(
                f"🧹 Variable auxiliaire supprimée : {var}"
            )


    # =====================================================
    # 3. Recherche variable météo principale
    # =====================================================

    if len(ds.data_vars) == 0:

        raise ValueError(
            "❌ Aucune variable météorologique trouvée"
        )


    variables = list(ds.data_vars)


    if len(variables) == 1:

        source_var = variables[0]


    else:

        candidates = []

        for var in variables:

            dims = ds[var].dims

            if (
                "time" in dims
                and "lat" in dims
                and "lon" in dims
            ):

                candidates.append(var)


        if candidates:

            source_var = candidates[0]

        else:

            source_var = variables[0]


        logger.warning(
            f"⚠️ Plusieurs variables détectées {variables}. "
            f"Sélection : {source_var}"
        )



    # =====================================================
    # 4. Renommage variable
    # =====================================================

    if source_var != target_var:

        ds = ds.rename(
            {
                source_var: target_var
            }
        )


        logger.info(
            f"📝 Variable renommée : {source_var} → {target_var}"
        )


    # =====================================================
    # 5. Ordre dimensions standard
    # =====================================================

    if target_var in ds:

        dims = ds[target_var].dims


        ordered_dims = [
            d for d in
            ["time", "lat", "lon"]
            if d in dims
        ]


        if ordered_dims:

            ds[target_var] = (
                ds[target_var]
                .transpose(*ordered_dims)
            )



    # =====================================================
    # 6. Métadonnées coordonnées CF
    # =====================================================

    if "lat" in ds.coords:

        ds.lat.attrs.update(
            {
                "standard_name":
                    "latitude",

                "units":
                    "degrees_north"
            }
        )


    if "lon" in ds.coords:

        ds.lon.attrs.update(
            {
                "standard_name":
                    "longitude",

                "units":
                    "degrees_east"
            }
        )


    if "time" in ds.coords:

        ds.time.attrs.update(
            {
                "standard_name":
                    "time"
            }
        )



    # =====================================================
    # 7. Metadata dataset
    # =====================================================

    temporal_resolution = "unknown"


    if "time" in ds.coords:

        if len(ds.time) > 1:

            delta = (
                ds.time[1]
                -
                ds.time[0]
            )


            if pd.Timedelta(delta.values) == pd.Timedelta(days=1):

                temporal_resolution = "daily"

            else:

                temporal_resolution = "sub-daily"



    ds.attrs.update(
        {
            "product":
                "AgERA5",

            "source":
                "CDS Copernicus",

            "temporal_resolution":
                temporal_resolution,

            "variable":
                target_var,

            "description":
                f"Daily meteorological variable {target_var}"
        }
    )


    logger.info(
        f"✅ Dataset standardisé : {target_var}"
    )


    return ds