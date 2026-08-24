import requests
import rioxarray
import pandas as pd
import xarray as xr
from pathlib import Path
from zipfile import ZipFile
from io import BytesIO
from datetime import datetime, timedelta
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

from .base import ClimateSource
from agrometflow.utils import get_logger


class Arc2Downloader:
    BASE_URL = "https://ftp.cpc.ncep.noaa.gov/fews/fewsdata/africa/arc2/geotiff"

    def __init__(self, output_dir="data/arc2", log_file=None, verbose=False, max_workers=6):
        self.logger = get_logger("arc2", log_file, verbose)
        self.max_workers = max_workers
        self.output_dir = Path(output_dir)


    def _parse_date(self, date):
        return datetime.strptime(date, "%Y-%m-%d") if isinstance(date, str) else date

    def build_url(self, date):
        return f"{self.BASE_URL}/africa_arc.{date.strftime('%Y%m%d')}.tif.zip"

    def download_and_extract(self, date, tif_dir):
        url = self.build_url(date)
        zip_name = url.split("/")[-1]
        tif_name = zip_name.replace(".zip", "")
        tif_path = tif_dir / tif_name

        if tif_path.exists():
            return tif_path

        try:
            # Vérifier si le fichier existe (HEAD request)
            head_response = requests.head(url, timeout=10)
            if head_response.status_code == 404:
                self.logger.warning(f"Fichier non trouvé pour {date.strftime('%Y-%m-%d')}. Ignoré.")
                return None
            
            self.logger.debug(f"⬇ Downloading {url}")
            response = requests.get(url, stream=True, timeout=30)
            response.raise_for_status()
            with ZipFile(BytesIO(response.content)) as thezip:
                thezip.extractall(tif_dir)
            return tif_path
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                self.logger.warning(f"Fichier non trouvé pour {date.strftime('%Y-%m-%d')}. Ignoré.")
                return None
            else:
                self.logger.warning(f"Failed for {zip_name}: {e}")
                return None
        except Exception as e:
            self.logger.warning(f"Failed for {zip_name}: {e}")
            return None

    def convert_all_to_netcdf_per_year(self, files_by_year: dict, output_dir):
        for year, tif_files in files_by_year.items():
            output_nc = output_dir / f"arc2_{year}.nc"
            if output_nc.exists():
                self.logger.info(f"{output_nc} already exists. Skipping.")
                continue

            datasets = []
            for tif_file in sorted(tif_files):
                try:
                    date_str = tif_file.stem.split(".")[-1]
                    timestamp = datetime.strptime(date_str, "%Y%m%d")
                    ds = rioxarray.open_rasterio(tif_file)
                    ds = ds.squeeze("band", drop=True)
                    
                    # AJOUT : Découpage spatial (bbox)
                    if hasattr(self, 'bbox') and self.bbox is not None:
                        south, north, west, east = self.bbox
                        # Les coordonnées sont dans le système de la grille
                        # Pour ARC2, les dimensions sont 'x' et 'y'
                        # On doit adapter le bbox aux coordonnées de la grille
                        # ARC2 utilise un système de coordonnées avec x = longitude, y = latitude
                        # Vérifier l'ordre des coordonnées
                        if ds.x.values[0] > ds.x.values[-1]:
                            x_slice = slice(east, west)  # si les x sont décroissants
                        else:
                            x_slice = slice(west, east)  # si les x sont croissants
                        
                        if ds.y.values[0] > ds.y.values[-1]:
                            y_slice = slice(north, south)  # si les y sont décroissants
                        else:
                            y_slice = slice(south, north)  # si les y sont croissants
                        
                        ds = ds.sel(x=x_slice, y=y_slice)
                        self.logger.info(f"Découpage spatial appliqué : {self.bbox}")
                        self.logger.info(f"   Dimensions après découpage : {ds.dims}")
                    
                    ds = ds.expand_dims(time=[timestamp])
                    datasets.append(ds)
                except Exception as e:
                    self.logger.error(f"Failed to read {tif_file.name}: {e}")

            if datasets:
                try:
                    merged = xr.concat(datasets, dim="time")
                    merged.name = "PR"  # Standardisation en PR
                    merged.to_netcdf(output_nc)
                    self.logger.info(f"Yearly NetCDF saved: {output_nc}")
                except Exception as e:
                    self.logger.error(f"Merge failed for {year}: {e}")

    def download(self, start_date, end_date, output_dir=None, bbox=None, points=None, **kwargs):
        if output_dir is None:
            output_dir = self.output_dir
        else:
            self.output_dir = Path(output_dir)

        output_dir = Path(output_dir) / "PR"
        max_workers = kwargs.get("max_workers", 6)
        
        # Vérification des dates de disponibilité
        AVAILABLE_FROM = "1983-01-01"
        AVAILABLE_TO = "present"
        
        start = self._parse_date(start_date)
        if start < datetime.strptime(AVAILABLE_FROM, "%Y-%m-%d"):
            raise ValueError(
                f"ARC2 n'est pas disponible avant le {AVAILABLE_FROM}. "
                f"Vous avez demandé à partir du {start_date}."
            )
        
        # Stockage du bbox
        self.bbox = bbox
        if bbox is not None:
            self.logger.info(f"Découpage spatial appliqué : {bbox}")
        else:
            self.logger.info("Aucun découpage spatial : données globales")
        
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        
        tif_dir = output_dir / "tifs"
        tif_dir.mkdir(parents=True, exist_ok=True)
        
        start = self._parse_date(start_date)
        end = self._parse_date(end_date)
        all_dates = list(self._daterange(start, end))
        
        self.logger.info(f"Downloading {len(all_dates)} daily ARC2 files with {self.max_workers} workers...")
        
        files_by_year = defaultdict(list)
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {executor.submit(self.download_and_extract, date, tif_dir): date for date in all_dates}
            
            for future in as_completed(futures):
                date = futures[future]
                tif_path = future.result()
                if tif_path:
                    files_by_year[date.year].append(tif_path)
        
        self.convert_all_to_netcdf_per_year(files_by_year, output_dir)

        # Si des points sont demandés → CSV direct
        if points is not None:
            self.logger.info("Extraction des points en CSV...")
            nc_files = list(Path(output_dir).glob("*.nc"))
            if not nc_files:
                nc_files = list(Path(output_dir).glob("PR/*.nc"))
            
            if nc_files:
                ds = xr.open_dataset(nc_files[0])
                return self.to_csv(points=points)
            else:
                self.logger.error("NetCDF non trouvé")
                return None

    def _daterange(self, start, end):
        while start <= end:
            yield start
            start += timedelta(days=1)

    def to_csv(self, output_csv=None, points=None):
        """
        Extrait les données en CSV pour des points spécifiques.

        Parameters
        ----------
        output_csv : str, optional
            Chemin du fichier CSV de sortie.
        points : list of dict
            Liste de points avec lat/lon, ex: [{"lat": 14.7, "lon": -17.4}]

        Returns
        -------
        pandas.DataFrame or None
        """
        if points is None:
            self.logger.error(" Le CSV nécessite une liste de points (lat/lon).")
            return None
        
        # Charger le fichier NetCDF
        nc_files = list(self.output_dir.glob("PR/*.nc"))
        if not nc_files:
            self.logger.error("Aucun fichier NetCDF trouvé.")
            return None
        
        ds = xr.open_dataset(nc_files[0])
        
        # Vérifier le nom de la variable (PR ou precip)
        var_name = 'PR' if 'PR' in ds.data_vars else 'precip'
        
        records = []
        for point in points:
            lat = point.get("lat")
            lon = point.get("lon")
            if lat is None or lon is None:
                self.logger.warning("Point ignoré : lat/lon manquant")
                continue
            
            # Sélectionner le point le plus proche
            da = ds[var_name].sel(y=lat, x=lon, method='nearest')
            df_point = da.to_dataframe().reset_index()
            df_point["point"] = f"({lat}, {lon})"
            records.append(df_point)
        
        if not records:
            self.logger.error("Aucun point valide.")
            return None
        
        df = pd.concat(records, ignore_index=True)
        
        if output_csv is None:
            output_csv = self.output_dir / "arc2_points.csv"
        else:
            output_csv = Path(output_csv)
        
        df.to_csv(output_csv, index=False)
        self.logger.info(f"CSV sauvegardé : {output_csv}")
        return df
