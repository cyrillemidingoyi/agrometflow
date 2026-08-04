import zipfile
import requests
import xarray as xr
import pandas as pd
import numpy as np
import re
from datetime import datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

from agrometflow.utils import get_logger

class TamsatDownloader:
    BASE_URL = "http://gws-access.jasmin.ac.uk/public/tamsat/rfe/data_zipped/v3.1/daily"

    # Dates de disponibilité
    AVAILABLE_FROM = "1983-01-01"
    AVAILABLE_TO = "present"

    def __init__(self, product="tamsat", output_dir="data/tamsat", log_file=None, verbose=False, max_workers=4):
        self.product = product
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.logger = get_logger("tamsat", log_file, verbose)
        self.max_workers = max_workers
        self.bbox = None

    def build_url(self, year):
        return f"{self.BASE_URL}/TAMSATv3.1_rfe_daily_{year}.zip"

    def process_year(self, year, start_date=None, end_date=None):
        tmp_dir = self.output_dir / "tmp" / str(year)
        tmp_dir.mkdir(parents=True, exist_ok=True)
        
        # Ajout du bbox dans le nom du fichier
        bbox_str = ""
        if hasattr(self, 'bbox') and self.bbox is not None:
            bbox_str = f"_bbox_{self.bbox[0]}_{self.bbox[1]}_{self.bbox[2]}_{self.bbox[3]}"
        output_file = self.output_dir / f"tamsat_{self.product}_{year}{bbox_str}.nc"

        if output_file.exists():
            self.logger.info(f"⏩ Skipping {year}, already processed.")
            return
        
        zip_path = tmp_dir / f"{year}.zip"
        
        if not zip_path.exists():
            try:
                url = self.build_url(year)
                self.logger.info(f"⬇ Downloading {url}")
                response = requests.get(url, stream=True, timeout=60)
                response.raise_for_status()
                with open(zip_path, "wb") as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
            except Exception:
                self.logger.exception("❌ Download failed")
                return
        
        nc_files = sorted(tmp_dir.glob("**/*.nc"))
        if not nc_files:
            try:
                with zipfile.ZipFile(zip_path, "r") as zip_ref:
                    zip_ref.extractall(tmp_dir)
                self.logger.info(f"📦 Extracted {year}.zip")
                nc_files = sorted(tmp_dir.glob("**/*.nc"))
            except Exception as e:
                self.logger.error(f"❌ Extraction failed: {e}")
                return
        else:
            self.logger.info(f"📂 {len(nc_files)} NetCDF files already extracted")
        
        if not nc_files:
            self.logger.warning(f"⚠️ No NetCDF files found for {year}")
            return
        
        # ✅ FILTRAGE PAR PÉRIODE
        if start_date and end_date:
            start = datetime.strptime(start_date, "%Y-%m-%d")
            end = datetime.strptime(end_date, "%Y-%m-%d")
            filtered_files = []
            date_pattern = re.compile(r'(\d{4})_(\d{2})_(\d{2})')
            
            for f in nc_files:
                match = date_pattern.search(f.name)
                if match:
                    year_str, month_str, day_str = match.groups()
                    try:
                        file_date = datetime(int(year_str), int(month_str), int(day_str))
                        if start <= file_date <= end:
                            filtered_files.append(f)
                    except ValueError:
                        continue
            nc_files = filtered_files
            self.logger.info(f"📂 {len(nc_files)} jours sélectionnés pour la période")
        
        if not nc_files:
            self.logger.warning(f"⚠️ Aucun fichier dans la période demandée pour {year}")
            return
        
        # ✅ FUSION DIRECTE (sans fichiers intermédiaires)
    
        try:
            self.logger.info("📦 Fusion directe de l'année...")
            
            # Fonction de pré-traitement pour découper AVANT la fusion
            def preprocess(ds):
                if hasattr(self, 'bbox') and self.bbox is not None:
                    south, north, west, east = self.bbox
                    if ds.lat.values[0] > ds.lat.values[-1]:
                        lat_slice = slice(north, south)
                    else:
                        lat_slice = slice(south, north)
                    ds = ds.sel(lat=lat_slice, lon=slice(west, east))
                return ds
            
            ds = xr.open_mfdataset(
                nc_files,
                combine="by_coords",
                preprocess=preprocess,  # ← Découpage AVANT la fusion !
                chunks={"time": 1}      # ← Uniquement sur time, pas sur lat/lon
            )
            
            # Si le bbox a déjà été appliqué, on le note
            if hasattr(self, 'bbox') and self.bbox is not None:
                self.logger.info(f"📦 Bbox appliqué pendant l'ouverture")
            
            # Standardisation
            var_name = None
            for possible in ['precip', 'rfe', 'PR', 'rainfall']:
                if possible in ds.data_vars:
                    var_name = possible
                    break
            if var_name is None:
                var_name = list(ds.data_vars)[0]
            if var_name != 'PR':
                ds = ds.rename({var_name: 'PR'})
            
            # Ajout des attributs CF
            if 'lat' in ds.coords:
                ds.lat.attrs['standard_name'] = 'latitude'
                ds.lat.attrs['units'] = 'degrees_north'
            if 'lon' in ds.coords:
                ds.lon.attrs['standard_name'] = 'longitude'
                ds.lon.attrs['units'] = 'degrees_east'
            
            ds.attrs["product"] = self.product
            ds.attrs["source"] = "TAMSAT"
            
            # Sauvegarde
            encoding = {'PR': {"zlib": True, "complevel": 4}}
            ds.to_netcdf(output_file, encoding=encoding)
            self.logger.info(f"💾 Saved: {output_file}")
            
            ds.close()
            del ds

        except Exception as e:
            self.logger.error(f"❌ Merge failed: {e}")
            return
    
    def download(self, start_date, end_date, output_dir=None, bbox=None, points=None, **kwargs):
        # Gestion du dossier de sortie
        if output_dir is not None:
            self.output_dir = Path(output_dir)
            self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Stockage du bbox
        self.bbox = bbox
        if bbox is not None:
            self.logger.info(f"📦 Découpage spatial appliqué : {bbox}")
        else:
            self.logger.info("🌍 Aucun découpage spatial : données globales")
        
        # Vérification des dates
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        
        if start < datetime.strptime(self.AVAILABLE_FROM, "%Y-%m-%d"):
            raise ValueError(
                f"❌ TAMSAT n'est pas disponible avant le {self.AVAILABLE_FROM}. "
                f"Vous avez demandé à partir du {start_date}."
            )
        
        years = list(range(start.year, end.year + 1))
        self.logger.info(f"🔁 Téléchargement TAMSAT pour les années : {years}")
        
        # Traitement parallèle
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {}
            for year in years:
                futures[executor.submit(self.process_year, year, start_date, end_date)] = year
            
            for future in tqdm(as_completed(futures), total=len(futures), desc="Téléchargement"):
                year = futures[future]
                try:
                    future.result()
                except Exception as e:
                    self.logger.error(f"❌ Erreur pour {year}: {e}")
        
        # Si des points sont demandés → CSV direct
        if points is not None:
            self.logger.info("📊 Extraction des points en CSV...")
            nc_files = list(self.output_dir.glob(f"tamsat_{self.product}_*.nc"))
            if nc_files:
                ds = xr.open_dataset(nc_files[0])
                return self.to_csv(points=points)
            else:
                self.logger.error("❌ NetCDF non trouvé")
                return None

    def to_csv(self, output_csv=None, points=None):
        """
        Extrait les données en CSV pour des points spécifiques.
        Utilise une moyenne spatiale sur une fenêtre de ±0.10°
        pour éviter les pixels vides.
        """

        import pandas as pd

        if points is None:
            self.logger.error("❌ Le CSV nécessite une liste de points (lat/lon).")
            return None

        nc_files = list(self.output_dir.glob(f"tamsat_{self.product}_*.nc"))
        if not nc_files:
            self.logger.error("❌ Aucun fichier NetCDF trouvé.")
            return None

        ds = xr.open_dataset(nc_files[0])

        # Déterminer la variable de pluie
        var_name = 'PR' if 'PR' in ds.data_vars else list(ds.data_vars)[0]
        if var_name != 'PR':
            ds = ds.rename({var_name: 'PR'})
            self.logger.info(f"📝 Variable renommée : {var_name} → PR")

        records = []

        for point in points:
            lat = point.get("lat")
            lon = point.get("lon")

            if lat is None or lon is None:
                self.logger.warning("⚠️ Point ignoré : lat/lon manquant")
                continue

            # Fenêtre spatiale ±0.10°
            south = lat - 0.10
            north = lat + 0.10
            west = lon - 0.10
            east = lon + 0.10

            # Gestion de l'ordre des latitudes
            if ds.lat.values[0] > ds.lat.values[-1]:
                # latitudes décroissantes
                lat_slice = slice(north, south)
            else:
                # latitudes croissantes
                lat_slice = slice(south, north)

            lon_slice = slice(west, east)

            self.logger.debug(
                f"📍 Extraction autour de ({lat}, {lon}) "
                f"lat={lat_slice}, lon={lon_slice}"
            )

            subset = ds.PR.sel(lat=lat_slice, lon=lon_slice)

            # Vérifier qu'on a bien des pixels
            if subset.size == 0:
                self.logger.warning(
                    f"⚠️ Aucun pixel trouvé autour de ({lat}, {lon})"
                )
                continue

            # Moyenne spatiale (ignore les NaN)
            mean_ts = subset.mean(dim=["lat", "lon"], skipna=True)

            df_point = mean_ts.to_dataframe().reset_index()

            # Ajouter les coordonnées demandées
            df_point["lat"] = lat
            df_point["lon"] = lon
            df_point["point"] = f"({lat}, {lon})"

            records.append(df_point)

        ds.close()

        if not records:
            self.logger.error("❌ Aucun point valide extrait.")
            return None

        df = pd.concat(records, ignore_index=True)

        # Conversion du temps
        if "time" in df.columns:
            df["time"] = pd.to_datetime(df["time"])

        # Chemin du CSV
        if output_csv is None:
            output_csv = self.output_dir / f"tamsat_{self.product}_points.csv"
        else:
            output_csv = Path(output_csv)

        df.to_csv(output_csv, index=False)
        self.logger.info(f"💾 CSV sauvegardé : {output_csv}")

        return df
    
    def extract(self, variables=None, start_date=None, end_date=None, as_long=False, **kwargs):
        nc_files = list(self.output_dir.glob(f"tamsat_{self.product}_*.nc"))
        if not nc_files:
            self.logger.warning("Aucun fichier NetCDF trouvé.")
            return None
        
        ds = xr.open_mfdataset(nc_files, combine="by_coords")
        
        if start_date:
            start = datetime.strptime(start_date, "%Y-%m-%d") if isinstance(start_date, str) else start_date
            ds = ds.sel(time=slice(start, None))
        if end_date:
            end = datetime.strptime(end_date, "%Y-%m-%d")
            ds = ds.sel(time=slice(None, end))
        
        if variables:
            available_vars = [v for v in variables if v in ds.data_vars]
            if available_vars:
                ds = ds[available_vars]
        
        if as_long:
            ds = ds.stack(point=("lat", "lon")).reset_index("point")
        
        return ds