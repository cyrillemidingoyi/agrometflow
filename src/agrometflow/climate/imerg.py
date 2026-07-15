import os
import requests
import xarray as xr
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from agrometflow.utils import get_logger
import numpy as np


class ImergDownloader:
    """
      Téléchargeur pour les produits IMERG (Late, Early, Final)
        Produits supportés : 
        - imergL : IMERG Late (3IMERGDL.07) - quasi temps réel, 12h de latence
        - imergE : IMERG Early (3IMERGDE.07) - quasi temps réel, 4h de latence
        - imergF : IMERG Final (3IMERGDF.07) - consolidé, référence scientifique
    """
    # Dictionnaire des produits disponibles
    PRODUCTS = {
        "imergL": {
            "base_url": "https://gpm1.gesdisc.eosdis.nasa.gov/data/GPM_L3/GPM_3IMERGDL.07",
            "description": "IMERG Late (near real-time, ~12h latency)",
            "filename_pattern": "3B-DAY-L.MS.MRG.3IMERG.{yyyymmdd}-S000000-E235959.V07B.nc4",
            "available_from": "1998-01-01",
            "available_to": "present"
        },
        "imergE": {
            "base_url": "https://gpm1.gesdisc.eosdis.nasa.gov/data/GPM_L3/GPM_3IMERGDE.07",
            "description": "IMERG Early (near real-time, ~4h latency)",
            "filename_pattern": "3B-DAY-E.MS.MRG.3IMERG.{yyyymmdd}-S000000-E235959.V07B.nc4",
            "available_from": "1998-01-01",
            "available_to": "present"
        },
        "imergF": {
            "base_url": "https://gpm1.gesdisc.eosdis.nasa.gov/data/GPM_L3/GPM_3IMERGDF.07",
            "description": "IMERG Final (research quality, ~2-3 months latency)",
            "filename_pattern": "3B-DAY-F.MS.MRG.3IMERG.{yyyymmdd}-S000000-E235959.V07B.nc4",
            "available_from": "1998-01-01",
            "available_to": "present"
        }
    }
     
    def __init__( self, product="imergL", output_dir="data/imerg", log_file=None, verbose=False, max_workers=4, token=None):

        if product not in self.PRODUCTS:
            raise ValueError(f"Produit '{product}' non supporté. Choisir parmi : {list(self.PRODUCTS.keys())}")

        self.product = product
        self.config = self.PRODUCTS[product]
        self.base_url = self.config["base_url"]
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.logger = get_logger("agrometflow.imerg", log_file=log_file, verbose=verbose)
        self.max_workers = max_workers
        self.token = token or os.getenv("NASA_EARTHDATA_TOKEN")

        if not self.token:
            self.logger.warning("⚠️ Aucun token NASA Earthdata trouvé. Les téléchargements peuvent échouer.")
        else:
            self.logger.info(f"🔐 Authentification NASA configurée")

        self.logger.info(f"🔧 Initialisation du produit IMERG : {product}")
        self.logger.info(f"   Description : {self.config['description']}")

    def _daterange(self, start_date, end_date):
        while start_date <= end_date:
            yield start_date
            start_date += timedelta(days=1)

    def _get_url_and_filename(self, date):
        """Construit l'URL et le nom du fichier pour une date donnée."""
        yyyy = date.strftime("%Y")
        mm = date.strftime("%m")
        dd = date.strftime("%d")
        yyyymmdd = date.strftime("%Y%m%d")
        
        filename = self.config["filename_pattern"].format(yyyymmdd=yyyymmdd)
        url = f"{self.base_url}/{yyyy}/{mm}/{filename}"
        return url, filename

    def _download_file(self, date):
        """Télécharge un fichier IMERG pour une date donnée."""
        url, filename = self._get_url_and_filename(date)
        local_path = self.output_dir / filename

        if local_path.exists():
            self.logger.debug(f"Already downloaded: {filename}")
            return local_path, date

        try:
            headers = {"Authorization": f"Bearer {self.token}"} if self.token else {}
            with requests.get(url, headers=headers, stream=True, timeout=60) as r:
                if r.status_code == 401:
                    self.logger.error(
                        "❌ Authentification NASA requise.\n"
                        "   Pour obtenir un token : https://urs.earthdata.nasa.gov/\n"
                        "   Puis définissez la variable d'environnement NASA_EARTHDATA_TOKEN"
                    )
                    return None, date
                r.raise_for_status()
                with open(local_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
            self.logger.info(f"✅ Downloaded: {filename}")
            return local_path, date
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                self.logger.warning(f"⚠️ Fichier non trouvé: {filename}")
            else:
                self.logger.error(f"❌ HTTP Error {e.response.status_code}: {filename}")
            return None, date
        except Exception as e:
            self.logger.error(f"❌ Failed to download {filename}: {e}")
            return None, date

    def _merge_yearly(self, files_by_year):
        for year, file_date_pairs in files_by_year.items():
            outfile = self.output_dir / f"imerg_{self.product}_{year}.nc"
            if outfile.exists():
                self.logger.info(f"⏩ Skipping merge for {year}, already exists.")
                continue

            arrays = []
            for f, date in sorted(file_date_pairs, key=lambda x: x[1]):
                try:
                    ds = xr.open_dataset(f)
                    ds = ds.expand_dims(time=[np.datetime64(date)])
                    arrays.append(ds)
                except Exception as e:
                    self.logger.error(f"⚠️ Failed to read {f.name}: {e}")

            if arrays:
                combined = xr.concat(arrays, dim="time")
                 
                # Découpage spatial (bbox)
                if hasattr(self, 'bbox') and self.bbox is not None:
                    south, north, west, east = self.bbox
                    if combined.lat.values[0] > combined.lat.values[-1]:
                        lat_slice = slice(north, south)
                    else:
                        lat_slice = slice(south, north)
                    combined = combined.sel(lat=lat_slice, lon=slice(west, east))
                    self.logger.info(f"📦 Découpage spatial appliqué : {self.bbox}")
                    self.logger.info(f"   Dimensions après découpage : {combined.dims}")

                    if combined.sizes.get('lat', 0) == 0 or combined.sizes.get('lon', 0) == 0:
                        raise ValueError(
                            f"❌ Le bbox {self.bbox} ne recoupe pas les données disponibles."
                        )

                # Standardiser le nom de la variable
                var_renamed = False
                for old_name in ['precipitationCal', 'precipitation', 'PR']:
                    if old_name in combined.data_vars:
                        if old_name != 'PR':
                            combined = combined.rename({old_name: 'PR'})
                        var_renamed = True
                        break

                if not var_renamed:
                    self.logger.warning(f"⚠️ Variable de précipitation non trouvée dans {outfile}")

                # Ajouter les attributs CF
                if 'lat' in combined.coords:
                    combined.lat.attrs['standard_name'] = 'latitude'
                    combined.lat.attrs['units'] = 'degrees_north'
                if 'lon' in combined.coords:
                    combined.lon.attrs['standard_name'] = 'longitude'
                    combined.lon.attrs['units'] = 'degrees_east'
                if 'time' in combined.coords:
                    combined.time.attrs['standard_name'] = 'time'

                combined.attrs["product"] = self.product
                combined.attrs["source"] = "IMERG"
                combined.attrs["description"] = self.config["description"]

                combined.to_netcdf(outfile)
                self.logger.info(f"💾 Saved yearly file: {outfile}")

    def download(self, start_date, end_date, output_dir=None, bbox=None, **kwargs):
        if output_dir is not None:
            self.output_dir = Path(output_dir)
            self.output_dir.mkdir(parents=True, exist_ok=True)

        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")

        available_from = self.config.get("available_from")
        available_to = self.config.get("available_to")

        if available_from:
            from_date = datetime.strptime(available_from, "%Y-%m-%d")
            if start < from_date:
                raise ValueError(
                    f"❌ Le produit '{self.product}' n'est pas disponible avant le {available_from}. "
                    f"Vous avez demandé à partir du {start_date}."
                )

        self.bbox = bbox
        if bbox is not None:
            self.logger.info(f"📦 Découpage spatial appliqué : {bbox}")
        else:
            self.logger.info("🌍 Aucun découpage spatial : données globales")

        dates = list(self._daterange(start, end))
        self.logger.info(f"🚀 Downloading IMERG '{self.product}' data from {start.date()} to {end.date()} using {self.max_workers} workers.")

        files_by_year = {}

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {executor.submit(self._download_file, date): date for date in dates}
            for future in as_completed(futures):
                f, date = future.result()
                if f:
                    files_by_year.setdefault(date.year, []).append((f, date))

        self._merge_yearly(files_by_year)

    def to_csv(self, output_csv=None, points=None):
        if points is None:
            self.logger.error("❌ Le CSV nécessite une liste de points (lat/lon).")
            return None

        nc_files = list(self.output_dir.glob(f"imerg_{self.product}_*.nc"))
        if not nc_files:
            self.logger.error("❌ Aucun fichier NetCDF trouvé.")
            return None

        ds = xr.open_dataset(nc_files[0])
        var_name = 'PR' if 'PR' in ds.data_vars else 'precipitationCal'

        records = []
        for point in points:
            lat = point.get("lat")
            lon = point.get("lon")
            if lat is None or lon is None:
                self.logger.warning("⚠️ Point ignoré : lat/lon manquant")
                continue

            da = ds[var_name].sel(lat=lat, lon=lon, method='nearest')
            df_point = da.to_dataframe().reset_index()
            df_point["point"] = f"({lat}, {lon})"
            records.append(df_point)

        if not records:
            self.logger.error("❌ Aucun point valide.")
            return None

        df = pd.concat(records, ignore_index=True)

        if output_csv is None:
            output_csv = self.output_dir / f"imerg_{self.product}_points.csv"
        else:
            output_csv = Path(output_csv)

        df.to_csv(output_csv, index=False)
        self.logger.info(f"💾 CSV sauvegardé : {output_csv}")
        return df
