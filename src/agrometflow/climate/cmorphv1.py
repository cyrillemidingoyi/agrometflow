import gzip
import shutil
import requests
import numpy as np
import xarray as xr
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from agrometflow.utils import get_logger


class Cmorphv1Downloader:
    """
    Downloader for CMORPH V1.0BETA daily precipitation data.
    Source: https://ftp.cpc.ncep.noaa.gov/precip/CMORPH1_BC_00Z/bin
    """

    BASE_URL = "https://ftp.cpc.ncep.noaa.gov/precip/CMORPH1_BC_00Z/bin"

    # Paramètres de la grille CMORPH
    ROWS = 480
    COLS = 1440
    LAT_START = 59.875
    LAT_END = -59.875
    LON_START = 0.125
    LON_END = 359.875

    def __init__(self, output_dir="data/cmorph", log_file=None, verbose=False, max_workers=4):
        self.output_dir = Path(output_dir)
        self.raw_dir = self.output_dir / "bin"
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.logger = get_logger("agrometflow.cmorph", log_file=log_file, verbose=verbose)
        self.max_workers = max_workers
        self.bbox = None

    def _daterange(self, start_date, end_date):
        while start_date <= end_date:
            yield start_date
            start_date += timedelta(days=1)

    def _build_url(self, date):
        day = date.strftime("%Y%m%d")
        filename = f"CMORPH_V1.0_ADJ_0.25deg-DLY_00Z_{day}"
        url = f"{self.BASE_URL}/{filename}"
        return url, filename

    # ✅ Conversion du binaire en xarray avec découpage spatial
    def _convert_bin_to_xarray(self, bin_file, date):
        """
        Convertit un fichier binaire CMORPH en DataArray xarray.
        Applique le découpage spatial (bbox) si défini.
        """
        try:
            # Lire le binaire (little-endian float32)
            data = np.fromfile(bin_file, dtype='<f4')
            data = data.reshape((self.ROWS, self.COLS))
            
            # Remplacer les valeurs manquantes (-999.0) par NaN
            data = np.where(data < -900, np.nan, data)
            
            # Créer les coordonnées
            lats = np.linspace(self.LAT_START, self.LAT_END, self.ROWS)
            lons = np.linspace(self.LON_START, self.LON_END, self.COLS)
            
            # Créer le DataArray
            da = xr.DataArray(
                data,
                dims=["lat", "lon"],
                coords={"lat": lats, "lon": lons},
                name="PR"
            )
            
            # Ajouter le temps
            da = da.expand_dims(time=[np.datetime64(date)])
            
            # ✅ CONVERSION 0-360° → -180-180°
            da = da.assign_coords(lon=(((da.lon + 180) % 360) - 180))
            da = da.sortby("lon")
            
            # ✅ TRI DES LATITUDES (ordre croissant)
            da = da.sortby("lat")
            
            # ✅ DÉCOUPAGE SPATIAL (bbox)
            if self.bbox is not None:
                south, north, west, east = self.bbox
                
                # Détection automatique du sens des latitudes
                if da.lat.values[0] > da.lat.values[-1]:
                    lat_slice = slice(north, south)
                else:
                    lat_slice = slice(south, north)
                
                da = da.sel(lat=lat_slice, lon=slice(west, east))
            
            return da
        except Exception as e:
            self.logger.error(f"❌ Failed to convert {bin_file.name}: {e}")
            return None

    # Fusion annuelle avec métadonnées CF
    def _merge_yearly(self, bin_files_by_year):
        """
        Fusionne les fichiers d'une année en un seul NetCDF.
        """
        for year, file_date_pairs in bin_files_by_year.items():
            # ✅ NOM DU FICHIER AVEC BBOX
            bbox_str = ""
            if self.bbox is not None:
                bbox_str = f"_bbox_{self.bbox[0]}_{self.bbox[1]}_{self.bbox[2]}_{self.bbox[3]}"
            
            output_nc = self.output_dir / f"cmorph_v1_{year}{bbox_str}.nc"
            
            # ✅ CACHE INTELLIGENT : si le NetCDF existe déjà, on saute
            if output_nc.exists():
                self.logger.info(f"⏩ Skipping {year}, NetCDF already exists.")
                continue

            arrays = []
            for bin_file, date in sorted(file_date_pairs, key=lambda x: x[1]):
                da = self._convert_bin_to_xarray(bin_file, date)
                if da is not None:
                    arrays.append(da)

            if not arrays:
                self.logger.warning(f"⚠️ Aucun fichier valide pour {year}")
                continue

            try:
                # Fusionner tous les jours
                combined = xr.concat(arrays, dim="time")
                
                # ✅ MÉTADONNÉES CF
                combined.attrs["product"] = "CMORPH"
                combined.attrs["source"] = "NOAA CPC"
                combined.attrs["version"] = "V1.0BETA"
                combined.attrs["resolution"] = "0.25°"
                combined.attrs["description"] = "CMORPH V1.0BETA daily precipitation"
                if self.bbox is not None:
                    combined.attrs["bbox"] = str(self.bbox)
                
                # Attributs CF pour les coordonnées
                if 'lat' in combined.coords:
                    combined.lat.attrs['standard_name'] = 'latitude'
                    combined.lat.attrs['units'] = 'degrees_north'
                if 'lon' in combined.coords:
                    combined.lon.attrs['standard_name'] = 'longitude'
                    combined.lon.attrs['units'] = 'degrees_east'
                if 'time' in combined.coords:
                    combined.time.attrs['standard_name'] = 'time'
                
                # ✅ CHUNKING POUR L'ÉCRITURE (optimisation mémoire)
                encoding = {'PR': {"zlib": True, "complevel": 4, "chunksizes": (1, 100, 100)}}
                combined.to_netcdf(output_nc, encoding=encoding)
                self.logger.info(f"💾 Saved yearly NetCDF: {output_nc}")
                
                # ✅ NETTOYAGE DES FICHIERS BINAIRES (optionnel)
                # for bin_file, _ in file_date_pairs:
                #     bin_file.unlink()
                    
            except Exception as e:
                self.logger.error(f"❌ Merge failed for {year}: {e}")

    def _download_file(self, date):
        """Télécharge un fichier binaire CMORPH."""
        url, filename = self._build_url(date)
        bin_path = self.raw_dir / filename

        if bin_path.exists():
            self.logger.debug(f"✔ Already exists: {filename}")
            return bin_path

        try:
            self.logger.info(f"⬇ Downloading {filename}")
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            response = requests.get(url, stream=True, timeout=60, headers=headers)

            response.raise_for_status()

            with open(bin_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

            self.logger.info(f"✅ Downloaded: {filename}")
            return bin_path
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                self.logger.warning(f"⚠️ File not found: {filename}")
            else:
                self.logger.error(f"❌ HTTP error: {e}")
            return None
        except Exception as e:
            self.logger.error(f"❌ Failed to download {filename}: {e}")
            return None
    
    def download(self, start_date, end_date, output_dir=None, bbox=None, points=None, **kwargs):
        """
        Télécharge les données CMORPH pour une période donnée.
        
        Parameters
        ----------
        start_date : str (YYYY-MM-DD)
        end_date : str (YYYY-MM-DD)
        output_dir : str, optional
            Dossier de sortie
        bbox : list, optional
            [lat_min, lat_max, lon_min, lon_max]
        """
        # Gestion du dossier de sortie
        if output_dir is not None:
            self.output_dir = Path(output_dir)
            self.raw_dir = self.output_dir / "bin"
            self.raw_dir.mkdir(parents=True, exist_ok=True)
        
        # Stockage du bbox
        self.bbox = bbox
        if bbox is not None:
            self.logger.info(f"📦 Découpage spatial appliqué : {bbox}")
        else:
            self.logger.info("🌍 Aucun découpage spatial : données globales")
        
        # Vérification des dates
        start = datetime.strptime(start_date, "%Y-%m-%d") if isinstance(start_date, str) else start_date
        end = datetime.strptime(end_date, "%Y-%m-%d") if isinstance(end_date, str) else end_date
        
        dates = list(self._daterange(start, end))
        
        self.logger.info(f"🚀 Téléchargement CMORPH pour {len(dates)} jours avec {self.max_workers} workers...")
        
        # Téléchargement parallèle
        bin_files_by_year = {}
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {executor.submit(self._download_file, date): date for date in dates}
            for future in as_completed(futures):
                date = futures[future]
                bin_file = future.result()
                if bin_file:
                    bin_files_by_year.setdefault(date.year, []).append((bin_file, date))
        
        # Fusion par année
        self._merge_yearly(bin_files_by_year)

        # Si des points sont demandés → les extraire en CSV
        if points is not None:
            self.logger.info("📊 Extraction des points en CSV...")
            
            # Chercher le fichier NetCDF qui vient d'être créé
            nc_files = list(self.output_dir.glob("cmorph_v1_*.nc"))
            if nc_files:
                # Charger le NetCDF et extraire les points
                ds = xr.open_dataset(nc_files[0])
                return self.to_csv(points=points)
            else:
                self.logger.error("❌ NetCDF non trouvé")
                return None

    # ✅ MÉTHODE CSV
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
            self.logger.error("❌ Le CSV nécessite une liste de points (lat/lon).")
            return None
        
        # ✅ VÉRIFICATION : les fichiers NetCDF existent-ils ?
        nc_files = list(self.output_dir.glob("cmorph_v1_*.nc"))
        if not nc_files:
            self.logger.error("❌ Aucun fichier NetCDF trouvé. Veuillez d'abord exécuter download().")
            return None
        
        ds = xr.open_dataset(nc_files[0])
        
        records = []
        for point in points:
            lat = point.get("lat")
            lon = point.get("lon")
            if lat is None or lon is None:
                self.logger.warning("⚠️ Point ignoré : lat/lon manquant")
                continue
            
            # Sélectionner le point le plus proche
            da = ds.PR.sel(lat=lat, lon=lon, method='nearest')
            df_point = da.to_dataframe().reset_index()
            df_point["point"] = f"({lat}, {lon})"
            records.append(df_point)
        
        if not records:
            self.logger.error("❌ Aucun point valide.")
            return None
        
        df = pd.concat(records, ignore_index=True)
        
        if output_csv is None:
            output_csv = self.output_dir / "cmorph_v1_points.csv"
        else:
            output_csv = Path(output_csv)
        
        df.to_csv(output_csv, index=False)
        self.logger.info(f"💾 CSV sauvegardé : {output_csv}")
        return df
    
    # ✅ MÉTHODE EXTRACT
    def extract(self, variables=None, start_date=None, end_date=None, as_long=False, **kwargs):
        """
        Extrait et filtre les données NetCDF existantes.
        
        Parameters
        ----------
        variables : list of str, optional
            Liste des variables à conserver (pour CMORPH, seulement 'PR')
        start_date : str, optional
            Date de début de l'extraction (YYYY-MM-DD)
        end_date : str, optional
            Date de fin de l'extraction (YYYY-MM-DD)
        as_long : bool, optional
            Si True, retourne les données en format long (time, point)
        
        Returns
        -------
        xarray.Dataset or None
        """
        nc_files = list(self.output_dir.glob("cmorph_v1_*.nc"))
        
        if not nc_files:
            self.logger.warning(f"Aucun fichier NetCDF trouvé dans {self.output_dir}")
            return None
        
        ds = xr.open_mfdataset(nc_files, combine="by_coords")
        
        if start_date:
            start = datetime.strptime(start_date, "%Y-%m-%d") if isinstance(start_date, str) else start_date
            ds = ds.sel(time=slice(start, None))
        if end_date:
            end = datetime.strptime(end_date, "%Y-%m-%d") if isinstance(end_date, str) else end_date
            ds = ds.sel(time=slice(None, end))
        
        if variables:
            available_vars = [v for v in variables if v in ds.data_vars]
            if available_vars:
                ds = ds[available_vars]
        
        if as_long:
            ds = ds.stack(point=("lat", "lon")).reset_index("point")
        
        return ds
