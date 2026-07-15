# -*- coding: utf-8 -*-
import xarray as xr
import numpy as np
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from agrometflow.utils import get_logger
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from oauth2client.file import Storage

drive = "https://drive.google.com/drive/folders/1Kok05OPVESTpyyan7NafR-2WwuSJ4TO9"

class MswepDownloader:
    def __init__(self, folder_id, dataset_type="auto", output_dir="data/mswep", log_file=None, verbose=False, max_workers=4):
        self.folder_id = folder_id
        self.dataset_type = dataset_type  # "auto", "Past", "NRT", "Past_nogauge"
        self.output_dir = Path(output_dir)
        self.raw_dir = self.output_dir / "daily"
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.logger = get_logger("agrometflow.mswep", log_file=log_file, verbose=verbose)
        self.max_workers = max_workers
        self.drive = self._auth_drive()

        # Récupérer les IDs des sous-dossiers
        self.past_daily_folder_id = "1gWoZ2bK2u5osJ8Iw-dvguZ56Kmz2QWrL"
        self.nrt_daily_folder_id = "1aehP6YDNOO73ab3tvTZet2Sh5uPdG9I_"
        self.nogauge_daily_folder_id = "1EDmRUu5rZyo-0eQ0dDild1A8i6GkcBFu"

    def _auth_drive(self):
        
        gauth = GoogleAuth()
        
        # Vérifier si un token sauvegardé existe
        storage = Storage('credentials.dat')
        credentials = storage.get()
        
        if credentials:
            gauth.credentials = credentials
        else:
            gauth.LocalWebserverAuth()
            # Sauvegarder le token pour la prochaine fois
            storage.put(gauth.credentials)
        
        return GoogleDrive(gauth)

    def _choose_dataset_type(self, start_date, end_date):
        """
        Sélectionne automatiquement le type de données MSWEP en fonction de la période.
        
        - Past : données historiques corrigées (1979-2020) → usage général
        - NRT  : données quasi temps réel (après 2020) → suivi en temps réel
        - Past_nogauge : données sans stations (1979-2020) → usage spécifique
        """
        start = datetime.strptime(start_date, "%Y-%m-%d")
        
        # Si la période est après 2020 → NRT
        if start.year >= 2021:
            return "NRT"
        # Sinon → Past (par défaut)
        else:
            return "Past"

    def _daterange(self, start_date, end_date):
        while start_date <= end_date:
            yield start_date
            start_date += timedelta(days=1)

    def _build_filename(self, date):
        year = date.strftime("%Y")
        doy = date.strftime("%j")
        return f"{year}{doy}.nc"

    def _find_file_in_drive(self, filename, dataset_type):
        # Choisir le bon ID en fonction du dataset_type
        if dataset_type == "NRT":
            search_folder_id = self.nrt_daily_folder_id
        elif dataset_type == "Past_nogauge":
            search_folder_id = self.nogauge_daily_folder_id
        else:  # "Past" ou "auto"
            search_folder_id = self.past_daily_folder_id

        query = f"'{search_folder_id}' in parents and title='{filename}'"
        file_list = self.drive.ListFile({'q': query}).GetList()
        return file_list[0] if file_list else None

    def _download_file(self, date, dataset_type):
        filename = self._build_filename(date)
        out_path = self.raw_dir / filename

        if out_path.exists():
            self.logger.debug(f"✔ Already exists: {filename}")
            return out_path, date

        drive_file = self._find_file_in_drive(filename, dataset_type)
        if not drive_file:
            self.logger.warning(f"⚠️ File not found: {filename}")
            return None, date

        try:
            drive_file.GetContentFile(str(out_path))
            self.logger.info(f"✅ Downloaded: {filename}")
            return out_path, date
        except Exception as e:
            self.logger.error(f"❌ Failed to download {filename}: {e}")
            return None, date
        
    def _merge_yearly(self, files_by_year):
        for year, file_date_pairs in files_by_year.items():
            outfile = self.output_dir / f"mswep_{year}.nc"
            if outfile.exists():
                self.logger.info(f"⏩ Skipping merge for {year}, already exists.")
                continue

            arrays = []
            for f, date in sorted(file_date_pairs, key=lambda x: x[1]):
                try:
                    ds = xr.open_dataset(f)
                    if 'time' not in ds.dims:
                        ds = ds.expand_dims(time=[np.datetime64(date)])
                    arrays.append(ds)
                except Exception as e:
                    self.logger.error(f"⚠️ Error reading {f.name}: {e}")

            if arrays:
                combined = xr.concat(arrays, dim="time")
                
                # ✅ S'assurer que lat et lon sont des coordonnées
                if 'lat' in combined.dims and 'lon' in combined.dims:
                    # Ajouter les attributs CF
                    combined.lat.attrs['standard_name'] = 'latitude'
                    combined.lat.attrs['units'] = 'degrees_north'
                    combined.lon.attrs['standard_name'] = 'longitude'
                    combined.lon.attrs['units'] = 'degrees_east'
                    if 'time' in combined.coords:
                        combined.time.attrs['standard_name'] = 'time'
                else:
                    # Si lat/lon sont des variables 1D, les promouvoir en coordonnées
                    if 'lat' in combined.data_vars:
                        combined = combined.set_coords('lat')
                    if 'lon' in combined.data_vars:
                        combined = combined.set_coords('lon')
                
                # ✅ Découpage spatial (bbox)
                if hasattr(self, 'bbox') and self.bbox is not None:
                    south, north, west, east = self.bbox

                    # Vérifier que le bbox est valide
                    if south >= north or west >= east:
                        raise ValueError(f"❌ Bbox invalide : {self.bbox}")

                    # Déterminer automatiquement le sens des latitudes
                    if combined.lat.values[0] > combined.lat.values[-1]:
                        lat_slice = slice(north, south)
                    else:
                        lat_slice = slice(south, north)

                    combined = combined.sel(
                        lat=lat_slice,
                        lon=slice(west, east)
                    )

                    # ✅ VÉRIFICATION : s'assurer que le bbox a donné des résultats
                    if combined.sizes.get('lat', 0) == 0 or combined.sizes.get('lon', 0) == 0:
                        raise ValueError(
                            f"❌ Le bbox {self.bbox} ne recoupe pas les données du jeu MSWEP.\n"
                            f"   Latitudes disponibles : {combined.lat.min().values:.2f} → {combined.lat.max().values:.2f}\n"
                            f"   Longitudes disponibles : {combined.lon.min().values:.2f} → {combined.lon.max().values:.2f}"
                        )

                    self.logger.info(f"📦 Découpage spatial appliqué : {self.bbox}")
                    self.logger.info(f"Dimensions après découpage : {combined.dims}")

                    if combined.sizes["lat"] == 0 or combined.sizes["lon"] == 0:
                        raise ValueError(
                            f"Le bbox {self.bbox} ne recoupe pas les données du jeu MSWEP."
                        )
                
                # ✅ Standardiser le nom de la variable
                if 'precipitation' in combined.data_vars:
                    combined = combined.rename({'precipitation': 'PR'})
                
                # ✅ Écrire en NetCDF
                combined.to_netcdf(outfile, engine='netcdf4', format='NETCDF4')
                self.logger.info(f"💾 Saved yearly file: {outfile}")

    def download(self, start_date, end_date, output_dir=None, bbox=None, dataset_type=None, **kwargs):
        # Stocker le bbox
        self.bbox = bbox
        # 1. Déterminer le type de données
        if dataset_type is None:
            dataset_type = self.dataset_type

        if dataset_type == "auto":
            dataset_type = self._choose_dataset_type(start_date, end_date)

        self.logger.info(f"📂 Type de données MSWEP utilisé : {dataset_type}")

        # 2. Si output_dir est passé, on le prend en compte
        if output_dir is not None:
            self.output_dir = Path(output_dir)

        # 3. Définir le dossier de téléchargement
        self.raw_dir = self.output_dir / dataset_type / "daily"
        self.raw_dir.mkdir(parents=True, exist_ok=True)

        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        dates = list(self._daterange(start, end))

        self.logger.info(f"🚀 Downloading MSWEP data from {start.date()} to {end.date()}")

        files_by_year = {}

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {executor.submit(self._download_file, date, dataset_type): date for date in dates}
            for future in as_completed(futures):
                f, date = future.result()
                if f:
                    files_by_year.setdefault(date.year, []).append((f, date))

        self._merge_yearly(files_by_year)

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
        
        import pandas as pd
        import xarray as xr
        
        # Charger le fichier NetCDF
        nc_files = list(self.output_dir.glob("mswep_*.nc"))
        if not nc_files:
            self.logger.error("❌ Aucun fichier NetCDF trouvé.")
            return None
        
        ds = xr.open_dataset(nc_files[0])
        
        # Vérifier le nom de la variable (PR ou precipitation)
        var_name = 'PR' if 'PR' in ds.data_vars else 'precipitation'
        
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
            output_csv = self.output_dir / "mswep_points.csv"
        else:
            output_csv = Path(output_csv)
        
        df.to_csv(output_csv, index=False)
        self.logger.info(f"💾 CSV sauvegardé : {output_csv}")
        return df