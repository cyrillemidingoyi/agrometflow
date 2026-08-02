import requests
import rioxarray
import xarray as xr
import numpy as np
import pandas as pd
from pathlib import Path
from zipfile import ZipFile
from io import BytesIO
from datetime import datetime, timedelta
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

from agrometflow.utils import get_logger


class Rfe2Downloader:
    BASE_URL = "https://ftp.cpc.ncep.noaa.gov/fews/fewsdata/africa/rfe2/geotiff"

    def __init__(self, output_dir="data/rfe2", log_file=None, verbose=False, max_workers=6):
        self.logger = get_logger("rfe2", log_file, verbose)
        self.output_dir = Path(output_dir)
        self.bbox = None
        self.max_workers = max_workers


    def _parse_date(self, date):
        return datetime.strptime(date, "%Y-%m-%d") if isinstance(date, str) else date

    def build_url(self, date):
        return f"{self.BASE_URL}/africa_rfe.{date.strftime('%Y%m%d')}.tif.zip"

    def download_and_extract(self, date, tif_dir):
        url = self.build_url(date)

        zip_name = url.split("/")[-1]
        tif_name = zip_name.replace(".zip", "")
        tif_path = tif_dir / tif_name

        # Le fichier existe déjà
        if tif_path.exists():
            return tif_path

        try:
            self.logger.debug(f"⬇ Downloading {url}")

            response = requests.get(url, stream=True, timeout=30)
            response.raise_for_status()

            with ZipFile(BytesIO(response.content)) as z:
                members = z.namelist()

                if not members:
                    self.logger.warning(f"Archive vide : {zip_name}")
                    return None

                # Recherche du GeoTIFF dans le ZIP
                member = next((m for m in members if m.lower().endswith(".tif")), None)

                if member is None:
                    self.logger.warning(f"Aucun fichier .tif trouvé dans {zip_name}")
                    return None

                with z.open(member) as src, open(tif_path, "wb") as dst:
                    dst.write(src.read())

            return tif_path if tif_path.exists() else None

        except Exception as e:
            self.logger.warning(f"Failed for {zip_name}: {e}")
            return None
    
    def convert_all_to_netcdf_per_year(self, files_by_year: dict, output_dir):
        for year, tif_files in files_by_year.items():
            self.logger.info(f"📂 Traitement de {len(tif_files)} fichiers pour l'année {year}")
            
            bbox_str = ""
            if self.bbox is not None:
                bbox_str = f"_bbox_{self.bbox[0]}_{self.bbox[1]}_{self.bbox[2]}_{self.bbox[3]}"
            output_nc = output_dir / f"rfe2_{year}{bbox_str}.nc"
            if output_nc.exists():
                self.logger.info(f"{output_nc} already exists. Skipping.")
                continue

            def convert_one(tif_file):
                try:
                    date_str = tif_file.stem.split(".")[-1]
                    timestamp = datetime.strptime(date_str, "%Y%m%d")

                    self.logger.debug(
                        f"📄 Conversion de {tif_file.name}"
                    )

                    da = self.convert_bin_to_xarray(
                        str(tif_file.resolve()),
                        timestamp
                    )

                    return timestamp, da

                except Exception as e:
                    self.logger.error(
                        f"❌ Failed to read {tif_file.name}: {e}"
                    )
                    return None, None


            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:

                futures = [
                    executor.submit(convert_one, tif_file)
                    for tif_file in sorted(tif_files)
                ]

                results = []

                for future in as_completed(futures):
                    timestamp, da = future.result()

                    if da is not None:
                        results.append((timestamp, da))


            # remettre dans l'ordre chronologique
            results.sort(key=lambda x: x[0])

            arrays = [da for _, da in results]

            self.logger.info(f"📊 {len(arrays)} fichiers lus sur {len(tif_files)}")
            
            if arrays:
                try:
                    self.logger.info(f"🔄 Fusion de {len(arrays)} fichiers...")
                    combined = xr.concat(arrays, dim="time")
                    self.logger.info(f"✅ Fusion réussie ! Dimensions : {combined.dims}")
                    self.logger.info(f"   - time : {len(combined.time)} jours")
        
                    # Métadonnées CF
                    for coord in combined.coords:
                        if coord in ['lon', 'longitude', 'x']:
                            combined[coord].attrs['standard_name'] = 'longitude'
                            combined[coord].attrs['units'] = 'degrees_east'
                        elif coord in ['lat', 'latitude', 'y']:
                            combined[coord].attrs['standard_name'] = 'latitude'
                            combined[coord].attrs['units'] = 'degrees_north'
                    
                    if 'time' in combined.coords:
                        combined.time.attrs['standard_name'] = 'time'
                    
                    combined.attrs["product"] = "RFE2"
                    combined.attrs["source"] = "NOAA CPC"
                    combined.attrs["description"] = "RFE2 daily precipitation"
                    if self.bbox is not None:
                        combined.attrs["bbox"] = str(self.bbox)
                    
                    # Chunking
                    encoding = {'PR': {"zlib": True, "complevel": 4, "chunksizes": (1, 100, 100)}}
                    combined.to_netcdf(output_nc, encoding=encoding)
                    self.logger.info(f"🎯 Yearly NetCDF saved: {output_nc}")
                except Exception as e:
                    self.logger.error(f"❌ Merge failed for {year}: {e}")

    def download(self, start_date, end_date, output_dir=None, bbox=None, points=None, **kwargs):
        """
        Télécharge les données RFE2 pour une période donnée.
        
        Parameters
        ----------
        start_date : str (YYYY-MM-DD)
        end_date : str (YYYY-MM-DD)
        output_dir : str, optional
            Dossier de sortie
        bbox : list, optional
            [south, north, west, east]
        points : list, optional
            Points à extraire en CSV
        """
        # ✅ Gestion du dossier de sortie
        if output_dir is None:
            output_dir = self.output_dir
        else:
            self.output_dir = Path(output_dir)
        
        output_dir = Path(output_dir) / "PR"
        max_workers = kwargs.get("max_workers", self.max_workers)
        
        # ✅ Stockage du bbox
        self.bbox = bbox
        if bbox is not None:
            self.logger.info(f"📦 Découpage spatial appliqué : {bbox}")
        else:
            self.logger.info("🌍 Aucun découpage spatial : données globales")
        
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        
        tif_dir = output_dir / "tifs"
        tif_dir.mkdir(parents=True, exist_ok=True)
        
        start = self._parse_date(start_date)
        end = self._parse_date(end_date)
        all_dates = list(self._daterange(start, end))
        
        self.logger.info(f"Downloading {len(all_dates)} daily RFE2 files with {max_workers} workers...")
        
        files_by_year = defaultdict(list)
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(self.download_and_extract, date, tif_dir): date for date in all_dates}
            
            for future in as_completed(futures):
                date = futures[future]
                tif_path = future.result()
                if tif_path is None:
                    continue

                expected = date.strftime("%Y%m%d")

                if expected not in tif_path.stem:
                    self.logger.error(
                        f"Le fichier retourné ({tif_path.name}) ne correspond pas à la date {expected}"
                    )
                    continue

                files_by_year[date.year].append(tif_path)
        
        self.convert_all_to_netcdf_per_year(files_by_year, output_dir)
        
        # ✅ Si des points sont demandés → CSV direct
        if points is not None:
            self.logger.info("📊 Extraction des points en CSV...")
            nc_files = list(Path(output_dir).glob("rfe2_*.nc"))
            if nc_files:
                ds = xr.open_dataset(nc_files[0])
                return self.to_csv(points=points)
            else:
                self.logger.error("❌ NetCDF non trouvé")
                return None
        
    
    def _daterange(self, start, end):
        while start <= end:
            yield start
            start += timedelta(days=1)
    
    def convert_bin_to_xarray(self, tif_file, date):
        """Convertit un GeoTIFF en DataArray xarray avec bbox."""
        try:
            # ✅ tif_file est maintenant une chaîne de caractères
            self.logger.debug(f"Ouverture du fichier : {tif_file}")
            ds = rioxarray.open_rasterio(tif_file)
            ds = ds.squeeze("band", drop=True)
            
            # Pour un DataArray, le nom est ds.name
            ds.name = 'PR'
            
            # ✅ Découpage spatial (bbox)
            if self.bbox is not None:
                south, north, west, east = self.bbox
                
                # Détection des noms de coordonnées
                lon_name = None
                lat_name = None
                for coord in ds.coords:
                    if coord in ['lon', 'longitude', 'x']:
                        lon_name = coord
                    elif coord in ['lat', 'latitude', 'y']:
                        lat_name = coord
                
                if lon_name is not None:
                    lon_ascending = ds[lon_name].values[0] <= ds[lon_name].values[-1]
                    lon_slice = slice(west, east) if lon_ascending else slice(east, west)
                    ds = ds.sel({lon_name: lon_slice})
                
                if lat_name is not None:
                    lat_ascending = ds[lat_name].values[0] <= ds[lat_name].values[-1]
                    lat_slice = slice(south, north) if lat_ascending else slice(north, south)
                    ds = ds.sel({lat_name: lat_slice})
            
            # Ajouter la dimension temps
            ds = ds.expand_dims(time=[np.datetime64(date)])
            return ds
        except Exception as e:
            self.logger.error(f"❌ Failed to convert {Path(tif_file).name}: {e}")
            return None
        
    def to_csv(self, output_csv=None, points=None):
        """Extrait les données en CSV pour des points spécifiques."""
        if points is None:
            self.logger.error("❌ Le CSV nécessite une liste de points (lat/lon).")
            return None
        
        nc_files = list(self.output_dir.glob("PR/*.nc"))
        if not nc_files:
            nc_files = list(self.output_dir.glob("rfe2_*.nc"))
        
        if not nc_files:
            self.logger.error("❌ Aucun fichier NetCDF trouvé.")
            return None
        
        ds = xr.open_dataset(nc_files[0])
        
        # ✅ Vérifier le nom de la variable PR
        if 'PR' not in ds.data_vars:
            var_name = list(ds.data_vars)[0]
            ds = ds.rename({var_name: 'PR'})
            self.logger.info(f"📝 Variable renommée : {var_name} → PR")
        
        # ✅ Identifier les noms des coordonnées
        lon_name = None
        lat_name = None
        for coord in ds.coords:
            if coord in ['lon', 'longitude', 'x']:
                lon_name = coord
            elif coord in ['lat', 'latitude', 'y']:
                lat_name = coord
        
        self.logger.info(f"🔍 Coordonnées trouvées : longitude={lon_name}, latitude={lat_name}")
        
        # ✅ Si les coordonnées sont x et y, les renommer en lat/lon
        if lon_name == 'x' and lat_name == 'y':
            ds = ds.rename({'x': 'lon', 'y': 'lat'})
            lon_name = 'lon'
            lat_name = 'lat'
            self.logger.info("📝 Coordonnées renommées : x→lon, y→lat")
        
        records = []
        for point in points:
            lat = point.get("lat")
            lon = point.get("lon")
            if lat is None or lon is None:
                self.logger.warning("⚠️ Point ignoré : lat/lon manquant")
                continue
            
            self.logger.info(f"📍 Extraction pour le point ({lat}, {lon})")
            
            # ✅ Extraire la série temporelle complète pour ce point
            try:
                da = ds.PR.sel(lat=lat, lon=lon, method='nearest')
                df_point = da.to_dataframe().reset_index()
                
                # ✅ Vérifier le nombre de jours extraits
                self.logger.info(f"   📊 {len(df_point)} jours extraits")
                
                if len(df_point) == 0:
                    self.logger.warning(f"⚠️ Aucune donnée pour ({lat}, {lon})")
                    continue
                
                df_point["point"] = f"({lat}, {lon})"
                records.append(df_point)
            except Exception as e:
                self.logger.error(f"❌ Erreur pour ({lat}, {lon}): {e}")
        
        if not records:
            self.logger.error("❌ Aucun point valide.")
            return None
        
        df = pd.concat(records, ignore_index=True)
        
        # ✅ Trier par date
        if 'time' in df.columns:
            df = df.sort_values('time')
            self.logger.info(f"📊 Total: {len(df)} lignes, {df['time'].nunique()} jours uniques")
        
        if output_csv is None:
            output_csv = self.output_dir / "rfe2_points.csv"
        else:
            output_csv = Path(output_csv)
        
        df.to_csv(output_csv, index=False)
        self.logger.info(f"💾 CSV sauvegardé : {output_csv}")
        return df