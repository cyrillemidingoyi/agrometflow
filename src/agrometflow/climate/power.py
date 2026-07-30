import requests
import pandas as pd
import xarray as xr
import numpy as np
from pathlib import Path
from datetime import datetime
from agrometflow.climate.base import ClimateSource
from agrometflow.utils import get_logger
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

#logger = get_logger(__name__)


class PowerDownloader(ClimateSource):
    """
    Downloader for NASA POWER daily climate data using the REST API.
    API doc: https://power.larc.nasa.gov/docs/services/api/

    Variables mapping (target → source):
    - PR → PRECTOTCORR (Precipitation)
    - T2M → T2M (Temperature at 2m)
    - RH2M → RH2M (Relative Humidity)
    - WS2M → WS2M (Wind Speed)
    - RSDS → ALLSKY_SFC_SW_DWN (Solar Radiation)
    """

    BASE_URL_POINT = "https://power.larc.nasa.gov/api/temporal/daily/point"
    BASE_URL_REGIONAL = "https://power.larc.nasa.gov/api/temporal/daily/regional"

    def __init__(self, log_file=None, verbose=False):
        self.logger = get_logger(__name__, log_file=log_file, verbose=verbose)
        self.data = None
        self.output_dir = None

    def download(self, **kwargs):
        """
        Downloads data for a single point or region.

        Parameters
        ----------
        start_date : str (YYYY-MM-DD)
        end_date : str (YYYY-MM-DD)
        variables : list of str
            List of target variable names (e.g., ["PR", "T2M"])
        output_dir : str or Path
            Directory where the downloaded files should be saved
        bbox : list [south, north, west, east]
        points : list of tuples (lat, lon)
        """
        try:
            start_date = kwargs["start_date"]
            end_date = kwargs["end_date"]
            variables = kwargs["variables"]
            output_dir = kwargs["output_dir"]
        except KeyError as e:
            raise ValueError(f"Missing required argument: {e}")

        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Format dates
        start = datetime.strptime(start_date, "%Y-%m-%d").strftime("%Y%m%d")
        end = datetime.strptime(end_date, "%Y-%m-%d").strftime("%Y%m%d")

        # ✅ Si des points sont demandés → CSV direct
        if "points" in kwargs:
            points = kwargs["points"]
            
            # ✅ Convertir les variables en format POWER
            power_vars = self._get_power_variables(variables)
            
            all_data = []

            with ThreadPoolExecutor(max_workers=8) as executor:
                futures = [
                    executor.submit(
                        self._fetch_power_point, lat, lon, start, end, power_vars, variables
                    )
                    for lat, lon in points
                ]

                for f in tqdm(as_completed(futures), total=len(futures), desc="Downloading NASA POWER data"):
                    result = f.result()
                    if result is not None:
                        all_data.append(result)

            if not all_data:
                raise RuntimeError("No data fetched from POWER.")

            full_df = pd.concat(all_data, ignore_index=True)
            self.data = full_df

            # Sauvegarde CSV
            filename = self.output_dir / f"power_points_{start_date}_{end_date}.csv"
            full_df.to_csv(filename, index=False)
            self.logger.info(f"💾 CSV sauvegardé : {filename}")
            return full_df

        # ✅ Si bbox est fourni → NetCDF
        elif "bbox" in kwargs:
            bbox = kwargs["bbox"]
            # ✅ bbox : [south, north, west, east] → convertir pour POWER
            power_bbox = self._convert_bbox(bbox)
            
            # ✅ Convertir les variables en format POWER
            power_vars = PowerDownloader._get_power_variables(variables)
            
            requests_list = self._build_requests_box(power_vars, variables, start_date, end_date, bbox)
            
            if not requests_list:
                self.logger.info("Tous les fichiers NetCDF existent déjà.")
                return
            
            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = [
                    executor.submit(self._fetch_and_save_netcdf, url, params, path, target_var)
                    for url, params, path, target_var in requests_list
                ]
                for f in tqdm(futures, desc="Downloading NASA POWER regional data"):
                    try:
                        f.result()
                    except Exception as e:
                        self.logger.error(f"Failed to fetch data — {e}")
            return

        else:
            raise ValueError("Either 'points' or 'bbox' must be provided.")
        
    def _fetch_power_point(self, lat, lon, start, end, power_vars, target_vars):
        """Télécharge les données pour un point."""
        try:
            params = {
                "parameters": ",".join(power_vars),
                "community": "AG",
                "longitude": lon,
                "latitude": lat,
                "start": start,
                "end": end,
                "format": "JSON"
            }
            self.logger.debug(f"Fetching POWER data for ({lat}, {lon})")

            response = requests.get(self.BASE_URL_POINT, params=params)
            response.raise_for_status()
            
            records = response.json()['properties']['parameter']
            
            # ✅ Convertir en DataFrame avec renommage
            df = self._json_to_dataframe(records, power_vars, target_vars)
            df["lat"] = lat
            df["lon"] = lon
            
            return df

        except Exception as e:
            self.logger.error(f"Failed for ({lat}, {lon}): {e}")
            return None
    
    def _build_requests_box(self, power_vars, target_vars, start_date, end_date, bbox):
        """Construit les requêtes pour une zone."""
        start_date = pd.to_datetime(start_date)
        end_date = pd.to_datetime(end_date)
        requests_list = []
        
        # ✅ Extraire les composants du bbox standard
        south, north, west, east = bbox

        for i, source_var in enumerate(power_vars):
            target_var = target_vars[i]
            var_dir = self.output_dir / target_var
            var_dir.mkdir(parents=True, exist_ok=True)
            
            for year in range(start_date.year, end_date.year + 1):
                start = max(start_date, pd.Timestamp(f"{year}-01-01"))
                end = min(end_date, pd.Timestamp(f"{year}-12-31"))

                bbox_str = f"_bbox_{south}_{north}_{west}_{east}"
                save_path = var_dir / f"power_{target_var}_{year}{bbox_str}.nc"
                
                if save_path.exists():
                    self.logger.info(f"⏩ Skipping {save_path.name}, already exists.")
                    continue

                # ✅ Requête avec les bons paramètres et le bon ordre
                params = {
                    "latitude-min": south,
                    "latitude-max": north,
                    "longitude-min": west,
                    "longitude-max": east,
                    "parameters": source_var,
                    "community": "AG",
                    "start": start.strftime("%Y%m%d"),
                    "end": end.strftime("%Y%m%d"),
                    "format": "netcdf"
                }
                
                requests_list.append((self.BASE_URL_REGIONAL, params, save_path, target_var))
        
        return requests_list
    
    
    def _fetch_and_save_netcdf(self, base_url, params, save_path, target_var):
        """Télécharge et sauvegarde un NetCDF avec standardisation."""
        save_path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = save_path.with_suffix(".tmp")

        try:
            self.logger.info(f"⬇ Downloading {save_path.name}")
            response = requests.get(base_url, params=params)
            response.raise_for_status()

            with open(temp_path, "wb") as f:
                f.write(response.content)

            ds = xr.open_dataset(temp_path)
            
            # ✅ Renommer la variable en PR (ou target_var)
            var_name = list(ds.data_vars)[0] if len(ds.data_vars) == 1 else None
            if var_name and var_name != target_var:
                ds = ds.rename({var_name: target_var})
                self.logger.info(f"📝 Variable renommée : {var_name} → {target_var}")
            
            # ✅ Ajouter les métadonnées CF
            for coord in ds.coords:
                if coord in ['lon', 'longitude']:
                    ds[coord].attrs['standard_name'] = 'longitude'
                    ds[coord].attrs['units'] = 'degrees_east'
                elif coord in ['lat', 'latitude']:
                    ds[coord].attrs['standard_name'] = 'latitude'
                    ds[coord].attrs['units'] = 'degrees_north'
            
            if 'time' in ds.coords:
                ds.time.attrs['standard_name'] = 'time'
            
            ds.attrs["product"] = "NASA POWER"
            ds.attrs["source"] = "NASA POWER API"
            ds.attrs["description"] = f"NASA POWER {target_var}"
            
            # ✅ Chunking pour l'écriture
            encoding = {target_var: {"zlib": True, "complevel": 4, "chunksizes": (1, 100, 100)}}
            ds.to_netcdf(save_path, encoding=encoding)
            self.logger.info(f"💾 Saved: {save_path}")
            
            temp_path.unlink()
            return save_path

        except Exception as e:
            self.logger.error(f"❌ Failed for {save_path}: {e}")
            if temp_path.exists():
                temp_path.unlink()
            return None

    def extract(self, variables=None, start_date=None, end_date=None, as_long=False, **kwargs):
        
        if self.data is None:
            raise ValueError("No data available. Run download() first.")

        df = self.data.copy()
        df["Date"] = pd.to_datetime(df["Date"])

        if start_date:
            df = df[df["Date"] >= pd.to_datetime(start_date)]
        if end_date:
            df = df[df["Date"] <= pd.to_datetime(end_date)]

        if variables:
            cols = ["Date", "lat", "lon"] + [v for v in variables if v in df.columns]
            df = df[cols]

        if as_long:
            df = df.melt(id_vars=["Date", "lat", "lon"], var_name="variable", value_name="value")

        return df

    @staticmethod
    def _json_to_dataframe(records, source_vars, target_vars):
        """
        Convertit les données JSON en DataFrame avec renommage.
        
        Parameters
        ----------
        records : dict
            Données JSON de l'API POWER
        source_vars : list
            Noms des variables dans POWER
        target_vars : list
            Noms des variables cibles (renommage)
        
        Returns
        -------
        pandas.DataFrame
            DataFrame avec les colonnes renommées
        """
        df = pd.DataFrame()
        for i, var in enumerate(source_vars):
            if var in records:
                series = pd.Series(records[var]).rename(target_vars[i])
                df = pd.concat([df, series], axis=1)
        df.index.name = "Date"
        df.reset_index(inplace=True)
        return df

    @staticmethod
    def _convert_bbox_to_power(bbox):
        """
        Convertit le format standard [south, north, west, east] 
        vers le format POWER [min_lon, min_lat, max_lon, max_lat]
        
        Parameters
        ----------
        bbox : list [south, north, west, east]
        
        Returns
        -------
        list [min_lon, min_lat, max_lon, max_lat]
        """
        south, north, west, east = bbox
        return [west, south, east, north]

    @staticmethod
    def _get_power_variable(target_var):
        """
        Retourne le nom POWER pour une variable cible.
        
        Parameters
        ----------
        target_var : str
            Nom de la variable cible (ex: "PR")
        
        Returns
        -------
        str
            Nom de la variable dans POWER (ex: "PRECTOTCORR")
        """
        mapping = {
            "PR": "PRECTOTCORR",
            "T2M": "T2M",
            "RH2M": "RH2M",
            "WS2M": "WS2M",
            "RSDS": "ALLSKY_SFC_SW_DWN",
            "T2M_MAX": "T2M_MAX",
            "T2M_MIN": "T2M_MIN",
        }
        return mapping.get(target_var, target_var)

    @staticmethod
    def _get_power_variables(target_vars):
        """
        Convertit une liste de variables cibles en noms POWER.
        
        Parameters
        ----------
        target_vars : list
            Liste des variables cibles
        
        Returns
        -------
        list
            Liste des variables POWER correspondantes
        """
        return [PowerDownloader._get_power_variable(v) for v in target_vars]

    def _convert_bbox(self, bbox):
        """Convertit le bbox standard en format POWER."""
        return self._convert_bbox_to_power(bbox)