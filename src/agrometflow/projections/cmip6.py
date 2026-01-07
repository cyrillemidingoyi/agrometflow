import os
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import xarray as xr
from pyesgf.search import SearchConnection
from agrometflow.utils import get_logger, extract_points_from_tuples, dataset_points_to_dataframe
import re
import numpy as np
import pandas as pd

#ESGF_URL = "https://esgf-node.llnl.gov/esg-search"
ESGF_URL ='https://esgf-data.dkrz.de/esg-search'
class CMIP6Downloader:
    
    def __init__(
        self,
        log_file=None,
        verbose=False,
    ):
        self.logger = get_logger("agrometflow.cmip6", log_file=log_file, verbose=verbose)


    def search(self, variable, scenario, model):
        self.logger.info(f" Searching: var={variable}, model={model}, scenario={scenario}")
        conn = SearchConnection(ESGF_URL, distrib=True)
        ctx = conn.new_context(
            project="CMIP6",
            data_node= 'esgf3.dkrz.de',
            source_id=model, 
            experiment_id=scenario,
            variable_id=variable,
            frequency="day"#,
            #latest=True,
            #replica=False,
        )
        results = ctx.search()
        self.logger.info(f" Found {len(results)} datasets for variable {variable}, model {model}, scenario {scenario}")
        return results

    def _download_file(self, url, dest, start_year=None):
        if start_year and not url_matches_start_year(url, start_year):
            self.logger.info(f"[SKIPPING] Skipping file {url} (before {start_year})")
            return

        if dest.exists():
            self.logger.debug(f" Already exists: {dest.name}")
            return dest
        try:
            cmd = f"wget --no-check-certificate -q -O {dest} '{url}'"
            os.system(cmd)
            self.logger.info(f" Downloaded: {dest.name}")
            return dest
        except Exception as e:
            self.logger.error(f" Failed to download {url}: {e}")
            return None

    def _correct_and_subset(self, path, bbox, points, output_dir, variable):
        try:
            self.logger.info(f"[INFO] Correcting and subsetting {path.name}")
            ds = xr.open_dataset(path) #, chunks=10
            self.logger.info(f"[INFO] Opened {path.name} with dimensions: {ds.dims}")
            # Correct longitude if necessary
            if ds.lon.max() > 180:
                ds = change_lon(ds) #ds.assign_coords(lon=(((ds.lon + 180) % 360) - 180))
                #ds = ds.sortby("lon")
                self.logger.info(f"[INFO] Corrected longitude for {path.name}")
            # Clip dataset if bbox is given
            print("iiiiiiiiiiiiiiiiiiiiiiiiiiii", "time" in ds.variables, "lon" in ds.variables)
            if bbox:
                self.logger.info(f"[INFO] Clipping process {path.name} to bbox: {bbox}")
                lat_min, lat_max, lon_min, lon_max = bbox[1], bbox[3], bbox[0], bbox[2]
                ds = ds.sel(
                    lat=slice(lat_min, lat_max),
                    lon=slice(lon_min, lon_max)
                )
                self.logger.info(f"[INFO] Clipped dataset {path.name} to bbox: {bbox}")
            elif points:
                self.logger.info(f"[INFO] Subsetting process {path.name} to points: {points}")
                ds_pts = extract_points_from_tuples(ds, points)   # ✅ NEW
                self.export_points_csv_by_year(ds_pts, path, output_dir, variable)  # ✅ NEW
                os.remove(path)  # on supprime le nc d’origine (comme tu fais déjà)
                self.logger.info(f"[INFO] Subsetted dataset {path.name} to points: {points}")
                return
                
            # Overwrite clipped version
            self.split_netcdf_by_year(ds, path, output_dir)
            #ds.to_netcdf(path)
            self.logger.debug(f"[CLIP] Applied clipping & lon correction to {path.name}")
        except Exception as e:
            self.logger.error(f"[ERROR] Failed to clip/correct {path.name}: {e}")

    def download(self, **kwargs):
        """
        Télécharge plusieurs variables CMIP6, les range par variable,
        fusionne les fichiers NetCDF par année.
        """
        """
        Télécharge des données Chirps par bbox et les enregistre en NetCDF, par année.

        Parameters
        ----------
        start_date : str (YYYY-MM-DD)
        end_date : str (YYYY-MM-DD)
        variables : [str]              # Only rainfall
        output_dir : str
        bbox : list [lon_min, lat_min, lon_max, lat_max]
        kwargs : 
        """
        self.username = kwargs.get("username")
        self.password = kwargs.get("password")
        if not self.username or not self.password:
            raise ValueError("Missing ESGF credentials in config")

        os.environ["ESGF_CREDENTIALS"] = f"{self.username}:{self.password}"

        try:
            models = kwargs.get("models", None)
            experiments = kwargs["scenarios"]
            variables = kwargs["variables"]
            output_dir = kwargs["output_dir"]
            bbox = kwargs.get("bbox", None)
            points = kwargs.get("points", None)
            start_year = kwargs.get("start", None)
            if bbox and points:
                raise ValueError("Choose either 'bbox' or 'points', not both.")
        except KeyError as e:
            raise ValueError(f"Missing required argument: {e}")
        
        all_downloaded = []
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        totmodels = list_of_models(experiments, variables, self.logger)
        
        nwmodels = set(models).intersection(totmodels) if models else totmodels
        self.logger.info(f"Models to download: {nwmodels}")
        
        for model in nwmodels: 
            for scenario in experiments:
                scenario_dir = os.path.join(output_dir, model, scenario)
                Path(scenario_dir).mkdir(parents=True, exist_ok=True)
                tmp_dir = os.path.join(scenario_dir, "_tmp_vars")   # temporaires par variable
                Path(tmp_dir).mkdir(parents=True, exist_ok=True)
                for variable in variables:
                    self.logger.info(f"Modèle : {model},Scenario : {scenario},  Variable : {variable}")
                    var_dir = os.path.join(tmp_dir, variable)
                    Path(var_dir).mkdir(parents=True, exist_ok=True)
                    results = self.search(variable, scenario, model)
                    self._download_files(results, var_dir, bbox, points, start_year, variable)
                    #self._merge_by_year(files, variable, var_dir)
                    #all_downloaded.extend(files)
        if points:
            self.merge_points_csvs_by_year(tmp_dir, scenario_dir, variables)

        import shutil
        #shutil.rmtree(tmp_dir, ignore_errors=True)
        return all_downloaded

    def _extract_url_and_path(self, file, output_dir):
        try:
            url = file.download_url
            filename = url.split("/")[-1]
            dest = Path(output_dir) / filename
            return url, dest
        except Exception as e:
            self.logger.error(f"[ERROR] Failed to extract URL: {e}")
            return None
        
                
    def _download_files(self, results, output_dir, bbox, points, start_year, variable):
        urls = []
        max_workers=1
        try:
            files = results[0].file_context().search()
            self.logger.info(f" Found {len(files)} files for this variable.")
            
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = [executor.submit(self._extract_url_and_path, file, output_dir) for file in files]
                for future in as_completed(futures):
                    result = future.result()
                    if result:
                        urls.append(result)
            self.logger.info(f"[INFO] Extracted {len(urls)} URLs.")
            
            downloaded = []
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {executor.submit(self._download_file, url, dest, start_year): dest for url, dest in urls}
                for future in as_completed(futures):
                    result = future.result()
                    downloaded.append(result)
            self.logger.info(f" {len(downloaded)} fichiers téléchargés pour cette variable.")

            # Now apply post-processing in parallel: clip + correct lon
            self.logger.info(f"[INFO] Applying spatial clipping and lon correction...")
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                executor.map(lambda f: self._correct_and_subset(f, bbox, points, output_dir, variable), downloaded)  
                    
        except Exception as e:
            self.logger.error(f"[ERROR] General failure: {e}")

    def split_netcdf_by_year(self, ds, nc_path, out_dir):
        nc_path = Path(nc_path)
        match = re.search(r"(\d{8})-(\d{8})", nc_path.name)
        if not match:
            raise ValueError(f"Cannot extract date range from filename: {nc_path.name}")

        start_date, end_date = match.groups()
        start_year = int(start_date[:4])
        end_year = int(end_date[:4])

        base_name = nc_path.name.replace(f"{start_date}-{end_date}", "").rstrip("_-")
        base_name = base_name.replace(".nc", "")

        if start_year == end_year:
            out_path = Path(out_dir) / f"{base_name}_{start_year}.nc"
            ds.to_netcdf(out_path)
            os.remove(nc_path)
            return out_path

        output_paths = []
        for year in range(start_year, end_year + 1):
            ds_year = ds.sel(time=ds.time.dt.year == year)
            if ds_year.time.size > 0:
                out_path = Path(out_dir) / f"{base_name}_{year}.nc"
                ds_year.to_netcdf(out_path)
                output_paths.append(out_path)
        os.remove(nc_path)

    def export_points_csv_by_year(self, ds_pts: xr.Dataset, nc_path, out_dir, variable):
        """
        Exporte 1 CSV par année
        Colonnes: time, lon, lat, <variables>
        Basé UNIQUEMENT sur ds.time
        """
        if "time" not in ds_pts.coords:
            raise ValueError("Dataset has no 'time' coordinate; cannot export CSV.")

        #ds_pts = ds_pts[[variable]].squeeze(drop=True)
        
        base = Path(nc_path).stem  # nom du fichier sans .nc
        years = np.unique(ds_pts.time.dt.year.values)

        for year in years:
            ds_y = ds_pts.sel(time=ds_pts.time.dt.year == int(year))
            if ds_y.time.size == 0:
                continue

            df = ds_y.to_dataframe().reset_index()

            # time YYYYMMDD
            df["time"] = pd.to_datetime(df["time"]).dt.strftime("%Y%m%d")
            
            #self.logger.info(f"tableauuuuuuuuu {df}")

            # colonnes strictes
            df = df[["time", "lon", "lat", variable]]

            out_path = Path(out_dir) / f"{base}_points_{int(year)}.csv"
            df.to_csv(out_path, index=False)

            '''self.logger.info(
                f"[CSV] {out_path.name} | "
                f"{len(df)} rows | "
                f"{len(np.unique(df[['lon','lat']].values, axis=0))} points"
            )'''

    def merge_points_csvs_by_year(self, tmp_dir, scenario_dir, variables):
        from pathlib import Path
        tmp_dir = Path(tmp_dir)
        scenario_dir = Path(scenario_dir)
        scenario_dir.mkdir(parents=True, exist_ok=True)

        # indexer les fichiers par "clé commune" = base_points_YYYY.csv
        files_by_key = {}

        for var in variables:
            for p in Path(os.path.join(tmp_dir, var)).glob(f"*_.csv"):
                # get the year frome the filename terminated by year.csv
                match = re.search(r"_(\d{4})\.csv$", p.name)
                if not match:
                    continue
                key = match.group(1)
                files_by_key.setdefault(key, []).append(p)

        for key, paths in files_by_key.items():
            df_merged = None

            for p in paths:
                dfv = pd.read_csv(p)

                if df_merged is None:
                    df_merged = dfv
                else:
                    df_merged = df_merged.merge(
                        dfv,
                        on=["time", "lon", "lat"],
                        how="outer",
                    )

            if df_merged is None or df_merged.empty:
                continue

            # ordre final strict
            cols = ["time", "lon", "lat"] + [v for v in variables if v in df_merged.columns]
            df_merged = df_merged[cols]

            out_path = scenario_dir / key
            df_merged.to_csv(out_path, index=False)

            self.logger.info(f"[CSV final] {out_path.name}")
        
def url_matches_start_year(url, start_year):
    match = re.search(r'(\d{8})-(\d{8})\.nc', url)
    if not match:
        return False
    start_date = match.group(1)
    year = int(start_date[:4])
    return year >= start_year

def list_of_models(experiments, variables, logger):
    logger.info(f" Searching: list of all models that satisfy the rest of arguments")
    conn = SearchConnection(ESGF_URL, distrib=True)
    # Dictionary to store models per (experiment, variable)
    models_per_exp_var = {}

    for exp in experiments:
        models_per_exp_var[exp] = {}
        for var in variables:
            ctx = conn.new_context(
                project="CMIP6",
                experiment_id=exp,
                variable_id=var,
                frequency="day",
                data_node= 'esgf3.dkrz.de' #"esgf3.dkrz.de"  #'esgf.ceda.ac.uk'
            )
            results = ctx.search()
            # Collect unique source_ids (model names)
            models = set()
            for result in results:
                source_ids = result.json.get("source_id", [])
                if isinstance(source_ids, list):
                    models.update(source_ids)
                else:
                    models.add(source_ids)
            models_per_exp_var[exp][var] = models
            logger.info(f"{len(models)} models for {var} in {exp}")
    # Find common models across all variables and experiments
    models_with_all = get_common_models(models_per_exp_var)
    # Print the final list of models
    logger.info(f"\n CMIP6 Models with daily {','.join(variables)} for all scenario: {models_with_all}")
    return models_with_all

from functools import reduce
# Suppose Y is your dictionary: Y[experiment][variable] = list of models
# We want to find models common to all Y[e][v]
def get_common_models(Y):
    all_model_sets = []
    for exp_dict in Y.values():  # loop over each experiment
        for model_list in exp_dict.values():  # loop over each variable
            all_model_sets.append(set(model_list))
    if not all_model_sets:
        return set()
    return reduce(set.intersection, all_model_sets)

def change_lon(df):
    # move lon from 0<>360 to -180<>180
    df = df.assign_coords(lon=(((df.lon + 180) % 360) - 180)).sortby('lon')
    return df

def clip_data(df, bbox):
    df = df.sel(lon=slice(bbox[0],bbox[2]),lat=slice(bbox[1],bbox[3]))
    return df

def change_precip_units(df):
    if 'pr' in df.variables:
        df['pr'] = df['pr'] * 86400  # Convert from kg/m²/s to mm/day
        df['pr'].attrs['units'] = 'mm/day'
    return df

def convert_temp_units(df):
    if 'tas' in df.variables:
        df['tas'] = df['tas'] - 273.15  # Convert from Kelvin to Celsius
        df['tas'].attrs['units'] = '°C'
    return df


