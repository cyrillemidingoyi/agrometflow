import os
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import xarray as xr
from pyesgf.search import SearchConnection
from agrometflow.utils import get_logger, extract_points_from_tuples, dataset_points_to_dataframe
import re
import numpy as np
import pandas as pd
from pandas._libs.tslibs.np_datetime import OutOfBoundsDatetime
from typing import Optional, List, Tuple, Dict, Union
import cftime

#ESGF_URL = "https://esgf-node.llnl.gov/esg-search"
ESGF_URL = 'https://esgf-data.dkrz.de/esg-search'

# Output format options
OUTPUT_FORMAT_BY_YEAR = "by_year"          # Un fichier CSV par année (comportement actuel)
OUTPUT_FORMAT_BY_STATION = "by_station"    # Un fichier CSV par station avec toutes les années

# Models known to use standard/gregorian calendar (365/366 days)
# This list can be extended based on CMIP6 documentation
GREGORIAN_CALENDAR_MODELS = {
    "IPSL-CM6A-LR", "IPSL-CM5A2-INCA", "IPSL-CM6A-LR-INCA",
    "CNRM-CM6-1", "CNRM-CM6-1-HR", "CNRM-ESM2-1",
    "EC-Earth3", "EC-Earth3-Veg", "EC-Earth3-Veg-LR", "EC-Earth3-CC", "EC-Earth3-AerChem",
    "MPI-ESM1-2-HR", "MPI-ESM1-2-LR", "MPI-ESM-1-2-HAM",
    "GFDL-CM4", "GFDL-ESM4",
    "CanESM5", "MIROC6", "MIROC-ES2L", 
    "ACCESS-CM2", "ACCESS-ESM1-5"
}

# Models known to use 360-day calendar (avoid these for standard analysis)
NON_GREGORIAN_CALENDAR_MODELS = {
    "FGOALS-g3", "FGOALS-f3-L",  # 365_day (noleap)
    "CESM2", "CESM2-WACCM", "CESM2-FV2", "CESM2-WACCM-FV2",  # 365_day (noleap)
    "NorESM2-LM", "NorESM2-MM",  # 365_day (noleap)
    "KACE-1-0-G", "UKESM1-0-LL",  # 360_day
    "HadGEM3-GC31-LL", "HadGEM3-GC31-MM",  # 360_day
    "NESM3","BCC-CSM2-MR", "BCC-ESM1",  # 365_day
}


def cftime_to_datetime(time_values):
    """
    Convert cftime objects (DatetimeNoLeap, Datetime360Day, etc.) to pandas datetime.
    Handles non-standard calendars by converting to string first.
    For dates beyond pandas datetime64[ns] range (~1678-2262), returns string dates.
    """
    if len(time_values) == 0:
        return pd.Series(dtype='datetime64[ns]')
    
    # Check if it's already a standard datetime
    first_val = time_values.iloc[0] if hasattr(time_values, 'iloc') else time_values[0]
    
    if isinstance(first_val, (cftime.DatetimeNoLeap, cftime.DatetimeAllLeap, 
                               cftime.Datetime360Day, cftime.DatetimeJulian,
                               cftime.DatetimeGregorian, cftime.DatetimeProlepticGregorian)):
        # Convert cftime to string first
        date_strings = [t.strftime("%Y-%m-%d") for t in time_values]
        
        # Check if dates are within pandas datetime64[ns] range (roughly 1678-2262)
        # by checking the first and last dates
        first_year = time_values.iloc[0].year if hasattr(time_values, 'iloc') else time_values[0].year
        last_year = time_values.iloc[-1].year if hasattr(time_values, 'iloc') else time_values[-1].year
        
        if first_year > 2262 or last_year > 2262:
            # Return as string series for far-future dates
            return pd.Series(date_strings)
        else:
            # Convert to datetime for dates within range
            return pd.to_datetime(date_strings)
    else:
        # Standard datetime, use pandas directly
        try:
            return pd.to_datetime(time_values)
        except (ValueError, OutOfBoundsDatetime):
            # Fallback to string for out-of-range dates
            return pd.Series([str(t) for t in time_values])


class CMIP6Downloader:
    
    def __init__(
        self,
        log_file=None,
        verbose=False,
    ):
        self.logger = get_logger("agrometflow.cmip6", log_file=log_file, verbose=verbose)
        self.station_data_cache: Dict[Tuple[float, float], List[pd.DataFrame]] = {}  # Cache pour les données par station


    def search(self, variable, scenario, model, member_id=None):
        self.logger.info(f" Searching: var={variable}, model={model}, scenario={scenario}, member={member_id or 'any'}")
        conn = SearchConnection(ESGF_URL, distrib=True)
        search_params = {
            "project": "CMIP6",
            "data_node": 'esgf3.dkrz.de',
            "source_id": model,
            "experiment_id": scenario,
            "variable_id": variable,
            "frequency": "day",
            "latest": True  # Explicitly request only the latest version to avoid ESGF warning
        }
        if member_id:
            search_params["variant_label"] = member_id
            
        ctx = conn.new_context(**search_params)
        results = ctx.search()
        self.logger.info(f" Found {len(results)} datasets for variable {variable}, model {model}, scenario {scenario}, member {member_id}.")
        return results

    def get_available_members(self, variable, scenario, model):
        """Get all available ensemble members for a variable/scenario/model combination."""
        conn = SearchConnection(ESGF_URL, distrib=True)
        ctx = conn.new_context(
            project="CMIP6",
            data_node='esgf3.dkrz.de',
            source_id=model,
            experiment_id=scenario,
            variable_id=variable,
            frequency="day",
            latest=True  # Explicitly request only the latest version
        )
        results = ctx.search()
        members = set()
        for result in results:
            variant_labels = result.json.get("variant_label", [])
            if isinstance(variant_labels, list):
                members.update(variant_labels)
            else:
                members.add(variant_labels)
        return members

    def find_common_member(self, variables, scenario, model):
        """
        Find a common ensemble member available for all variables in a model/scenario.
        Returns the first common member found, prioritizing r1i1p1f1 if available.
        """
        self.logger.info(f"Finding common member for model={model}, scenario={scenario}, variables={variables}")
        
        members_per_var = {}
        for var in variables:
            members = self.get_available_members(var, scenario, model)
            members_per_var[var] = members
            self.logger.info(f"  Variable {var}: {len(members)} members available")
        
        # Find intersection of all members
        if not members_per_var:
            return None
            
        common_members = set.intersection(*members_per_var.values()) if len(members_per_var) > 1 else list(members_per_var.values())[0]
        
        if not common_members:
            self.logger.warning(f"No common member found for model {model}, scenario {scenario}")
            return None
        
        self.logger.info(f"Common members for {model}/{scenario}: {common_members}")
        
        # Prioritize common members: r1i1p1f1 > r1i1p1f2 > others sorted
        priority_members = ["r1i1p1f1", "r1i1p1f2", "r1i1p1f3", "r2i1p1f1", "r3i1p1f1"]
        for member in priority_members:
            if member in common_members:
                self.logger.info(f"Selected member: {member}")
                return member
        
        # Return first available sorted
        selected = sorted(common_members)[0]
        self.logger.info(f"Selected member: {selected}")
        return selected

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

    def _correct_and_subset(self, path, bbox, points, output_dir, variable, output_format=OUTPUT_FORMAT_BY_YEAR):
        try:
            self.logger.info(f"[INFO] Correcting and subsetting {path.name}")
            ds = xr.open_dataset(path) #, chunks=10
            self.logger.info(f"[INFO] Opened {path.name} with dimensions: {ds.dims}")
            # Correct longitude if necessary
            if ds.lon.max() > 180:
                ds = change_lon(ds) #ds.assign_coords(lon=(((ds.lon + 180) % 360) - 180))
                #ds = ds.sortby("lon")
                self.logger.info(f"[INFO] Corrected longitude for {path.name}")
            
            # Apply conversions
            ds = apply_unit_conversions(ds, variable)
            self.logger.info(f"[INFO] Applied unit conversions for {path.name}, variable: {variable}")
            
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
                ds_pts = extract_points_from_tuples(ds, points)
                
                if output_format == OUTPUT_FORMAT_BY_STATION:
                    self.logger.info(f"[INFO] Caching points data for station export for variable: {variable}")
                    self.cache_points_data_for_station_export(ds_pts, variable)
                else:
                    self.export_points_csv_by_year(ds_pts, path, output_dir, variable)
                
                os.remove(path) 
                self.logger.info(f"[INFO] Subsetted dataset {path.name} to points: {points}")
                return
                
            self.split_netcdf_by_year(ds, path, output_dir)
            ds.to_netcdf(path)
            self.logger.debug(f"[CLIP] Applied clipping & lon correction to {path.name}")
        except Exception as e:
            self.logger.error(f"[ERROR] Failed to clip/correct {path.name}: {e}")

    def download(self, **kwargs):
        """
        Télécharge plusieurs variables CMIP6, les range par variable,
        fusionne les fichiers NetCDF par année ou par station.
        
        Parameters
        ----------
        username : str
            ESGF username
        password : str
            ESGF password
        models : list[str], optional
            List of CMIP6 models to download
        scenarios : list[str]
            List of scenarios (e.g., ['ssp126', 'ssp585'])
        variables : list[str]
            List of variables (e.g., ['tas', 'pr', 'tasmax'])
        output_dir : str
            Output directory path
        bbox : list, optional
            Bounding box [lon_min, lat_min, lon_max, lat_max]
        points : list[tuple], optional
            List of (lon, lat) tuples for point extraction
        start : int, optional
            Start year filter
        calendar : str, optional
            Calendar filter ("gregorian", "standard", "365_day", "360_day")
        output_format : str, optional
            Output format for points data:
            - "by_year" (default): Un fichier CSV par année avec toutes les stations
            - "by_station": Un fichier CSV par station avec toutes les années et variables en colonnes
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
            calendar_filter = kwargs.get("calendar", None)
            output_format = kwargs.get("output_format", OUTPUT_FORMAT_BY_YEAR)
            
            if bbox and points:
                raise ValueError("Choose either 'bbox' or 'points', not both.")
            
            # Validate output_format
            if output_format not in [OUTPUT_FORMAT_BY_YEAR, OUTPUT_FORMAT_BY_STATION]:
                raise ValueError(f"Invalid output_format: {output_format}. Use '{OUTPUT_FORMAT_BY_YEAR}' or '{OUTPUT_FORMAT_BY_STATION}'")
                
        except KeyError as e:
            raise ValueError(f"Missing required argument: {e}")
        
        all_downloaded = []
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        totmodels = list_of_models(experiments, variables, self.logger)
        
        nwmodels = set(models).intersection(totmodels) if models else totmodels
        
        # Filter models by calendar type if requested
        if calendar_filter in ["gregorian", "standard"]:
            gregorian_models = nwmodels.intersection(GREGORIAN_CALENDAR_MODELS)
            excluded = nwmodels - gregorian_models
            if excluded:
                self.logger.info(f"Excluding non-gregorian calendar models: {excluded}")
            nwmodels = gregorian_models
            self.logger.info(f"Models with gregorian calendar: {nwmodels}")
        elif calendar_filter == "365_day":
            self.logger.info(f"Including all models (no 360-day filter)")
        elif calendar_filter == "360_day":
            self.logger.warning("360-day calendar selected - data will have 360 days per year")
        
        self.logger.info(f"Models to download: {nwmodels}")
        
        for model in nwmodels: 
            for scenario in experiments:
                scenario_dir = os.path.join(output_dir, model, scenario)
                Path(scenario_dir).mkdir(parents=True, exist_ok=True)
                tmp_dir = os.path.join(scenario_dir, "_tmp_vars")  
                Path(tmp_dir).mkdir(parents=True, exist_ok=True)
                
                # Reset station cache for each model/scenario combination
                self.station_data_cache = {}
                
                # Find common member for all variables in this model/scenario
                common_member = self.find_common_member(variables, scenario, model)
                if not common_member:
                    self.logger.warning(f"Skipping model {model}, scenario {scenario}: no common member found for all variables")
                    continue
                
                self.logger.info(f"Using member {common_member} for model {model}, scenario {scenario}")
                
                for variable in variables:
                    self.logger.info(f"Modèle : {model}, Scenario : {scenario}, Variable : {variable}, Member : {common_member}")
                    var_dir = os.path.join(tmp_dir, variable)
                    Path(var_dir).mkdir(parents=True, exist_ok=True)
                    results = self.search(variable, scenario, model, member_id=common_member)
                    self._download_files(results, var_dir, bbox, points, start_year, variable, output_format)

                # After all variables downloaded, export based on format
                if points:
                    if output_format == OUTPUT_FORMAT_BY_STATION:
                        self.export_stations_to_csv(scenario_dir, variables, model, scenario)
                    else:
                        self.merge_points_csvs_by_year(tmp_dir, scenario_dir, variables)
                    
                    import shutil
                    shutil.rmtree(tmp_dir, ignore_errors=True)
                    
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
        
                
    def _download_files(self, results, output_dir, bbox, points, start_year, variable, output_format=OUTPUT_FORMAT_BY_YEAR):
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

            self.logger.info(f"[INFO] Applying spatial clipping and lon correction...")
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                executor.map(lambda f: self._correct_and_subset(f, bbox, points, output_dir, variable, output_format), downloaded)  
                    
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
        
        base = Path(nc_path).stem 
        years = np.unique(ds_pts.time.dt.year.values)
        
        ds_pts = ds_pts.drop_vars("time_bounds", errors="ignore")
        ds_pts["time"].attrs.pop("bounds", None)
        ds_pts = ds_pts.drop_dims("axis_nbounds", errors="ignore")

        for year in years:
            ds_y = ds_pts.sel(time=ds_pts.time.dt.year == int(year))
            if ds_y.time.size == 0:
                continue

            df = ds_y.to_dataframe().reset_index()

            # time YYYYMMDD - handle cftime calendars (noleap, 360_day, etc.)
            df["time"] = cftime_to_datetime(df["time"]).dt.strftime("%Y%m%d")
            
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

        files_by_key = {}

        for var in variables:
            for p in Path(os.path.join(tmp_dir, var)).glob(f"*.csv"):
                match = re.search(r"_(\d{4})\.csv$", p.name)
                if not match:
                    continue
                key = match.group(1)
                files_by_key.setdefault(key, []).append(p)
        
        self.logger.info(f"Merging CSVs by year for points... {len(files_by_key)} years found.")
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

            cols = ["time", "lon", "lat"] + [v for v in variables if v in df_merged.columns]
            df_merged = df_merged[cols]

            out_path = scenario_dir / key
            df_merged.to_csv(out_path, index=False)

            self.logger.info(f"[CSV final] {out_path.name}")

    def cache_points_data_for_station_export(self, ds_pts: xr.Dataset, variable: str):
        """
        Cache les données extraites pour un export ultérieur par station.
        Accumule les données de toutes les variables pour chaque point (station).
        """
        if "time" not in ds_pts.coords:
            raise ValueError("Dataset has no 'time' coordinate; cannot cache data.")
        
        # Clean dataset
        ds_pts = ds_pts.drop_vars("time_bounds", errors="ignore")
        ds_pts["time"].attrs.pop("bounds", None)
        ds_pts = ds_pts.drop_dims("axis_nbounds", errors="ignore")
        
        # Convert to dataframe
        df = ds_pts.to_dataframe().reset_index()
        # Handle cftime calendars (noleap, 360_day, etc.)
        df["time"] = cftime_to_datetime(df["time"])
        
        # Group by station (lon, lat)
        for (lon, lat), group in df.groupby(["lon", "lat"]):
            station_key = (float(lon), float(lat))
            station_df = group[["time", variable]].copy()
            station_df = station_df.sort_values("time").reset_index(drop=True)
            
            if station_key not in self.station_data_cache:
                self.station_data_cache[station_key] = []
            
            self.station_data_cache[station_key].append(station_df)
        
        self.logger.info(f"[CACHE] Cached variable {variable} for {len(df.groupby(['lon', 'lat']))} stations")

    def export_stations_to_csv(self, output_dir: str, variables: List[str], model: str, scenario: str):
        """
        Exporte les données en un fichier CSV par station.
        Chaque fichier contient toutes les années avec les variables en colonnes.
        
        Format du fichier:
        - Nom: lon_{lon}_lat_{lat}_{model}_{scenario}.csv
        - Colonnes: date, year, month, day, doy, var1, var2, var3, ...
        """
        if not self.station_data_cache:
            self.logger.warning("[EXPORT] No cached data to export")
            return
        
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        exported_count = 0
        
        for station_key, dataframes in self.station_data_cache.items():
            lon, lat = station_key
            
            # Merge all variable dataframes on time
            df_merged = None
            for df in dataframes:
                if df_merged is None:
                    df_merged = df
                else:
                    # Get the variable column name (exclude 'time')
                    new_cols = [c for c in df.columns if c != "time"]
                    # Check which columns already exist in df_merged
                    existing_cols = [c for c in new_cols if c in df_merged.columns]
                    new_only_cols = [c for c in new_cols if c not in df_merged.columns]
                    
                    if existing_cols:
                        # Concatenate data for existing variables (different time periods)
                        df_merged = pd.concat([df_merged, df], ignore_index=True)
                        # Remove duplicates based on time and keep all variable columns
                        df_merged = df_merged.groupby("time", as_index=False).first()
                    elif new_only_cols:
                        # Merge new variables
                        df_merged = df_merged.merge(df[["time"] + new_only_cols], on="time", how="outer")
            
            if df_merged is None or df_merged.empty:
                continue
            
            # Sort by time
            df_merged = df_merged.sort_values("time").reset_index(drop=True)
            
            # Add date components
            df_merged["date"] = df_merged["time"].dt.strftime("%Y%m%d")
            df_merged["year"] = df_merged["time"].dt.year
            df_merged["month"] = df_merged["time"].dt.month
            df_merged["day"] = df_merged["time"].dt.day
            df_merged["doy"] = df_merged["time"].dt.dayofyear
            
            # Reorder columns: date info first, then variables
            date_cols = ["date", "year", "month", "day", "doy"]
            var_cols = [v for v in variables if v in df_merged.columns]
            df_merged = df_merged[date_cols + var_cols]
            
            # Generate filename using coordinates
            lon_str = f"{lon:.2f}".replace(".", "p").replace("-", "m")
            lat_str = f"{lat:.2f}".replace(".", "p").replace("-", "m")
            filename = f"lon_{lon_str}_lat_{lat_str}_{model}_{scenario}.csv"
            
            out_path = output_dir / filename
            df_merged.to_csv(out_path, index=False)
            exported_count += 1
            
            # Log summary
            years = df_merged["year"].unique()
            self.logger.info(
                f"[CSV] {filename} | "
                f"Station: ({lon:.3f}, {lat:.3f}) | "
                f"Years: {years.min()}-{years.max()} | "
                f"Rows: {len(df_merged)} | "
                f"Variables: {', '.join(var_cols)}"
            )
        
        self.logger.info(f"[EXPORT] Exported {exported_count} station files to {output_dir}")
        
        # Clear cache after export
        self.station_data_cache = {}

    def get_station_summary(self, output_dir: str) -> pd.DataFrame:
        """
        Génère un résumé de toutes les stations exportées.
        
        Returns
        -------
        pd.DataFrame
            DataFrame avec colonnes: file, lon, lat, start_year, end_year, n_records, variables
        """
        output_dir = Path(output_dir)
        summaries = []
        
        for csv_file in output_dir.glob("lon_*.csv"):
            try:
                df = pd.read_csv(csv_file)
                if "year" in df.columns and "date" in df.columns:
                    # Parse coordinates from filename pattern: lon_{lon}_lat_{lat}_...
                    match = re.search(r"lon_(m?\d+p\d+)_lat_(m?\d+p\d+)_", csv_file.name)
                    lon, lat = None, None
                    if match:
                        lon_str = match.group(1).replace("m", "-").replace("p", ".")
                        lat_str = match.group(2).replace("m", "-").replace("p", ".")
                        lon, lat = float(lon_str), float(lat_str)
                    
                    var_cols = [c for c in df.columns if c not in ["date", "year", "month", "day", "doy"]]
                    
                    summaries.append({
                        "file": csv_file.name,
                        "lon": lon,
                        "lat": lat,
                        "start_year": int(df["year"].min()),
                        "end_year": int(df["year"].max()),
                        "n_records": len(df),
                        "variables": ", ".join(var_cols)
                    })
            except Exception as e:
                self.logger.warning(f"Could not read {csv_file.name}: {e}")
        
        return pd.DataFrame(summaries)
        
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

def change_precip_units(ds, var):
    if var == "pr":
        ds[var] = ds[var] * 86400  # Convert from kg/m²/s to mm/day
        ds[var].attrs['units'] = 'mm/day'
    return ds

def convert_temp_units(ds, var):
    if var in ["tas", "tasmax", "tasmin"]:
        ds[var] = ds[var] - 273.15  # Convert from Kelvin to Celsius
        ds[var].attrs['units'] = '°C'
    return ds

def convert_wind_to_2meters(ds, var):
    if var in ["sfcWind", "uas", "vas"]:
        ds[var] = ds[var] * 0.75 
        ds[var].attrs['units'] = 'm/s at 2m'
        ds[var].attrs["comment"] = "Scaled to 2m using constant factor 0.75 (approx)."
    return ds


def apply_unit_conversions(ds, var):
    ds = change_precip_units(ds, var)
    ds = convert_temp_units(ds, var)
    ds = convert_wind_to_2meters(ds, var)
    return ds


def get_calendar_type(ds):
    """
    Get the calendar type from an xarray dataset.
    Returns: 'gregorian', 'standard', 'proleptic_gregorian', '365_day', 'noleap', '360_day', or 'unknown'
    """
    if "time" not in ds.coords:
        return "unknown"
    
    # Try to get calendar from encoding
    calendar = ds.time.encoding.get("calendar", None)
    if calendar:
        return calendar.lower()
    
    # Try to get from attrs
    calendar = ds.time.attrs.get("calendar", None)
    if calendar:
        return calendar.lower()
    
    return "unknown"


def is_gregorian_calendar(calendar_type):
    """Check if a calendar type is gregorian (365/366 days with leap years)."""
    gregorian_types = ["gregorian", "standard", "proleptic_gregorian"]
    return calendar_type.lower() in gregorian_types


def check_model_calendar(model_name, variable, scenario, logger=None):
    """
    Check the calendar type of a CMIP6 model by downloading a small sample file.
    Returns the calendar type string.
    """
    # First check if model is in known lists
    if model_name in GREGORIAN_CALENDAR_MODELS:
        return "gregorian"
    if model_name in NON_GREGORIAN_CALENDAR_MODELS:
        return "non_gregorian"
    
    # If not in known lists, would need to download a sample file to check
    # This is expensive, so we return "unknown" and let user decide
    if logger:
        logger.warning(f"Calendar type unknown for model {model_name}. Add to GREGORIAN_CALENDAR_MODELS or NON_GREGORIAN_CALENDAR_MODELS in cmip6.py")
    return "unknown"

