"""
GHCN-Daily (GHCNd) downloader — NOAA Global Historical Climatology Network Daily.

Data source: https://www.ncei.noaa.gov/products/land-based-station/global-historical-climatology-network-daily
FTP mirror : https://www.ncei.noaa.gov/pub/data/ghcn/daily/

Supported access methods
------------------------
1. By station ID(s)  — downloads the per-station CSV from the NOAA HTTP endpoint
2. By bounding box   — searches the station inventory and downloads all matching stations
3. Inventory only    — download/parse the full station metadata table
"""

import io
import time
import requests
import pandas as pd
import numpy as np
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

from agrometflow.climate.base import ClimateSource
from agrometflow.utils import get_logger


# ---------------------------------------------------------------------------
# Public constants
# ---------------------------------------------------------------------------

#: Base URL for per-station CSV files (NOAA HTTPS endpoint)
GHCND_CSV_BASE = "https://www.ncei.noaa.gov/pub/data/ghcn/daily/by_station"

#: Full station inventory (fixed-width text)
GHCND_INVENTORY_URL = "https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-inventory.txt"

#: Station metadata (fixed-width text)
GHCND_STATIONS_URL = "https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt"

#: Standard variables and their descriptions
GHCND_VARIABLES = {
    "TMAX": "Maximum temperature (°C × 10)",
    "TMIN": "Minimum temperature (°C × 10)",
    "PRCP": "Precipitation (mm × 10)",
    "SNOW": "Snowfall (mm)",
    "SNWD": "Snow depth (mm)",
    "TAVG": "Average temperature (°C × 10)",
    "AWND": "Average daily wind speed (m/s × 10)",
    "EVAP": "Evaporation (mm × 10)",
    "WSFG": "Peak gust wind speed (m/s × 10)",
}

# Variables whose raw values are in tenths and need ÷10 conversion
_TENTHS_VARS = {"TMAX", "TMIN", "TAVG", "PRCP", "AWND", "EVAP", "WSFG"}

# Fixed-width column specs for the raw dly file (per NOAA readme.txt)
_DLY_COLSPECS = (
    [(0, 11), (11, 15), (15, 17), (17, 21)]
    + [(21 + i * 8, 26 + i * 8) for i in range(31)]   # value
    + [(26 + i * 8, 27 + i * 8) for i in range(31)]   # mflag
    + [(27 + i * 8, 28 + i * 8) for i in range(31)]   # qflag
    + [(28 + i * 8, 29 + i * 8) for i in range(31)]   # sflag
)

# ---------------------------------------------------------------------------
# Downloader class
# ---------------------------------------------------------------------------

class GHCNDDownloader(ClimateSource):
    """
    Downloader for NOAA GHCN-Daily station data.

    Parameters
    ----------
    log_file : str or Path, optional
        Path to a log file. Logs to console only if not set.
    verbose : bool
        Enable DEBUG-level logging.

    Examples
    --------
    >>> from agrometflow.climate.ghcnd import GHCNDDownloader
    >>> dl = GHCNDDownloader()

    # Download by station ID
    >>> dl.download(station_ids=["AGM00060360"], start_date="2000-01-01",
    ...             end_date="2020-12-31", output_dir="data/ghcnd",
    ...             variables=["TMAX", "TMIN", "PRCP"])

    # Download all stations inside a bounding box
    >>> dl.download(bbox=(-1.5, 12.0, -0.5, 13.0),
    ...             start_date="2000-01-01", end_date="2020-12-31",
    ...             output_dir="data/ghcnd", variables=["TMAX", "TMIN", "PRCP"])
    """

    def __init__(self, log_file=None, verbose=False):
        self.logger = get_logger(__name__, log_file=log_file, verbose=verbose)
        self.data: pd.DataFrame | None = None
        self._stations: pd.DataFrame | None = None

    # ------------------------------------------------------------------
    # ClimateSource interface
    # ------------------------------------------------------------------

    def download(self, **kwargs):
        """
        Download GHCN-Daily data.

        Parameters
        ----------
        station_ids : list of str, optional
            List of GHCN station IDs (e.g. ``["AGM00060360"]``).
            Mutually exclusive with ``bbox``.
        bbox : tuple (min_lon, min_lat, max_lon, max_lat), optional
            Bounding box to search for stations.
            Mutually exclusive with ``station_ids``.
        start_date : str
            Start date ``YYYY-MM-DD``.
        end_date : str
            End date ``YYYY-MM-DD``.
        variables : list of str
            Variable codes, e.g. ``["TMAX", "TMIN", "PRCP"]``.
        output_dir : str or Path
            Directory where merged CSV is saved.
        max_workers : int, optional
            Number of parallel download threads (default: 8).
        min_years : int, optional
            When using ``bbox``: skip stations with fewer than this many
            years of data for **any** of the requested variables.
        convert_units : bool, optional
            If True (default), divide tenths-unit variables by 10 and
            replace missing values (-9999) with NaN.
        """
        try:
            start_date = kwargs["start_date"]
            end_date = kwargs["end_date"]
            variables = [v.upper() for v in kwargs["variables"]]
            output_dir = Path(kwargs["output_dir"])
        except KeyError as e:
            raise ValueError(f"Missing required argument: {e}")

        max_workers = kwargs.get("max_workers", 8)
        convert = kwargs.get("convert_units", True)
        output_dir.mkdir(parents=True, exist_ok=True)

        # -- resolve station list ----------------------------------------
        if "station_ids" in kwargs:
            station_ids = kwargs["station_ids"]
        elif "bbox" in kwargs:
            min_years = kwargs.get("min_years", 0)
            station_ids = self._stations_in_bbox(
                bbox=kwargs["bbox"],
                variables=variables,
                start_year=int(start_date[:4]),
                end_year=int(end_date[:4]),
                min_years=min_years,
            )
            self.logger.info(f"Found {len(station_ids)} stations in bounding box")
        else:
            raise ValueError("Provide either 'station_ids' or 'bbox'")

        if not station_ids:
            self.logger.warning("No stations found — nothing to download.")
            return

        stations_df = self.get_stations()
        metadata_cols = ["station_id", "lat", "lon", "elevation"]
        station_meta = (
            stations_df[stations_df["station_id"].isin(station_ids)][metadata_cols]
            .drop_duplicates("station_id")
            .set_index("station_id")
        )
        station_meta_map = station_meta.to_dict(orient="index")

        # -- download in parallel ----------------------------------------
        all_frames = []
        failed = []

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_sid = {
                executor.submit(
                    _fetch_station_csv,
                    sid, start_date, end_date, variables, convert, self.logger, station_meta_map.get(sid)
                ): sid
                for sid in station_ids
            }
            for future in tqdm(
                as_completed(future_to_sid),
                total=len(future_to_sid),
                desc="Downloading GHCN-Daily"
            ):
                sid = future_to_sid[future]
                try:
                    df = future.result()
                    if df is not None and not df.empty:
                        all_frames.append(df)
                except Exception as exc:
                    self.logger.warning(f"Station {sid} failed: {exc}")
                    failed.append(sid)

        if failed:
            self.logger.warning(f"{len(failed)} station(s) failed: {failed}")

        if not all_frames:
            self.logger.error("No data downloaded.")
            return

        merged = pd.concat(all_frames, ignore_index=True)
        self.data = merged

        # Save merged CSV
        start_tag = start_date.replace("-", "")
        end_tag = end_date.replace("-", "")
        out_file = output_dir / f"ghcnd_{start_tag}_{end_tag}.csv"
        merged.to_csv(out_file, index=False)
        self.logger.info(f"Saved {len(merged)} rows → {out_file}")

    def extract(
        self,
        variables=None,
        start_date=None,
        end_date=None,
        as_long=False,
        qc_format=False,
        **kwargs,
    ):
        """
        Return a filtered copy of the downloaded data.

        Parameters
        ----------
        variables : list of str, optional
            Variables to keep. If None, all are returned.
        start_date, end_date : str, optional
            Filter date range (``YYYY-MM-DD``).
        as_long : bool
            If True, melt to long format with a ``variable`` column.
        qc_format : bool
            If True, prepare data for quality control by adding Year, Month, Day
            columns and standardizing variable names (TMAX→Tx, TMIN→Tn, etc.).
            Requires at least one variable in ``variables``.
        source : str or Path, optional
            Path to an existing CSV to load instead of using in-memory data.
        station_ids : list of str, optional
            Keep only these station IDs.
        """
        source = kwargs.get("source")
        if self.data is None and source is None:
            raise ValueError("No data available. Run download() first or pass source=<path>.")

        if source is not None:
            df = pd.read_csv(source, parse_dates=["date"])
        else:
            df = self.data.copy()
            df["date"] = pd.to_datetime(df["date"])

        if start_date:
            df = df[df["date"] >= pd.to_datetime(start_date)]
        if end_date:
            df = df[df["date"] <= pd.to_datetime(end_date)]

        if "station_ids" in kwargs:
            df = df[df["station"].isin(kwargs["station_ids"])]

        if variables:
            keep = ["date", "station", "lat", "lon", "elevation"]
            keep += [v for v in variables if v in df.columns]
            df = df[[c for c in keep if c in df.columns]]

        if as_long:
            id_vars = [c for c in ["date", "station", "lat", "lon", "elevation"] if c in df.columns]
            df = df.melt(id_vars=id_vars, var_name="variable", value_name="value")

        # Prepare for QC: add date parts (Year, Month, Day) and standardize variable names
        if qc_format:
            if not variables:
                raise ValueError("qc_format requires at least one variable in 'variables' parameter")
            
            # Add date parts
            df["Year"] = df["date"].dt.year
            df["Month"] = df["date"].dt.month
            df["Day"] = df["date"].dt.day
            
            # Standardize variable name mapping
            var_mapping = {
                "TMAX": "Tx",
                "TMIN": "Tn",
                "TAVG": "Ta",
                "PRCP": "rr",
                "SNOW": "sd",
                "SNWD": "fs",
                "AWND": "w",
            }
            
            # Rename all specified variables and collect renamed column names
            rename_dict = {}
            qc_var_names = []
            for var_name in variables:
                if var_name not in df.columns:
                    raise ValueError(f"Variable {var_name} not found in data")
                qc_var_name = var_mapping.get(var_name, var_name)
                rename_dict[var_name] = qc_var_name
                qc_var_names.append(qc_var_name)
            
            df = df.rename(columns=rename_dict)
            
            # Keep only required columns for QC (station + date parts + all renamed variables)
            keep_cols = ["station", "Year", "Month", "Day"] + qc_var_names
            df = df[[c for c in keep_cols if c in df.columns]]

        return df

    # ------------------------------------------------------------------
    # Station inventory helpers
    # ------------------------------------------------------------------

    def get_stations(self, force_reload=False) -> pd.DataFrame:
        """
        Load and return the GHCN-Daily station metadata table.

        Returns
        -------
        pd.DataFrame
            Columns: station_id, lat, lon, elevation, name, country
        """
        if self._stations is not None and not force_reload:
            return self._stations

        self.logger.info("Fetching GHCN-Daily station list …")
        resp = requests.get(GHCND_STATIONS_URL, timeout=60)
        resp.raise_for_status()

        colspecs = [(0, 11), (12, 20), (21, 30), (31, 37), (41, 71)]
        names = ["station_id", "lat", "lon", "elevation", "name"]
        df = pd.read_fwf(
            io.StringIO(resp.text),
            colspecs=colspecs,
            header=None,
            names=names,
        )
        df["lat"] = pd.to_numeric(df["lat"], errors="coerce")
        df["lon"] = pd.to_numeric(df["lon"], errors="coerce")
        df["elevation"] = pd.to_numeric(df["elevation"], errors="coerce")
        df["country"] = df["station_id"].str[:2]
        self._stations = df
        self.logger.info(f"Loaded {len(df)} stations")
        return df

    def search_stations(
        self,
        bbox=None,
        country=None,
        name_contains=None,
    ) -> pd.DataFrame:
        """
        Search stations by bounding box, country code, or name substring.

        Parameters
        ----------
        bbox : tuple (min_lon, min_lat, max_lon, max_lat), optional
        country : str, optional
            Two-letter country code prefix (e.g. ``"AG"`` for Argentina).
        name_contains : str, optional
            Case-insensitive substring to match station name.

        Returns
        -------
        pd.DataFrame
        """
        df = self.get_stations()

        if bbox is not None:
            min_lon, min_lat, max_lon, max_lat = bbox
            df = df[
                (df["lat"] >= min_lat) & (df["lat"] <= max_lat)
                & (df["lon"] >= min_lon) & (df["lon"] <= max_lon)
            ]
        if country is not None:
            df = df[df["country"] == country.upper()]
        if name_contains is not None:
            df = df[df["name"].str.contains(name_contains, case=False, na=False)]

        return df.reset_index(drop=True)

    def get_inventory(self) -> pd.DataFrame:
        """
        Return the GHCN-Daily variable inventory (which variables are
        available for each station and for which years).

        Returns
        -------
        pd.DataFrame
            Columns: station_id, variable, firstyear, lastyear
        """
        self.logger.info("Fetching GHCN-Daily inventory …")
        resp = requests.get(GHCND_INVENTORY_URL, timeout=120)
        resp.raise_for_status()

        colspecs = [(0, 11), (12, 20), (21, 30), (31, 35), (36, 40), (41, 45)]
        names = ["station_id", "lat", "lon", "variable", "firstyear", "lastyear"]
        df = pd.read_fwf(
            io.StringIO(resp.text),
            colspecs=colspecs,
            header=None,
            names=names,
        )
        df["firstyear"] = pd.to_numeric(df["firstyear"], errors="coerce")
        df["lastyear"] = pd.to_numeric(df["lastyear"], errors="coerce")
        return df

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _stations_in_bbox(
        self,
        bbox,
        variables,
        start_year,
        end_year,
        min_years=0,
    ) -> list[str]:
        """Return station IDs inside bbox that cover the requested period."""
        inventory = self.get_inventory()
        stations = self.get_stations()

        min_lon, min_lat, max_lon, max_lat = bbox

        # Spatial filter on inventory
        inv = inventory.merge(
            stations[["station_id", "lat", "lon"]],
            on="station_id",
            how="left",
        )
        inv = inv[
            (inv["lat"] >= min_lat) & (inv["lat"] <= max_lat)
            & (inv["lon"] >= min_lon) & (inv["lon"] <= max_lon)
        ]

        # Variable + temporal filter
        results = set()
        for var in variables:
            sub = inv[inv["variable"] == var]
            # Keep stations whose record overlaps the requested period
            sub = sub[
                (sub["firstyear"] <= end_year)
                & (sub["lastyear"] >= start_year)
            ]
            if min_years > 0:
                overlap = sub["lastyear"].clip(upper=end_year) - sub["firstyear"].clip(lower=start_year)
                sub = sub[overlap >= min_years]
            results |= set(sub["station_id"].tolist())

        return sorted(results)


# ---------------------------------------------------------------------------
# Module-level helpers (private)
# ---------------------------------------------------------------------------

def _fetch_station_csv(
    station_id: str,
    start_date: str,
    end_date: str,
    variables: list[str],
    convert: bool,
    logger,
    station_meta: dict | None = None,
) -> pd.DataFrame | None:
    """
    Fetch a single station's CSV file from NOAA, parse it, filter by date
    range and variables, and return a tidy DataFrame.

    The NOAA per-station CSV format has columns:
        ID, YEAR/MONTH/DAY, ELEMENT, DATA VALUE, M-FLAG, Q-FLAG, S-FLAG, OBS-TIME
    One row per (station, date, element).
    """
    url = f"{GHCND_CSV_BASE}/{station_id}.csv.gz"
    for attempt in range(3):
        try:
            resp = requests.get(url, timeout=60)
            if resp.status_code == 404:
                logger.debug(f"Station {station_id} not found (404)")
                return None
            resp.raise_for_status()
            break
        except requests.RequestException as exc:
            if attempt == 2:
                raise
            time.sleep(2 ** attempt)

    # Parse gzipped CSV payload.
    # NOAA by-station files can arrive with or without a header line,
    # so we always enforce the canonical 8-column layout.
    csv_columns = ["ID", "DATE", "ELEMENT", "DATA_VALUE", "M_FLAG", "Q_FLAG", "S_FLAG", "OBS_TIME"]
    df = pd.read_csv(
        io.BytesIO(resp.content),
        compression="gzip",
        header=None,
        names=csv_columns,
        low_memory=False,
    )

    # If a header line exists in-file, it is now the first data row: drop it.
    if not df.empty:
        first_id = str(df.iloc[0]["ID"]).strip().upper()
        first_date = str(df.iloc[0]["DATE"]).strip().upper()
        if first_id in {"ID", "STATION"} and first_date in {"DATE", "YEAR/MONTH/DAY", "YEAR_MONTH_DAY"}:
            df = df.iloc[1:].copy()

    # Normalise NOAA header variants to canonical names used below.
    df.columns = [
        c.upper().strip().replace("/", "_").replace("-", "_").replace(" ", "_")
        for c in df.columns
    ]

    rename_map = {
        "ID": "STATION",
        "YEAR_MONTH_DAY": "DATE",
        "DATA_VALUE": "DATA_VALUE",
        "M_FLAG": "M_FLAG",
        "Q_FLAG": "Q_FLAG",
        "S_FLAG": "S_FLAG",
        "OBS_TIME": "OBS_TIME",
    }
    present_renames = {k: v for k, v in rename_map.items() if k in df.columns}
    if present_renames:
        df = df.rename(columns=present_renames)

    if "DATE" not in df.columns:
        logger.warning(f"Station {station_id}: missing DATE column in NOAA CSV")
        return None

    if "STATION" not in df.columns:
        # NOAA by-station files are station-specific; populate explicitly if absent.
        df["STATION"] = station_id

    # NOAA DATE is typically YYYYMMDD in by-station CSV.
    date_as_str = df["DATE"].astype(str).str.strip()
    parsed_date = pd.to_datetime(date_as_str, format="%Y%m%d", errors="coerce")
    df["DATE"] = parsed_date.where(parsed_date.notna(), pd.to_datetime(date_as_str, errors="coerce"))
    df = df.dropna(subset=["DATE"])

    # Filter date range
    start = pd.to_datetime(start_date)
    end = pd.to_datetime(end_date)
    df = df[(df["DATE"] >= start) & (df["DATE"] <= end)]
    if df.empty:
        return None

    # Filter variables
    if "ELEMENT" in df.columns:
        # Long format: one row per (station, date, element)
        df = df[df["ELEMENT"].isin(variables)]
        if df.empty:
            return None

        # Pivot to wide
        df_wide = df.pivot_table(
            index=["STATION", "DATE"],
            columns="ELEMENT",
            values="DATA_VALUE",
            aggfunc="first",
        ).reset_index()
        df_wide.columns.name = None
    else:
        # Wide format (some versions of the endpoint)
        df_wide = df.rename(columns={"STATION": "STATION", "DATE": "DATE"})
        keep = ["STATION", "DATE"] + [v for v in variables if v in df_wide.columns]
        df_wide = df_wide[keep]

    # Rename
    df_wide = df_wide.rename(columns={"STATION": "station", "DATE": "date"})

    # Replace missing value sentinel
    for var in variables:
        if var in df_wide.columns:
            df_wide[var] = pd.to_numeric(df_wide[var], errors="coerce")
            df_wide[var] = df_wide[var].replace(-9999, np.nan)
            if convert and var in _TENTHS_VARS:
                df_wide[var] = df_wide[var] / 10.0

    # Attach station coordinates from metadata (best-effort)
    station_meta = station_meta or {}
    df_wide["lat"] = station_meta.get("lat", np.nan)
    df_wide["lon"] = station_meta.get("lon", np.nan)
    df_wide["elevation"] = station_meta.get("elevation", np.nan)

    return df_wide


def fetch_station_metadata(station_ids: list[str]) -> pd.DataFrame:
    """
    Convenience function: fetch metadata for a list of station IDs
    from the NOAA station list.

    Returns
    -------
    pd.DataFrame  with columns: station_id, lat, lon, elevation, name, country
    """
    resp = requests.get(GHCND_STATIONS_URL, timeout=60)
    resp.raise_for_status()
    colspecs = [(0, 11), (12, 20), (21, 30), (31, 37), (41, 71)]
    names = ["station_id", "lat", "lon", "elevation", "name"]
    df = pd.read_fwf(io.StringIO(resp.text), colspecs=colspecs, header=None, names=names)
    df["country"] = df["station_id"].str[:2]
    return df[df["station_id"].isin(station_ids)].reset_index(drop=True)
