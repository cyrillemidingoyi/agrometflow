# -*- coding: utf-8 -*-
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import tempfile

import pandas as pd
import requests
import xarray as xr
from tqdm.auto import tqdm

from .base import ClimateSource
from agrometflow.utils import (
    dataset_points_to_dataframe,
    extract_points_from_tuples,
    get_logger,
)


REMOTE_URL = "https://data.chc.ucsb.edu/products/CHIRPS-2.0"
DEFAULT_SOURCE_VAR = "precip"
DEFAULT_TARGET_VAR = "PR"


def _is_notebook_environment():
    try:
        from IPython import get_ipython

        shell = get_ipython().__class__.__name__
        return shell == "ZMQInteractiveShell"
    except (NameError, AttributeError):
        return False


class ChirpsDownloader(ClimateSource):
    def __init__(self, log_file=None, verbose=False):
        self.logger = get_logger(__name__, log_file=log_file, verbose=verbose)
        self.data = None

    def download(self, **kwargs):
        """
        Télécharge CHIRPS annuel et extrait directement les points ou la bbox
        quand une géométrie est fournie.

        Avec points ou bbox, les fichiers annuels globaux ne sont pas conservés:
        ils sont téléchargés en temporaire, sous-échantillonnés, puis supprimés.
        Sans points ni bbox, le comportement historique est conservé et les
        NetCDF annuels complets sont écrits dans output_dir/PR.
        """
        try:
            start_date = kwargs["start_date"]
            end_date = kwargs["end_date"]
            output_dir = Path(kwargs["output_dir"])
        except KeyError as e:
            raise ValueError(f"Missing required argument: {e}")

        variables = _normalize_variables(kwargs.get("variables", [DEFAULT_TARGET_VAR]))
        bbox = kwargs.get("bbox")
        points = kwargs.get("points") or kwargs.get("multipoints")
        timeout = kwargs.get("timeout", 180)

        is_notebook = _is_notebook_environment()
        max_workers = kwargs.get("max_workers", 4)
        if is_notebook and not kwargs.get("force_parallel", False):
            max_workers = 1

        start = pd.to_datetime(start_date)
        end = pd.to_datetime(end_date)
        years = list(range(start.year, end.year + 1))

        self.logger.info(f"Downloading CHIRPS precipitation for {len(years)} year(s)")
        self.logger.info(f"BBOX: {bbox}")
        self.logger.info(f"Max workers: {max_workers} (notebook mode: {is_notebook})")
        if points:
            self.logger.info(f"Points: {points}")

        if points:
            points_csv = build_points_csv_path(output_dir, start, end, points)
            if points_csv.exists() and not kwargs.get("overwrite_points_cache", False):
                self.logger.info(f"Using cached points CSV: {points_csv}")
                self.data = pd.read_csv(points_csv)
                return

            requests_to_run = build_requests(years, output_dir, variables, bbox=None)
            self.data = download_points_stream(
                requests_to_run=requests_to_run,
                timeout=timeout,
                points=points,
                start_date=start_date,
                end_date=end_date,
                logger=self.logger,
                max_workers=max_workers,
            )
            if self.data is not None and not self.data.empty:
                points_csv.parent.mkdir(parents=True, exist_ok=True)
                self.data.to_csv(points_csv, index=False)
                self.logger.info(f"Saved points CSV: {points_csv}")
            return

        requests_to_run = build_requests(years, output_dir, variables, bbox=bbox)
        if bbox:
            requests_to_run = [
                request
                for request in requests_to_run
                if not request["group"]["final_nc"].exists()
            ]
            if not requests_to_run:
                self.logger.info("All CHIRPS bbox outputs already exist.")
                return

            download_bbox_stream(
                requests_to_run=requests_to_run,
                timeout=timeout,
                bbox=bbox,
                start_date=start_date,
                end_date=end_date,
                logger=self.logger,
                max_workers=max_workers,
            )
            return

        download_full_years(
            requests_to_run=requests_to_run,
            timeout=timeout,
            logger=self.logger,
            max_workers=max_workers,
        )

    def extract(self, variables=None, start_date=None, end_date=None, as_long=False, **kwargs):
        if self.data is None:
            raise ValueError("No point data available. Run download(points=...) first.")

        df = self.data.copy()
        if "time" in df.columns:
            df["time"] = pd.to_datetime(df["time"])
            if start_date:
                df = df[df["time"] >= pd.to_datetime(start_date)]
            if end_date:
                df = df[df["time"] <= pd.to_datetime(end_date)]

        if variables:
            keep = [c for c in ["time", "lon", "lat"] if c in df.columns]
            keep += [v for v in variables if v in df.columns]
            df = df[keep]

        if as_long:
            id_vars = [c for c in ["time", "lon", "lat"] if c in df.columns]
            value_vars = [c for c in df.columns if c not in id_vars]
            df = df.melt(
                id_vars=id_vars,
                value_vars=value_vars,
                var_name="variable",
                value_name="value",
            )

        return df


def build_requests(years, output_dir, variables, bbox=None):
    requests_to_run = []
    for source_var, target_var in variables:
        var_dir = Path(output_dir) / target_var
        var_dir.mkdir(parents=True, exist_ok=True)

        for year in years:
            filename = f"chirps-v2.0.{year}.days_p05.nc"
            url = f"{REMOTE_URL}/global_daily/netcdf/p05/{filename}"
            final_nc = build_yearly_nc_path(output_dir, target_var, year, bbox=bbox)
            requests_to_run.append(
                {
                    "url": url,
                    "filename": filename,
                    "path": var_dir / filename,
                    "group": {
                        "year": year,
                        "source_var": source_var,
                        "target_var": target_var,
                        "final_nc": final_nc,
                    },
                }
            )

    return requests_to_run


def download_points_stream(requests_to_run, timeout, points, start_date, end_date, logger, max_workers=4):
    frames_by_var = {}
    progress_desc = "Downloading CHIRPS files and extracting points"

    if max_workers == 1:
        for request in tqdm(requests_to_run, total=len(requests_to_run), desc=progress_desc):
            download = _download_request_to_tempfile(request, timeout, logger)
            if download is None:
                continue
            result = _extract_points_subset_from_file(
                download["request"],
                download["path"],
                points,
                start_date,
                end_date,
                logger,
            )
            if result is not None:
                frames_by_var.setdefault(result["target_var"], []).append(result["df"])
    else:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(_download_request_to_tempfile, request, timeout, logger): request
                for request in requests_to_run
            }
            for future in tqdm(as_completed(futures), total=len(futures), desc=progress_desc):
                download = future.result()
                if download is None:
                    continue
                result = _extract_points_subset_from_file(
                    download["request"],
                    download["path"],
                    points,
                    start_date,
                    end_date,
                    logger,
                )
                if result is not None:
                    frames_by_var.setdefault(result["target_var"], []).append(result["df"])

    return _merge_frames_by_var(frames_by_var)


def download_bbox_stream(requests_to_run, timeout, bbox, start_date, end_date, logger, max_workers=4):
    progress_desc = "Downloading CHIRPS files and clipping bbox"

    if max_workers == 1:
        for request in tqdm(requests_to_run, total=len(requests_to_run), desc=progress_desc):
            download = _download_request_to_tempfile(request, timeout, logger)
            if download is None:
                continue
            _write_bbox_subset_from_file(
                download["request"],
                download["path"],
                bbox,
                start_date,
                end_date,
                logger,
            )
    else:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(_download_request_to_tempfile, request, timeout, logger): request
                for request in requests_to_run
            }
            for future in tqdm(as_completed(futures), total=len(futures), desc=progress_desc):
                download = future.result()
                if download is None:
                    continue
                _write_bbox_subset_from_file(
                    download["request"],
                    download["path"],
                    bbox,
                    start_date,
                    end_date,
                    logger,
                )


def download_full_years(requests_to_run, timeout, logger, max_workers=4):
    progress_desc = "Downloading CHIRPS yearly files"
    requests_to_run = [request for request in requests_to_run if not request["path"].exists()]
    if not requests_to_run:
        logger.info("All CHIRPS yearly files already exist.")
        return

    if max_workers == 1:
        for request in tqdm(requests_to_run, total=len(requests_to_run), desc=progress_desc):
            fetch(request, timeout, logger)
    else:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(fetch, request, timeout, logger): request
                for request in requests_to_run
            }
            for future in tqdm(as_completed(futures), total=len(futures), desc=progress_desc):
                future.result()


def fetch(request, timeout, logger):
    path = Path(request["path"])
    if path.exists():
        logger.info(f"Already exists: {path}")
        return path

    try:
        logger.debug(f"Downloading {request['url']}")
        path.parent.mkdir(parents=True, exist_ok=True)
        with requests.get(request["url"], stream=True, timeout=timeout) as response:
            response.raise_for_status()
            _raise_if_html(response)
            with open(path, "wb") as f:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)
        return path
    except Exception as e:
        logger.warning(f"Failed for {request['filename']}: {e}")
        path.unlink(missing_ok=True)
        return None


def _download_request_to_tempfile(request, timeout, logger):
    local_path = _download_to_tempfile(request["url"], request["filename"], timeout, logger)
    if local_path is None:
        return None
    return {"request": request, "path": local_path}


def _download_to_tempfile(url, filename, timeout, logger):
    tmp = tempfile.NamedTemporaryFile(prefix="chirps_", suffix=".nc", delete=False)
    tmp_path = Path(tmp.name)
    tmp.close()

    try:
        logger.debug(f"Downloading {url}")
        with requests.get(url, stream=True, timeout=timeout) as response:
            response.raise_for_status()
            _raise_if_html(response)
            with open(tmp_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)
        return tmp_path
    except Exception as e:
        logger.warning(f"Failed for {filename}: {e}")
        tmp_path.unlink(missing_ok=True)
        return None


def _extract_points_subset_from_file(request, local_path, points, start_date, end_date, logger):
    group = request["group"]
    source_var = group["source_var"]
    target_var = group["target_var"]

    try:
        with xr.open_dataset(local_path) as ds:
            ds = _prepare_dataset(ds, source_var=source_var, bbox=None)
            ds = _subset_time(ds, start_date, end_date)
            ds_pts = extract_points_from_tuples(ds, points)
            df = dataset_points_to_dataframe(ds_pts)

        if "time" in df.columns:
            df["time"] = pd.to_datetime(df["time"])
        base_cols = [c for c in ["time", "lon", "lat"] if c in df.columns]
        keep_cols = base_cols + [c for c in [source_var] if c in df.columns]
        df = df[keep_cols]
        if source_var in df.columns and source_var != target_var:
            df = df.rename(columns={source_var: target_var})
        return {"target_var": target_var, "df": df}
    except Exception as e:
        logger.warning(f"Failed to extract points from {request['filename']}: {e}")
        return None
    finally:
        Path(local_path).unlink(missing_ok=True)


def _write_bbox_subset_from_file(request, local_path, bbox, start_date, end_date, logger):
    group = request["group"]
    final_nc = Path(group["final_nc"])
    source_var = group["source_var"]
    target_var = group["target_var"]

    if final_nc.exists():
        logger.info(f"Already exists: {final_nc}")
        Path(local_path).unlink(missing_ok=True)
        return final_nc

    try:
        with xr.open_dataset(local_path) as ds:
            ds = _prepare_dataset(ds, source_var=source_var, bbox=bbox)
            ds = _subset_time(ds, start_date, end_date)
            if source_var != target_var:
                ds = ds.rename({source_var: target_var})
            ds = ds.load()

        final_nc.parent.mkdir(parents=True, exist_ok=True)
        encoding = {target_var: {"zlib": True, "complevel": 4}}
        ds.to_netcdf(final_nc, encoding=encoding)
        ds.close()
        logger.info(f"Saved CHIRPS bbox NetCDF: {final_nc}")
        return final_nc
    except Exception as e:
        logger.warning(f"Failed to clip bbox from {request['filename']}: {e}")
        return None
    finally:
        Path(local_path).unlink(missing_ok=True)


def _prepare_dataset(ds, source_var, bbox=None):
    resolved = _resolve_data_var(ds, source_var)
    ds = ds[[resolved]]
    if resolved != source_var:
        ds = ds.rename({resolved: source_var})
    return _subset_bbox(ds, bbox)


def _subset_bbox(ds, bbox):
    if not bbox:
        return ds

    lon_min, lat_min, lon_max, lat_max = bbox
    lon_name = _find_coord_name(ds, ("lon", "longitude"))
    lat_name = _find_coord_name(ds, ("lat", "latitude"))

    if lon_name:
        lon_values = ds[lon_name]
        lon_ascending = lon_values.values[0] <= lon_values.values[-1]
        lon_slice = slice(lon_min, lon_max) if lon_ascending else slice(lon_max, lon_min)
        ds = ds.sel({lon_name: lon_slice})

    if lat_name:
        lat_values = ds[lat_name]
        lat_ascending = lat_values.values[0] <= lat_values.values[-1]
        lat_slice = slice(lat_min, lat_max) if lat_ascending else slice(lat_max, lat_min)
        ds = ds.sel({lat_name: lat_slice})

    return ds


def _subset_time(ds, start_date, end_date):
    if "time" not in ds.coords:
        return ds
    return ds.sel(time=slice(pd.to_datetime(start_date), pd.to_datetime(end_date)))


def _merge_frames_by_var(frames_by_var):
    if not frames_by_var:
        return pd.DataFrame()

    frames = []
    for target_var, var_frames in frames_by_var.items():
        combined = pd.concat(var_frames, ignore_index=True)
        combined = combined.sort_values(["time", "lon", "lat"]).reset_index(drop=True)
        frames.append(combined)

    merged = frames[0]
    for frame in frames[1:]:
        merged = merged.merge(frame, on=["time", "lon", "lat"], how="outer")

    return merged.sort_values(["time", "lon", "lat"]).reset_index(drop=True)


def _normalize_variables(variables):
    normalized = []
    for item in variables:
        if isinstance(item, (list, tuple)) and len(item) == 2:
            source_var, target_var = item
        else:
            source_var = DEFAULT_SOURCE_VAR if item == DEFAULT_TARGET_VAR else item
            target_var = item
        normalized.append([source_var, target_var])
    return normalized


def _resolve_data_var(ds, expected_name):
    aliases = {
        "PR": "precip",
        "pr": "precip",
    }
    candidates = [expected_name, aliases.get(expected_name)]

    for candidate in candidates:
        if candidate and candidate in ds.data_vars:
            return candidate

    lowered = {name.lower(): name for name in ds.data_vars}
    for candidate in candidates:
        if candidate and candidate.lower() in lowered:
            return lowered[candidate.lower()]

    if len(ds.data_vars) == 1:
        return next(iter(ds.data_vars))

    raise KeyError(
        f"Unable to resolve data variable '{expected_name}'. Available: {list(ds.data_vars)}"
    )


def _raise_if_html(response):
    content_type = response.headers.get("content-type", "")
    if "text/html" in content_type.lower():
        raise PermissionError("Received HTML instead of NetCDF.")


def build_points_csv_path(output_dir, start_date, end_date, points):
    output_dir = Path(output_dir)
    start = pd.to_datetime(start_date)
    end = pd.to_datetime(end_date)
    return output_dir / f"chirps_{start:%Y%m%d}_{end:%Y%m%d}_{_points_suffix(points)}.csv"


def build_yearly_nc_path(output_dir, target_var, year, bbox=None):
    output_dir = Path(output_dir)
    var_dir = output_dir / target_var
    var_dir.mkdir(parents=True, exist_ok=True)

    suffix = f"_{_bbox_suffix(bbox)}" if bbox else ""
    return var_dir / f"chirps_{target_var}_{year}{suffix}.nc"


def _points_suffix(points):
    tokens = []
    for lon, lat in points:
        tokens.append(f"pt_{_coord_token(lon)}_{_coord_token(lat)}")
    return "__".join(tokens)


def _bbox_suffix(bbox):
    lon_min, lat_min, lon_max, lat_max = bbox
    return (
        f"bbox_"
        f"{_coord_token(lon_min)}_"
        f"{_coord_token(lat_min)}_"
        f"{_coord_token(lon_max)}_"
        f"{_coord_token(lat_max)}"
    )


def _coord_token(value):
    value = float(value)
    sign = "m" if value < 0 else "p"
    return f"{sign}{abs(value):.2f}".replace(".", "p")


def _find_coord_name(ds, candidates):
    for name in candidates:
        if name in ds.coords or name in ds.dims:
            return name
    return None


def build_request(startdate, enddate, logger=None):
    """Backward-compatible helper returning CHIRPS yearly URLs."""
    years = range(int(startdate), int(enddate) + 1)
    return [
        (
            f"{REMOTE_URL}/global_daily/netcdf/p05/chirps-v2.0.{year}.days_p05.nc",
            f"chirps-v2.0.{year}.days_p05.nc",
        )
        for year in years
    ]
