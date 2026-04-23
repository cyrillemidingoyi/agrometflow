import os
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


PRODUCTS = {
    "mdmetv3": {
        "base_url": "https://datalsasaf.lsasvcs.ipma.pt/PRODUCTS/MSG/MDMETv3/NETCDF",
        "file_prefix": "NETCDF4_LSASAF_MSG_DMETv3_MSG-Disk_",
        "default_var": "ET",
    },
    "metref": {
        "base_url": "https://datalsasaf.lsasvcs.ipma.pt/PRODUCTS/MSG/METREF/NETCDF",
        "file_prefix": "NETCDF4_LSASAF_MSG_METREF_MSG-Disk_",
        "default_var": "ETo",
    },
}


def _is_notebook_environment():
    try:
        from IPython import get_ipython
        shell = get_ipython().__class__.__name__
        return shell == "ZMQInteractiveShell"
    except (NameError, AttributeError):
        return False


class LSASAFDownloader(ClimateSource):
    def __init__(self, log_file=None, verbose=False):
        self.logger = get_logger(__name__, log_file=log_file, verbose=verbose)
        self.data = None

    def download(self, **kwargs):
        """
        Télécharge des produits quotidiens LSA SAF par HTTP et produit
        un NetCDF annuel par variable.

        Parameters
        ----------
        start_date : str (YYYY-MM-DD)
        end_date : str (YYYY-MM-DD)
        variables : list of [source_var, target_var]
        output_dir : str
        bbox : list [lon_min, lat_min, lon_max, lat_max], optional
        product : str, default "mdmetv3"
        username/password : optional credentials for protected access
        """
        try:
            start_date = kwargs["start_date"]
            end_date = kwargs["end_date"]
            variables = kwargs["variables"]
            output_dir = Path(kwargs["output_dir"])
        except KeyError as e:
            raise ValueError(f"Missing required argument: {e}")

        product = kwargs.get("product", "mdmetv3").lower()
        if product not in PRODUCTS:
            raise ValueError(
                f"Unsupported LSA SAF product '{product}'. Available: {sorted(PRODUCTS)}"
            )

        bbox = kwargs.get("bbox")
        points = kwargs.get("points")
        is_notebook = _is_notebook_environment()
        max_workers = kwargs.get("max_workers", 4)
        if is_notebook and not kwargs.get("force_parallel", False):
            max_workers = 1
        timeout = kwargs.get("timeout", 120)
        auth = _build_auth(kwargs)

        start = pd.to_datetime(start_date)
        end = pd.to_datetime(end_date)
        all_dates = list(pd.date_range(start, end, freq="D"))

        self.logger.info(
            f"Downloading LSA SAF product '{product}' for {len(all_dates)} day(s)"
        )
        self.logger.info(f"BBOX: {bbox}")
        self.logger.info(f"Max workers: {max_workers} (notebook mode: {is_notebook})")
        if points:
            self.logger.info(f"Points: {points}")

            points_csv = build_points_csv_path(output_dir, product, start, end, points)
        if points and points_csv.exists() and not kwargs.get("overwrite_points_cache", False):
            self.logger.info(f"Using cached points CSV: {points_csv}")
            self.data = pd.read_csv(points_csv)
            return

        final_files = collect_final_files(variables, all_dates, output_dir, product, bbox=bbox)
        if points and final_files and not kwargs.get("prefer_redownload", False):
            self.logger.info("Using existing yearly NetCDF file(s) to rebuild point extractions")
            self.data = extract_points_from_yearly_files(
                final_files=final_files,
                points=points,
                start_date=start_date,
                end_date=end_date,
                logger=self.logger,
            )
            if self.data is not None and not self.data.empty:
                self.data.to_csv(points_csv, index=False)
                self.logger.info(f"Saved points CSV: {points_csv}")
                return

        requests_to_run = build_requests(
            variables,
            all_dates,
            output_dir,
            product,
            bbox=bbox,
            skip_existing_final=not bool(points),
        )
        self.logger.info(f"Prepared {len(requests_to_run)} daily request(s)")

        if points:
            self.data = download_points_stream(
                requests_to_run=requests_to_run,
                auth=auth,
                timeout=timeout,
                points=points,
                start_date=start_date,
                end_date=end_date,
                logger=self.logger,
                max_workers=max_workers,
            )
            if self.data is not None and not self.data.empty:
                self.data.to_csv(points_csv, index=False)
                self.logger.info(f"Saved points CSV: {points_csv}")
            return

        if bbox:
            download_bbox_stream(
                requests_to_run=requests_to_run,
                auth=auth,
                timeout=timeout,
                bbox=bbox,
                logger=self.logger,
                max_workers=max_workers,
            )
            return

        files_by_group = {}
        progress_desc = "Downloading LSA SAF daily files"
        if max_workers == 1:
            for request in tqdm(requests_to_run, total=len(requests_to_run), desc=progress_desc):
                result = fetch(request, auth, timeout, self.logger)
                if result:
                    files_by_group.setdefault(result["group"], []).append(result["path"])
        else:
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {
                    executor.submit(fetch, request, auth, timeout, self.logger): request
                    for request in requests_to_run
                }
                for future in tqdm(as_completed(futures), total=len(futures), desc=progress_desc):
                    result = future.result()
                    if result:
                        files_by_group.setdefault(result["group"], []).append(result["path"])

        for group, files in files_by_group.items():
            merge_yearly(group, files, bbox=None, logger=self.logger)

    def extract(self, variables=None, start_date=None, end_date=None, as_long=False, **kwargs):
        if self.data is None:
            raise ValueError("No point data available. Run download(points=...) first.")

        df = self.data.copy()

        time_col = "time" if "time" in df.columns else "Date" if "Date" in df.columns else None
        if time_col:
            df[time_col] = pd.to_datetime(df[time_col])
            if start_date:
                df = df[df[time_col] >= pd.to_datetime(start_date)]
            if end_date:
                df = df[df[time_col] <= pd.to_datetime(end_date)]

        if variables:
            keep = [c for c in [time_col, "lon", "lat"] if c and c in df.columns]
            keep += [v for v in variables if v in df.columns]
            df = df[keep]

        if as_long:
            id_vars = [c for c in [time_col, "lon", "lat"] if c and c in df.columns]
            value_vars = [c for c in df.columns if c not in id_vars]
            df = df.melt(id_vars=id_vars, value_vars=value_vars, var_name="variable", value_name="value")

        return df


def build_requests(variables, dates, output_dir, product, bbox=None, skip_existing_final=True):
    product_cfg = PRODUCTS[product]
    requests_to_run = []

    for source_var, target_var in variables:
        var_dir = Path(output_dir) / target_var
        var_dir.mkdir(parents=True, exist_ok=True)

        for year, group_dates in pd.Series(dates).groupby(pd.Series(dates).dt.year):
            final_nc = build_yearly_nc_path(
                output_dir=output_dir,
                target_var=target_var,
                product=product,
                year=int(year),
                bbox=bbox,
            )
            if skip_existing_final and final_nc.exists():
                continue

            tmp_dir = var_dir / "tmp" / str(year)
            tmp_dir.mkdir(parents=True, exist_ok=True)

            effective_source_var = source_var or product_cfg["default_var"]

            for day in group_dates.tolist():
                datestr = pd.Timestamp(day).strftime("%Y%m%d")
                url = (
                    f"{product_cfg['base_url']}/{day:%Y}/{day:%m}/{day:%d}/"
                    f"{product_cfg['file_prefix']}{datestr}0000.nc"
                )
                local_path = tmp_dir / f"{datestr}.nc"
                requests_to_run.append(
                    {
                        "url": url,
                        "path": local_path,
                        "group": {
                            "product": product,
                            "year": int(year),
                            "source_var": effective_source_var,
                            "target_var": target_var,
                            "final_nc": final_nc,
                        },
                    }
                )

    return requests_to_run


def collect_final_files(variables, dates, output_dir, product, bbox=None):
    final_files = []
    years = sorted(pd.Series(dates).dt.year.unique().tolist())

    for _, target_var in variables:
        for year in years:
            final_nc = build_yearly_nc_path(
                output_dir=output_dir,
                target_var=target_var,
                product=product,
                year=year,
                bbox=bbox,
            )
            if final_nc.exists():
                final_files.append(
                    {
                        "path": final_nc,
                        "target_var": target_var,
                        "year": year,
                    }
                )

    return final_files


def download_points_stream(requests_to_run, auth, timeout, points, start_date, end_date, logger, max_workers=4):
    frames_by_var = {}
    progress_desc = "Downloading LSA SAF files and extracting points"

    if max_workers == 1:
        for request in tqdm(requests_to_run, total=len(requests_to_run), desc=progress_desc):
            download = _download_request_to_tempfile(request, auth, timeout, logger)
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
            if result is None:
                continue
            frames_by_var.setdefault(result["target_var"], []).append(result["df"])
    else:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    _download_request_to_tempfile,
                    request,
                    auth,
                    timeout,
                    logger,
                ): request
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
                if result is None:
                    continue
                frames_by_var.setdefault(result["target_var"], []).append(result["df"])

    return _merge_frames_by_var(frames_by_var)


def download_bbox_stream(requests_to_run, auth, timeout, bbox, logger, max_workers=4):
    datasets_by_group = {}
    progress_desc = "Downloading LSA SAF files and clipping bbox"

    if max_workers == 1:
        for request in tqdm(requests_to_run, total=len(requests_to_run), desc=progress_desc):
            download = _download_request_to_tempfile(request, auth, timeout, logger)
            if download is None:
                continue
            result = _extract_bbox_subset_from_file(
                download["request"],
                download["path"],
                bbox,
                logger,
            )
            if result is None:
                continue
            datasets_by_group.setdefault(result["group"], []).append(result["dataset"])
    else:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(_download_request_to_tempfile, request, auth, timeout, logger): request
                for request in requests_to_run
            }
            for future in tqdm(as_completed(futures), total=len(futures), desc=progress_desc):
                download = future.result()
                if download is None:
                    continue
                result = _extract_bbox_subset_from_file(
                    download["request"],
                    download["path"],
                    bbox,
                    logger,
                )
                if result is None:
                    continue
                datasets_by_group.setdefault(result["group"], []).append(result["dataset"])

    for group_key, datasets in datasets_by_group.items():
        _write_yearly_subset(group_key, datasets, logger)


def extract_points_from_files(files_by_group, points, start_date, end_date, logger):
    frames_by_var = {}

    for group_key, files in files_by_group.items():
        group = _group_from_key(group_key)
        target_var = group["target_var"]
        source_var = group["source_var"]

        for path in sorted(Path(f) for f in files):
            try:
                file_date = _date_from_daily_file(path)
                ds = xr.open_dataset(path)
                ds = _prepare_dataset(ds, source_var=source_var, bbox=None)
                if "time" in ds.coords and ds.sizes.get("time", 0) == 1:
                    ds = ds.assign_coords(time=[file_date])
                ds_pts = extract_points_from_tuples(ds, points)
                df = dataset_points_to_dataframe(ds_pts)
                if "time" in df.columns:
                    df["time"] = pd.to_datetime(df["time"])
                    df = df[
                        (df["time"] >= pd.to_datetime(start_date))
                        & (df["time"] <= pd.to_datetime(end_date))
                    ]

                base_cols = [c for c in ["time", "lon", "lat"] if c in df.columns]
                keep_cols = base_cols + [c for c in [source_var] if c in df.columns]
                df = df[keep_cols]
                if source_var in df.columns and source_var != target_var:
                    df = df.rename(columns={source_var: target_var})
                frames_by_var.setdefault(target_var, []).append(df)
                ds.close()
            except Exception as e:
                logger.warning(f"Failed to extract points from {path.name}: {e}")

    return _merge_frames_by_var(frames_by_var)


def extract_points_from_yearly_files(final_files, points, start_date, end_date, logger):
    frames_by_var = {}

    for item in final_files:
        path = Path(item["path"])
        target_var = item["target_var"]
        year = item["year"]
        try:
            ds = xr.open_dataset(path)
            if "time" in ds.coords:
                ds = ds.assign_coords(
                    time=pd.date_range(f"{year}-01-01", periods=ds.sizes["time"], freq="D")
                )
            ds_pts = extract_points_from_tuples(ds, points)
            df = dataset_points_to_dataframe(ds_pts)
            if "time" in df.columns:
                df["time"] = pd.to_datetime(df["time"])
                df = df[
                    (df["time"] >= pd.to_datetime(start_date))
                    & (df["time"] <= pd.to_datetime(end_date))
                ]

            base_cols = [c for c in ["time", "lon", "lat"] if c in df.columns]
            keep_cols = base_cols + [c for c in [target_var] if c in df.columns]
            df = df[keep_cols]
            frames_by_var.setdefault(target_var, []).append(df)
            ds.close()
        except Exception as e:
            logger.warning(f"Failed to extract points from yearly file {path.name}: {e}")

    return _merge_frames_by_var(frames_by_var)


def fetch(request, auth, timeout, logger):
    url = request["url"]
    path = Path(request["path"])
    group = request["group"]

    if path.exists():
        return {"group": _group_key(group), "path": path}

    try:
        logger.debug(f"Downloading {url}")
        with requests.get(url, auth=auth, stream=True, timeout=timeout) as response:
            if response.status_code == 404:
                logger.warning(f"Missing file: {url}")
                return None
            response.raise_for_status()

            content_type = response.headers.get("content-type", "")
            if "text/html" in content_type.lower():
                raise PermissionError(
                    "Received HTML instead of NetCDF. Check LSA SAF credentials/access."
                )

            path.parent.mkdir(parents=True, exist_ok=True)
            with open(path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)

        return {"group": _group_key(group), "path": path}
    except Exception as e:
        logger.warning(f"Failed for {path.name}: {e}")
        return None


def merge_yearly(group, files, bbox, logger):
    group = _group_from_key(group)
    files = sorted(Path(f) for f in files)
    if not files:
        return

    final_nc = Path(group["final_nc"])
    if final_nc.exists():
        logger.info(f"Already exists: {final_nc}")
        return

    try:
        logger.info(
            f"Merging {len(files)} daily file(s) into {final_nc.name}"
        )
        datasets = []
        crs_var = None

        for file_path in files:
            file_date = _date_from_daily_file(file_path)
            ds_day = xr.open_dataset(file_path)
            ds_day = _prepare_dataset(ds_day, source_var=group["source_var"], bbox=bbox)

            source_var = _resolve_data_var(ds_day, group["source_var"])
            ds_day = ds_day[[source_var]].assign_coords(time=[file_date])

            if "crs" in ds_day.variables and crs_var is None:
                crs_var = ds_day["crs"]

            datasets.append(ds_day.load())
            ds_day.close()

        if not datasets:
            logger.warning(f"No dataset available for {final_nc.name}")
            return

        ds = xr.concat(datasets, dim="time")
        if group["source_var"] != group["target_var"]:
            ds = ds.rename({group["source_var"]: group["target_var"]})
        if crs_var is not None and "crs" not in ds.variables:
            ds["crs"] = crs_var

        encoding = {
            group["target_var"]: {
                "zlib": True,
                "complevel": 4,
            }
        }
        ds.to_netcdf(final_nc, encoding=encoding)
        ds.close()
        logger.info(f"Saved yearly NetCDF: {final_nc}")
    except Exception as e:
        logger.error(f"Failed to merge {final_nc.name}: {e}")
        return
    finally:
        for file_path in files:
            if file_path.exists():
                file_path.unlink()
        tmp_dir = final_nc.parent / "tmp" / str(group["year"])
        if tmp_dir.exists() and not any(tmp_dir.iterdir()):
            tmp_dir.rmdir()


def cleanup_group_tempdirs(files_by_group, logger):
    return None


def _build_auth(kwargs):
    username = kwargs.get("username", os.environ.get("LSASAF_USERNAME"))
    password = kwargs.get("password", os.environ.get("LSASAF_PASSWORD"))
    if username and password:
        return (username, password)
    return None


def _group_key(group):
    return (
        group["product"],
        group["year"],
        group["source_var"],
        group["target_var"],
        str(group["final_nc"]),
    )


def _group_from_key(group_key):
    product, year, source_var, target_var, final_nc = group_key
    return {
        "product": product,
        "year": year,
        "source_var": source_var,
        "target_var": target_var,
        "final_nc": Path(final_nc),
    }


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


def _prepare_dataset(ds, source_var, bbox=None):
    keep_vars = []
    resolved = _resolve_data_var(ds, source_var)
    keep_vars.append(resolved)
    if "crs" in ds.data_vars and "crs" not in keep_vars:
        keep_vars.append("crs")
    ds = ds[keep_vars]
    ds = _subset_bbox(ds, bbox)
    return ds


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


def _download_request_to_tempfile(request, auth, timeout, logger):
    local_path = _download_to_tempfile(request["url"], auth, timeout, logger)
    if local_path is None:
        return None
    return {"request": request, "path": local_path}


def _fetch_points_subset(request, auth, timeout, points, start_date, end_date, logger):
    local_path = _download_to_tempfile(request["url"], auth, timeout, logger)
    if local_path is None:
        return None

    return _extract_points_subset_from_file(
        request,
        local_path,
        points,
        start_date,
        end_date,
        logger,
    )


def _extract_points_subset_from_file(request, local_path, points, start_date, end_date, logger):
    group = request["group"]
    source_var = group["source_var"]
    target_var = group["target_var"]

    try:
        file_date = _date_from_daily_file(request["path"])
        ds = xr.open_dataset(local_path)
        ds = _prepare_dataset(ds, source_var=source_var, bbox=None)
        if "time" in ds.coords and ds.sizes.get("time", 0) == 1:
            ds = ds.assign_coords(time=[file_date])
        ds_pts = extract_points_from_tuples(ds, points)
        df = dataset_points_to_dataframe(ds_pts)
        if "time" in df.columns:
            df["time"] = pd.to_datetime(df["time"])
            df = df[
                (df["time"] >= pd.to_datetime(start_date))
                & (df["time"] <= pd.to_datetime(end_date))
            ]

        base_cols = [c for c in ["time", "lon", "lat"] if c in df.columns]
        keep_cols = base_cols + [c for c in [source_var] if c in df.columns]
        df = df[keep_cols]
        if source_var in df.columns and source_var != target_var:
            df = df.rename(columns={source_var: target_var})
        ds.close()
        return {"target_var": target_var, "df": df}
    except Exception as e:
        logger.warning(f"Failed to extract points from {request['path'].name}: {e}")
        return None
    finally:
        Path(local_path).unlink(missing_ok=True)


def _fetch_bbox_subset(request, auth, timeout, bbox, logger):
    local_path = _download_to_tempfile(request["url"], auth, timeout, logger)
    if local_path is None:
        return None

    return _extract_bbox_subset_from_file(request, local_path, bbox, logger)


def _extract_bbox_subset_from_file(request, local_path, bbox, logger):
    group = request["group"]
    source_var = group["source_var"]

    try:
        file_date = _date_from_daily_file(request["path"])
        ds = xr.open_dataset(local_path)
        ds = _prepare_dataset(ds, source_var=source_var, bbox=bbox)
        ds = ds[[source_var]].assign_coords(time=[file_date]).load()
        ds.close()
        return {"group": _group_key(group), "dataset": ds}
    except Exception as e:
        logger.warning(f"Failed to clip bbox from {request['path'].name}: {e}")
        return None
    finally:
        Path(local_path).unlink(missing_ok=True)


def _download_to_tempfile(url, auth, timeout, logger):
    tmp = tempfile.NamedTemporaryFile(prefix="lsasaf_", suffix=".nc", delete=False)
    tmp_path = Path(tmp.name)
    tmp.close()

    try:
        logger.debug(f"Downloading {url}")
        with requests.get(url, auth=auth, stream=True, timeout=timeout) as response:
            if response.status_code == 404:
                logger.warning(f"Missing file: {url}")
                tmp_path.unlink(missing_ok=True)
                return None
            response.raise_for_status()
            content_type = response.headers.get("content-type", "")
            if "text/html" in content_type.lower():
                raise PermissionError(
                    "Received HTML instead of NetCDF. Check LSA SAF credentials/access."
                )

            with open(tmp_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
        return tmp_path
    except Exception as e:
        logger.warning(f"Failed for {url}: {e}")
        tmp_path.unlink(missing_ok=True)
        return None


def _write_yearly_subset(group_key, datasets, logger):
    group = _group_from_key(group_key)
    final_nc = Path(group["final_nc"])
    if final_nc.exists():
        logger.info(f"Already exists: {final_nc}")
        return
    if not datasets:
        logger.warning(f"No dataset available for {final_nc.name}")
        return

    try:
        datasets = sorted(datasets, key=lambda ds: pd.to_datetime(ds.time.values[0]))
        ds = xr.concat(datasets, dim="time")
        if group["source_var"] != group["target_var"]:
            ds = ds.rename({group["source_var"]: group["target_var"]})
        encoding = {group["target_var"]: {"zlib": True, "complevel": 4}}
        ds.to_netcdf(final_nc, encoding=encoding)
        ds.close()
        logger.info(f"Saved yearly NetCDF: {final_nc}")
    except Exception as e:
        logger.error(f"Failed to write {final_nc.name}: {e}")


def _date_from_daily_file(path):
    return pd.to_datetime(Path(path).stem, format="%Y%m%d").normalize()


def build_points_csv_path(output_dir, product, start_date, end_date, points):
    output_dir = Path(output_dir)
    start = pd.to_datetime(start_date)
    end = pd.to_datetime(end_date)
    return output_dir / f"{product}_{start:%Y%m%d}_{end:%Y%m%d}_{_points_suffix(points)}.csv"


def build_yearly_nc_path(output_dir, target_var, product, year, bbox=None):
    output_dir = Path(output_dir)
    var_dir = output_dir / target_var
    var_dir.mkdir(parents=True, exist_ok=True)

    suffix = f"_{_bbox_suffix(bbox)}" if bbox else ""
    return var_dir / f"{product}_{target_var}_{year}{suffix}.nc"


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


def _resolve_data_var(ds, expected_name):
    if expected_name in ds.data_vars:
        return expected_name

    lowered = {name.lower(): name for name in ds.data_vars}
    if expected_name.lower() in lowered:
        return lowered[expected_name.lower()]

    if len(ds.data_vars) == 1:
        return next(iter(ds.data_vars))

    raise KeyError(
        f"Unable to resolve data variable '{expected_name}'. Available: {list(ds.data_vars)}"
    )
