import requests
import rioxarray
import xarray as xr
from pathlib import Path
from zipfile import ZipFile
from io import BytesIO
from datetime import datetime, timedelta
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

from agrometflow.utils import get_logger


class Arc2Downloader:
    BASE_URL = "https://ftp.cpc.ncep.noaa.gov/fews/fewsdata/africa/arc2/geotiff"

    def __init__(self, output_dir="data/arc2", log_file=None, verbose=False, max_workers=6):
        self.logger = get_logger("arc2", log_file, verbose)


    def _parse_date(self, date):
        return datetime.strptime(date, "%Y-%m-%d") if isinstance(date, str) else date

    def build_url(self, date):
        return f"{self.BASE_URL}/africa_arc.{date.strftime('%Y%m%d')}.tif.zip"

    def download_and_extract(self, date, tif_dir):
        url = self.build_url(date)
        zip_name = url.split("/")[-1]
        tif_name = zip_name.replace(".zip", "")
        tif_path = tif_dir / tif_name

        if tif_path.exists():
            return tif_path

        try:
            self.logger.debug(f"⬇ Downloading {url}")
            response = requests.get(url, stream=True, timeout=30)
            response.raise_for_status()
            with ZipFile(BytesIO(response.content)) as thezip:
                thezip.extractall(tif_dir)
            return tif_path
        except Exception as e:
            self.logger.warning(f"Failed for {zip_name}: {e}")
            return None

    def convert_all_to_netcdf_per_year(self, files_by_year: dict, output_dir):
        for year, tif_files in files_by_year.items():
            output_nc = output_dir / f"arc2_{year}.nc"
            if output_nc.exists():
                self.logger.info(f"{output_nc} already exists. Skipping.")
                continue

            datasets = []
            for tif_file in sorted(tif_files):
                try:
                    date_str = tif_file.stem.split(".")[-1]
                    timestamp = datetime.strptime(date_str, "%Y%m%d")
                    ds = rioxarray.open_rasterio(tif_file)
                    ds = ds.squeeze("band", drop=True)
                    ds = ds.expand_dims(time=[timestamp])
                    datasets.append(ds)
                except Exception as e:
                    self.logger.error(f"❌ Failed to read {tif_file.name}: {e}")

            if datasets:
                try:
                    merged = xr.concat(datasets, dim="time")
                    merged.name = "precip"
                    merged.to_netcdf(output_nc)
                    self.logger.info(f"🎯 Yearly NetCDF saved: {output_nc}")
                except Exception as e:
                    self.logger.error(f"❌ Merge failed for {year}: {e}")

    def download(self, **kwargs):
        try:
            start_date = kwargs["start_date"]
            end_date = kwargs["end_date"]
            output_dir = kwargs["output_dir"] + "/PR"
            max_workers = kwargs.get("max_workers", 6)
        except KeyError as e:
            raise ValueError(f"Missing required argument: {e}")
        
        Path(output_dir).mkdir(parents=True, exist_ok=True)

        tif_dir = output_dir / "tifs"
        tif_dir.mkdir(parents=True, exist_ok=True)

        start = self._parse_date(start_date)
        end = self._parse_date(end_date)
        all_dates = list(self._daterange(start, end))

        self.logger.info(f"Downloading {len(all_dates)} daily ARC2 files with {self.max_workers} workers...")

        files_by_year = defaultdict(list)

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {executor.submit(self.download_and_extract, date, tif_dir): date for date in all_dates}

            for future in as_completed(futures):
                date = futures[future]
                tif_path = future.result()
                if tif_path:
                    files_by_year[date.year].append(tif_path)

        self.convert_all_to_netcdf_per_year(files_by_year, output_dir)

    def _daterange(self, start, end):
        while start <= end:
            yield start
            start += timedelta(days=1)
