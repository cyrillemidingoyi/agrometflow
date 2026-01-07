import os
import requests
import xarray as xr
from datetime import datetime, timedelta
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from agrometflow.utils import get_logger
import numpy as np


class ImergDownloader:
    """
    Downloader for NASA IMERG V6-Late Run daily precipitation data.
    Downloads from NASA GES DISC (NetCDF format).
    """

    BASE_URL = "https://gpm1.gesdisc.eosdis.nasa.gov/data/GPM_L3/GPM_3IMERGDL.07"

    def __init__(self, output_dir="data/imerg", log_file=None, verbose=False, max_workers=4, token=None):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.logger = get_logger("agrometflow.imerg", log_file=log_file, verbose=verbose)
        self.max_workers = max_workers
        self.token = token or os.getenv("NASA_EARTHDATA_TOKEN")

    def _daterange(self, start_date, end_date):
        while start_date <= end_date:
            yield start_date
            start_date += timedelta(days=1)

    def _get_url_and_filename(self, date):
        yyyy = date.strftime("%Y")
        mm = date.strftime("%m")
        dd = date.strftime("%d")
        yyyymmdd = date.strftime("%Y%m%d")
        folder = f"{self.BASE_URL}/{yyyy}/{mm}"
        filename = f"3B-DAY-L.MS.MRG.3IMERG.{yyyymmdd}-S000000-E235959.V06.nc4"
        return f"{folder}/{filename}", filename

    def _download_file(self, date):
        url, filename = self._get_url_and_filename(date)
        local_path = self.output_dir / filename
        if local_path.exists():
            self.logger.debug(f"Already downloaded: {filename}")
            return local_path, date

        try:
            headers = {"Authorization": f"Bearer {self.token}"} if self.token else {}
            with requests.get(url, headers=headers, stream=True, timeout=60) as r:
                r.raise_for_status()
                with open(local_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
            self.logger.info(f"Downloaded: {filename}")
            return local_path, date
        except Exception as e:
            self.logger.error(f"Failed to download {filename}: {e}")
            return None, date

    def _merge_yearly(self, files_by_year):
        for year, file_date_pairs in files_by_year.items():
            outfile = self.output_dir / f"imerg_{year}.nc"
            if outfile.exists():
                self.logger.info(f"⏩ Skipping merge for {year}, already exists.")
                continue

            arrays = []
            for f, date in sorted(file_date_pairs, key=lambda x: x[1]):
                try:
                    ds = xr.open_dataset(f)
                    ds = ds.expand_dims(time=[np.datetime64(date)])
                    arrays.append(ds)
                except Exception as e:
                    self.logger.error(f"⚠️ Failed to read {f.name}: {e}")

            if arrays:
                combined = xr.concat(arrays, dim="time")
                combined.to_netcdf(outfile)
                self.logger.info(f"💾 Saved yearly file: {outfile}")

    def download(self, start_date, end_date):
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        dates = list(self._daterange(start, end))

        self.logger.info(f"🚀 Downloading IMERG data from {start.date()} to {end.date()}")

        files_by_year = {}

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {executor.submit(self._download_file, date): date for date in dates}
            for future in as_completed(futures):
                f, date = future.result()
                if f:
                    files_by_year.setdefault(date.year, []).append((f, date))

        self._merge_yearly(files_by_year)
