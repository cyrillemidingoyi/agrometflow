import numpy as np
import xarray as xr
import requests
import gzip
import shutil
from pathlib import Path
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from agrometflow.utils import get_logger


class PersiannDownloader:
    BASE_URL = "https://persiann.eng.uci.edu/CHRSdata/PERSIANN/daily"

    def __init__(self, output_dir="data/persiann", log_file=None, verbose=False, max_workers=6):
        self.output_dir = Path(output_dir)
        self.raw_dir = self.output_dir / "bin"
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.max_workers = max_workers
        self.logger = get_logger("agrometflow.persiann", log_file=log_file, verbose=verbose)

    def build_filename(self, date):
        year = date.strftime("%y")
        doy = date.strftime("%j")
        return f"ms6s4_d{year}{doy}.bin.gz"

    def build_url(self, date):
        return f"{self.BASE_URL}/{self.build_filename(date)}"

    def _download_and_extract(self, date):
        url = self.build_url(date)
        gz_filename = self.build_filename(date)
        gz_path = self.raw_dir / gz_filename
        bin_path = self.raw_dir / gz_filename.replace(".gz", "")

        if bin_path.exists():
            return bin_path

        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            with open(gz_path, "wb") as f:
                f.write(response.content)

            with gzip.open(gz_path, "rb") as f_in, open(bin_path, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)

            self.logger.info(f"✅ Downloaded and extracted: {gz_filename}")
            return bin_path
        except Exception as e:
            self.logger.error(f"❌ Error downloading {gz_filename}: {e}")
            return None

    def _daterange(self, start_date, end_date):
        while start_date <= end_date:
            yield start_date
            start_date += timedelta(days=1)

    def convert_bin_to_xarray(self, bin_file, date):
        rows, cols = 480, 1440
        nodata = -9999
        dtype = ">f4"

        data = np.fromfile(bin_file, dtype=dtype).reshape((rows, cols))
        data = np.where(data == nodata, np.nan, data)

        lats = np.linspace(59.875, -59.875, rows)
        lons = np.linspace(0.125, 359.875, cols)

        da = xr.DataArray(
            data,
            dims=["lat", "lon"],
            coords={"lat": lats, "lon": lons},
            name="precip"
        )
        da = da.expand_dims(time=[np.datetime64(date)])
        return da

    def convert_downloaded_to_netcdf(self, bin_files_by_year):
        for year, file_date_pairs in bin_files_by_year.items():
            output_nc = self.output_dir / f"persiann_{year}.nc"
            if output_nc.exists():
                self.logger.info(f"⏩ Skipping {year}, NetCDF already exists.")
                continue

            arrays = []
            for bin_file, date in sorted(file_date_pairs, key=lambda x: x[1]):
                try:
                    da = self.convert_bin_to_xarray(bin_file, date)
                    arrays.append(da)
                except Exception as e:
                    self.logger.error(f"❌ Failed to convert {bin_file.name}: {e}")

            if arrays:
                combined = xr.concat(arrays, dim="time")
                combined.to_netcdf(output_nc)
                self.logger.info(f"💾 Saved NetCDF: {output_nc}")

    def download(self, start_date, end_date):
        start = datetime.strptime(start_date, "%Y-%m-%d") if isinstance(start_date, str) else start_date
        end = datetime.strptime(end_date, "%Y-%m-%d") if isinstance(end_date, str) else end_date
        dates = list(self._daterange(start, end))

        self.logger.info(f"🚀 Downloading PERSIANN data from {start.date()} to {end.date()} using {self.max_workers} workers.")

        bin_files_by_year = {}

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {executor.submit(self._download_and_extract, date): date for date in dates}
            for future in as_completed(futures):
                bin_path = future.result()
                if bin_path:
                    try:
                        fname = bin_path.stem
                        yy = int(fname[8:10])
                        doy = int(fname[10:13])
                        year = 2000 + yy if yy < 50 else 1900 + yy
                        date = datetime.strptime(f"{year}-{doy:03}", "%Y-%j")
                        bin_files_by_year.setdefault(year, []).append((bin_path, date))
                    except Exception as e:
                        self.logger.warning(f"Failed to parse date from {bin_path.name}: {e}")

        self.convert_downloaded_to_netcdf(bin_files_by_year)
    
    def extract(self, start_date, end_date):
        """
        Extracts PERSIANN data from binary files and saves them as NetCDF files.
        
        Args:
            start_date (str or datetime): Start date in 'YYYY-MM-DD' format or datetime object.
            end_date (str or datetime): End date in 'YYYY-MM-DD' format or datetime object.
        """
        pass
