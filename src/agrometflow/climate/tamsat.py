import zipfile
import requests
import xarray as xr
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

from agrometflow.utils import get_logger

class TamsatDownloader:
    BASE_URL = "http://gws-access.jasmin.ac.uk/public/tamsat/rfe/data_zipped/v3.1/daily"

    def __init__(self, output_dir="data/tamsat", log_file=None, verbose=False, max_workers=4):
        self.output_dir = Path(output_dir)
        self.logger = get_logger("tamsat", log_file, verbose)
        self.max_workers = max_workers

    def build_url(self, year):
        return f"{self.BASE_URL}/TAMSATv3.1_rfe_daily_{year}.zip"

    def process_year(self, year):
        tmp_dir = self.output_dir / "tmp" / str(year)
        tmp_dir.mkdir(parents=True, exist_ok=True)

        zip_path = tmp_dir / f"{year}.zip"
        output_file = self.output_dir / f"tamsat_{year}.nc"

        if output_file.exists():
            self.logger.info(f" Skipping {year}, already processed.")
            return

        # Download
        url = self.build_url(year)
        try:
            self.logger.info(f"⬇ Downloading {url}")
            response = requests.get(url, stream=True)
            response.raise_for_status()
            with open(zip_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
        except Exception as e:
            self.logger.error(f" Failed to download {url} — {e}")
            return

        # Extract
        try:
            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                zip_ref.extractall(tmp_dir)
            self.logger.info(f" Extracted {year}.zip")
        except Exception as e:
            self.logger.error(f" Extraction failed for {year}: {e}")
            return

        # Merge NetCDFs
        try:
            nc_files = sorted(tmp_dir.glob(f"{year}*.nc"))
            if not nc_files:
                self.logger.warning(f"No NetCDF files found for {year}")
                return
            ds = xr.open_mfdataset(nc_files, combine="by_coords")
            ds.to_netcdf(output_file)
            self.logger.info(f" Yearly NetCDF saved: {output_file}")
        except Exception as e:
            self.logger.error(f" Merge failed for {year}: {e}")
            return

        # Cleanup
        for f in tmp_dir.glob("*.nc"):
            f.unlink()
        zip_path.unlink()
        tmp_dir.rmdir()

    def download(self, start_year, end_year):
        years = list(range(start_year, end_year + 1))
        self.logger.info(f"🔁 Scheduling downloads for years: {years}")

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            executor.map(self.process_year, years)
