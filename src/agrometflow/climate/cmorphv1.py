import gzip
import shutil
import requests
from datetime import datetime, timedelta
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from agrometflow.utils import get_logger


class Cmorphv1Downloader:
    """
    Downloader for CMORPH V1.0BETA daily precipitation data.
    Source: https://ftp.cpc.ncep.noaa.gov/precip/CMORPH_V1.0/BLD/0.25deg-DLY_EOD/GLB
    """

    BASE_URL = "https://ftp.cpc.ncep.noaa.gov/precip/CMORPH_V1.0/BLD/0.25deg-DLY_EOD/GLB"

    def __init__(self, output_dir="data/cmorph", log_file=None, verbose=False, max_workers=4):
        self.output_dir = Path(output_dir)
        self.raw_dir = self.output_dir / "bin"
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.logger = get_logger("agrometflow.cmorph", log_file=log_file, verbose=verbose)
        self.max_workers = max_workers

    def _daterange(self, start_date, end_date):
        while start_date <= end_date:
            yield start_date
            start_date += timedelta(days=1)

    def _build_url(self, date):
        year = date.strftime("%Y")
        yearmonth = date.strftime("%Y%m")
        day = date.strftime("%Y%m%d")
        filename = f"CMORPH_V1.0BETA_BLD_0.25deg-DLY_EOD_{day}.gz"
        url = f"{self.BASE_URL}/{year}/{yearmonth}/{filename}"
        return url, filename

    def _download_and_extract(self, date):
        url, filename = self._build_url(date)
        gz_path = self.raw_dir / filename
        bin_path = gz_path.with_suffix("")  # remove .gz

        if bin_path.exists():
            self.logger.debug(f"✔ Already downloaded: {bin_path.name}")
            return

        try:
            response = requests.get(url, stream=True, timeout=60)
            response.raise_for_status()

            with open(gz_path, "wb") as f:
                f.write(response.content)

            with gzip.open(gz_path, "rb") as f_in, open(bin_path, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)

            self.logger.info(f"✅ Downloaded and extracted: {filename}")
        except Exception as e:
            self.logger.error(f"❌ Failed to download {url}: {e}")

    def download(self, start_date, end_date):
        """Download CMORPH data from start_date to end_date (inclusive)."""
        start = datetime.strptime(start_date, "%Y-%m-%d") if isinstance(start_date, str) else start_date
        end = datetime.strptime(end_date, "%Y-%m-%d") if isinstance(end_date, str) else end_date
        dates = list(self._daterange(start, end))

        self.logger.info(f"📦 Downloading CMORPH data for {len(dates)} days with {self.max_workers} workers...")

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(self._download_and_extract, d) for d in dates]
            for future in as_completed(futures):
                _ = future.result()
