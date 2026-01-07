# -*- coding: utf-8 -*-
import xarray as xr
import numpy as np
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from agrometflow.utils import get_logger

drive = "https://drive.google.com/drive/folders/1Kok05OPVESTpyyan7NafR-2WwuSJ4TO9"

class MswepDownloader:
    def __init__(self, folder_id, output_dir="data/mswep", log_file=None, verbose=False, max_workers=4):
        self.folder_id = folder_id
        self.output_dir = Path(output_dir)
        self.raw_dir = self.output_dir / "daily"
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.logger = get_logger("agrometflow.mswep", log_file=log_file, verbose=verbose)
        self.max_workers = max_workers
        #self.drive = self._auth_drive()

    '''def _auth_drive(self):
        gauth = GoogleAuth()
        gauth.LocalWebserverAuth()
        return GoogleDrive(gauth)'''

    def _daterange(self, start_date, end_date):
        while start_date <= end_date:
            yield start_date
            start_date += timedelta(days=1)

    def _build_filename(self, date):
        year = date.strftime("%Y")
        doy = date.strftime("%j")
        return f"{year}{doy}.nc"

    def _find_file_in_drive(self, filename):
        query = f"'{self.folder_id}' in parents and title='{filename}'"
        file_list = self.drive.ListFile({'q': query}).GetList()
        return file_list[0] if file_list else None

    def _download_file(self, date):
        filename = self._build_filename(date)
        out_path = self.raw_dir / filename
        if out_path.exists():
            self.logger.debug(f"✔ Already exists: {filename}")
            return out_path, date

        drive_file = self._find_file_in_drive(filename)
        if not drive_file:
            self.logger.warning(f"⚠️ File not found: {filename}")
            return None, date

        try:
            drive_file.GetContentFile(str(out_path))
            self.logger.info(f"✅ Downloaded: {filename}")
            return out_path, date
        except Exception as e:
            self.logger.error(f"❌ Failed to download {filename}: {e}")
            return None, date

    def _merge_yearly(self, files_by_year):
        for year, file_date_pairs in files_by_year.items():
            outfile = self.output_dir / f"mswep_{year}.nc"
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
                    self.logger.error(f"⚠️ Error reading {f.name}: {e}")

            if arrays:
                combined = xr.concat(arrays, dim="time")
                combined.to_netcdf(outfile)
                self.logger.info(f"💾 Saved yearly file: {outfile}")

    def download(self, start_date, end_date):
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        dates = list(self._daterange(start, end))

        self.logger.info(f"🚀 Downloading MSWEP data from {start.date()} to {end.date()}")

        files_by_year = {}

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {executor.submit(self._download_file, date): date for date in dates}
            for future in as_completed(futures):
                f, date = future.result()
                if f:
                    files_by_year.setdefault(date.year, []).append((f, date))

        self._merge_yearly(files_by_year)
