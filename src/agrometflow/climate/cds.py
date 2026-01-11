from .base import ClimateSource
import cdsapi
import pandas as pd
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from agrometflow.utils import get_logger
import zipfile
import xarray as xr
import os
import tempfile
from pathlib import Path

# Disable tqdm notebook mode to avoid issues in Binder/remote environments
os.environ["TQDM_NOTEBOOK_DISABLE"] = "1"

class CDSDownloader(ClimateSource):
    def __init__(self, log_file=None, verbose=False):
        self.logger = get_logger(__name__, log_file=log_file, verbose=verbose)

    def download(self, **kwargs):
        """
        Télécharge des données ERA5 par bbox et les enregistre en NetCDF, par année.

        Parameters
        ----------
        start_date : str (YYYY-MM-DD)
        end_date : str (YYYY-MM-DD)
        variables : list of str (must match CDS ERA5 variable names)
        output_dir : str
        bbox : list [south, north, west, east]
        kwargs : e.g. product_type='reanalysis', dataset='reanalysis-era5-single-levels'
        """
        
        try:
            start_date = kwargs["start_date"]
            end_date = kwargs["end_date"]
            variables = kwargs["variables"]
            output_dir = kwargs["output_dir"]
            bbox = kwargs.get("bbox", None)
            product = kwargs.get("product", "era5")
        except KeyError as e:
            raise ValueError(f"Missing required argument: {e}")

        if product.lower() == "era5": 
            dataset = kwargs.get("dataset", "sis-agrometeorological-indicators") # "reanalysis-era5-single-levels" 
            self.logger.info("Using AgERA5 dataset")     
                
        Path(output_dir).mkdir(parents=True, exist_ok=True)

        # Extra years
        start_year = pd.to_datetime(start_date).year
        end_year = pd.to_datetime(end_date).year
        years = list(range(start_year, end_year + 1))

        self.logger.info(f"Downloading {product} data for {variables} from {start_year} to {end_year}")
        self.logger.info(f"BBOX: {bbox}, Dataset: {dataset}")
        self.client = cdsapi.Client(kwargs["url"], kwargs["key"],quiet=True)

        # Parallel download per year
        max_workers = kwargs.get("max_workers", 4)
        self.logger.info(f"Max workers: {max_workers}")
        
        requests = build_requests(variables, years, output_dir, bbox)
        self.logger.info(f"Requests: {requests}")
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            executor.map(lambda req: fetch_and_merge(req, self.client, self.logger, dataset), requests)

    def extract(self, variables=None, start_date=None, end_date=None, as_long=False, **kwargs):
        return super().extract(variables, start_date, end_date, as_long, **kwargs)
    
    
    
def build_requests(variables, years, output_dir, bbox):
    """
    Retourne une liste de requêtes (req_dict, zip_path, final_netcdf_path).
    """

    requests = []

    for var in variables:
        var_name = var[0]["variable"]
        stat = var[0].get("statistic")
        var_dir = Path(output_dir) / var[1]
        var_dir.mkdir(parents=True, exist_ok=True)

        for year in years:
            zip_path = var_dir / f"agera5_{var[1]}_{year}.zip"
            nc_path = var_dir / f"agera5_{var[1]}_{year}.nc"
            if nc_path.exists():
                continue
            request = {
                "format": "zip",
                "variable": [var_name],
                "year": [str(year)],
                "month": [f"{m:02}" for m in range(1, 13)],
                "day": [f"{d:02}" for d in range(1, 32)]
            }
            if stat:
                request["statistic"] = [stat]
            request["area"] = [bbox[3], bbox[0], bbox[1], bbox[2]]  
            request["version"] = "1_1"
            requests.append((request, str(zip_path), str(nc_path)))
    return requests




def fetch_and_merge(request_tuple, client, logger, dataset):
    request, zip_path, final_nc = request_tuple

    if os.path.exists(final_nc):
        logger.info(f"Already exists: {final_nc}")
        return

    try:
        logger.info(f" Downloading zip to {zip_path}")
        client.retrieve(dataset, request, zip_path)

        with tempfile.TemporaryDirectory() as tmpdir:
            logger.info(f"Unzipping to {tmpdir}")
            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                zip_ref.extractall(tmpdir)

            nc_files = sorted(Path(tmpdir).glob("*.nc"))
            logger.info(f"Merging {len(nc_files)} NetCDF files")
            ds = xr.open_mfdataset(nc_files, combine="by_coords")
            ds.to_netcdf(final_nc)
            logger.info(f"Merged NetCDF saved to {final_nc}")
            os.remove(zip_path) 

    except Exception as e:
        logger.error(f"Failed for {zip_path}: {e}")

