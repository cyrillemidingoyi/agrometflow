# -*- coding: utf-8 -*-
from glob import glob
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
import requests


remoteurl = 'https://data.chc.ucsb.edu/products/CHIRPS-2.0'
#https://github.com/zhou100/chirps_rainfall/blob/master/ExtractNC/extract_chirps_nc_global.R

class ChirpsDownloader(ClimateSource):
    def __init__(self, log_file=None, verbose=False):
        self.logger = get_logger(__name__, log_file=log_file, verbose=verbose)

    def download(self, **kwargs):
        """
        Télécharge des données Chirps par bbox et les enregistre en NetCDF, par année.

        Parameters
        ----------
        start_date : str (YYYY-MM-DD)
        end_date : str (YYYY-MM-DD)
        variables : [str]              # Only rainfall
        output_dir : str
        bbox : list [lon_min, lat_min, lon_max, lat_max]
        kwargs : 
        """

        try:
            start_date = kwargs["start_date"]
            end_date = kwargs["end_date"]
            output_dir = kwargs["output_dir"] + "/PR"
        except KeyError as e:
            raise ValueError(f"Missing required argument: {e}")


        Path(output_dir).mkdir(parents=True, exist_ok=True)

        # Extra years
        start_year = pd.to_datetime(start_date).year
        end_year = pd.to_datetime(end_date).year
        self.logger.info(f"Downloading CHIRPS data for Precipitation from {start_year} to {end_year}")

        # Parallel download per year
        max_workers = kwargs.get("max_workers", 4)
        requests = build_request(start_year, end_year, self.logger)
        self.logger.info(f"Requests: {requests}")
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            executor.map(lambda req: fetch(req, output_dir, self.logger), requests)


    def extract(self, localdir, extract_repo, domain=None, point=None, multipoints=None, date=None, start_date=None, end_date=None):
        """Extract Chirps at a point or domain

        Args:
            domain (list, optional): list of 4 values [min_lon, min_lat, max_lon, max_lat]. Defaults to None.
            area_average (list, optional): list of 4 values [min_lon, min_lat, max_lon, max_lat]. Defaults to None.
            point (list, optional): list of two values [lon, lat]. Defaults to None.
            multipoints (list, optional): list of n points. Defaults to None.
            localdir (str): local directory. Defaults to None.
            date (str, optional): specific date for extraction. Defaults to None.
            start_date (str, optional): start date for extraction. Defaults to None.
            end_date (str, optional): end date for extraction. Defaults to None.
            
            
        """

        if date:
            self.logger.info(f"Extracting data for specific date: {date}")
        elif start_date and end_date:
            self.logger.info(f"Extracting data from {start_date} to {end_date}")
        else:
            self.logger.warning("No valid date parameters provided for extraction.")
        

def build_request(startdate, enddate, logger):
    """Create list of CHIRPS filenames to download.
    
    Parameters
    ----------
    startdate : str start date.
    enddate : str
        end date.
    version : float
        Version of CHIRPS rainfall estimates.
    
    Returns
    -------
    list
        List of filenames to process.
    
    """
    files_to_download = []
    dataurl = remoteurl + '/' + 'global_' + "daily" + '/'+ "netcdf" + '/'+ "p05"  
    files_to_download = [(f"{dataurl}/chirps-v2.0.{m}.days_p05.nc", f"chirps-v2.0.{m}.days_p05.nc") for m in range(startdate, enddate+1)  ]
    return files_to_download


def fetch(file_to_download, save_path, logger):
    lnk, filename = file_to_download
    file_path = Path(save_path) / filename
    if os.path.exists(file_path):
        logger.info(f"Already exists: {file_path}")
        return

    try:
        logger.info(f" Downloading {file_path} to {save_path}")
        response = requests.get(lnk, stream=True)
        response.raise_for_status()
        with open(file_path, "wb") as f:
            f.write(response.content)
        with open(file_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
            return
    except Exception as e:
        logger.error(f"Failed for {filename}: {e}")
        return




    


    


          
                    
                     
  
