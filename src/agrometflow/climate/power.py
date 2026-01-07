import requests
import pandas as pd
from pathlib import Path
from datetime import datetime
from agrometflow.climate.base import ClimateSource
from agrometflow.utils import get_logger
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

#logger = get_logger(__name__)


class PowerDownloader(ClimateSource):
    """
    Downloader for NASA POWER daily climate data using the REST API.
    API doc: https://power.larc.nasa.gov/docs/services/api/
    """

    BASE_URL_POINT = "https://power.larc.nasa.gov/api/temporal/daily/point"
    BASE_URL_REGIONAL = "https://power.larc.nasa.gov/api/temporal/daily/regional"

    def __init__(self, log_file=None, verbose=False):
        self.logger = get_logger(__name__, log_file=log_file, verbose=verbose)
        self.data = None

    def download(self,**kwargs):
        """
        Downloads data for a single point (center of bbox).

        Parameters
        ----------
        start_date : str
            Start date in 'YYYY-MM-DD' format
        end_date : str
            End date in 'YYYY-MM-DD' format
        variables : list of str
            List of variable names to download
        output_dir : str or Path
            Directory where the downloaded files should be saved
        kwargs : dict
            Additional arguments such as 'bbox' or 'resolution'
            - bbox : tuple (min_lon, min_lat, max_lon, max_lat)
            - resolution : float
                Desired output spatial resolution in degrees
            - points : list of tuples (lat, lon)
            - format : str
        """
        try:
            start_date = kwargs["start_date"]
            end_date = kwargs["end_date"]
            variables = kwargs["variables"]
            output_dir = kwargs["output_dir"]
        except KeyError as e:
            raise ValueError(f"Missing required argument: {e}")

        # Format dates
        start = datetime.strptime(start_date, "%Y-%m-%d").strftime("%Y%m%d")
        end = datetime.strptime(end_date, "%Y-%m-%d").strftime("%Y%m%d")
        
        all_data = []
        if "points" in kwargs:
            points = kwargs["points"]
            all_data = []

            with ThreadPoolExecutor(max_workers=8) as executor:
                futures = [
                    executor.submit(
                        _fetch_power_point, lat, lon, start, end, variables, self.BASE_URL_POINT, self.logger
                    )
                    for lat, lon in points
                ]

                for f in tqdm(as_completed(futures), total=len(futures), desc="Downloading NASAPOWER data"):
                    result = f.result()
                    if result is not None:
                        all_data.append(result)

            if not all_data:
                raise RuntimeError("No data fetched from POWER.")

            full_df = pd.concat(all_data, ignore_index=True)
            self.data = full_df
            # Sauvegarde
            Path(output_dir).mkdir(parents=True, exist_ok=True)
            filename = Path(output_dir) / f"power_{start}_{end}.csv"
            full_df.to_csv(filename, index=False)
            self.logger.info(f"Data saved to {filename}")
        
        elif "bbox" in kwargs:
            requests_list = build_requests_box(self.BASE_URL_REGIONAL, variables, start_date, end_date, kwargs["bbox"], output_dir)
            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = [
                    executor.submit(fetch_and_save, url, params, path)
                    for url, params, path in requests_list
                ]
                for f in tqdm(futures):
                    try:
                        f.result()
                    except Exception as e:
                        self.logger.error(f"Failed to fetch data — {e}")
                return


    def extract(self, variables=None, start_date=None, end_date=None, as_long=False, **kwargs):
        
        source = None
        if "source" in kwargs:
            source = kwargs["source"]
        if self.data is None and source is None:
            raise ValueError("No data available. Run download() first.")

        df = self.data.copy()
        df["date"] = pd.to_datetime(df["date"])

        if start_date:
            df = df[df["date"] >= pd.to_datetime(start_date)]
        if end_date:
            df = df[df["date"] <= pd.to_datetime(end_date)]

        if variables:
            cols = ["date", "lat", "lon"] + [v for v in variables if v in df.columns]
            df = df[cols]

        if as_long:
            df = df.melt(id_vars=["date", "lat", "lon"], var_name="variable", value_name="value")

        return df

    @staticmethod
    def _json_to_dataframe2(records):
        df = pd.DataFrame()
        for var, daily_values in records.items():
            series = pd.Series(daily_values).rename(var)
            df = pd.concat([df, series], axis=1)
        df.index.name = "date"
        df.reset_index(inplace=True)
        return df
    

    def _json_to_dataframe(self, records):
        df = pd.DataFrame()
        for var, daily_values in records.items():
            series = pd.Series(daily_values).rename(var)
            df = pd.concat([df, series], axis=1)
        df.index.name = "date"
        df.reset_index(inplace=True)
        return df


def build_requests_box(base_url, variables, start_date, end_date, bbox, output_dir):
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)
    output_dir = Path(output_dir)
    requests_list = []
    for var in variables:
        for year in range(start_date.year, end_date.year + 1):
            start = max(start_date, pd.Timestamp(f"{year}-01-01"))
            end = min(end_date, pd.Timestamp(f"{year}-12-31"))

            params = {
                            "latitude-min": bbox[1],
                            "latitude-max": bbox[3],
                            "longitude-min": bbox[0],
                            "longitude-max": bbox[2],
                            "parameters": var[0],
                            "community": "AG", 
                            "start": start.strftime("%Y%m%d"),
                            "end": end.strftime("%Y%m%d"),
                            "format": "netcdf"
                        }
            save_path = output_dir / var[1] / f"{year}.nc"
            requests_list.append((base_url, params, save_path))

    return requests_list


def fetch_and_save(base_url, params, save_path):
    save_path.parent.mkdir(parents=True, exist_ok=True)

    response = requests.get(base_url, params=params)
    response.raise_for_status()

    with open(save_path, "wb") as f:
        f.write(response.content)

    return save_path

def _fetch_power_point(lat, lon, start, end, variables, base_url, logger):
    realvar = [el[1] for el in variables]
    source_variables = [el[0] for el in variables]
    try:
        params = {
            "parameters": ",".join(source_variables),
            "community": "AG",
            "longitude": lon,
            "latitude": lat,
            "start": start,
            "end": end,
            "format": "JSON"
        }
        logger.info(f"Fetching POWER data for ({lat}, {lon})")

        response = requests.get(base_url, params=params)
        logger.info(f"Response status: {response.url}")
        response.raise_for_status()
        records = response.json()['properties']['parameter']
        df = PowerDownloader._json_to_dataframe2(records)
        df["lat"] = lat
        df["lon"] = lon
        df = df.rename(columns={var: realvar[i] for i, var in enumerate(source_variables)})
        return df

    except Exception as e:
        logger.error(f"Failed for ({lat}, {lon}): {e}")
        return None