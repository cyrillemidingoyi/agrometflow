import gzip
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from agrometflow.climate.ghcnd import GHCNDDownloader


class _FakeResponse:
    def __init__(self, content: bytes, status_code: int = 200):
        self.content = content
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class TestGhcndDownloader(unittest.TestCase):
    def test_download_attaches_station_coordinates(self):
        csv_text = "\n".join(
            [
                "AGM00060360,20200101,TMAX,250,,, ,",
                "AGM00060360,20200101,TMIN,100,,, ,",
                "AGM00060360,20200101,PRCP,12,,, ,",
            ]
        ).replace(" ,", ",")
        payload = gzip.compress(csv_text.encode("utf-8"))
        stations = pd.DataFrame(
            {
                "station_id": ["AGM00060360"],
                "lat": [36.71],
                "lon": [3.25],
                "elevation": [24.0],
                "name": ["ALGIERS"],
                "country": ["AG"],
            }
        )

        with tempfile.TemporaryDirectory() as tmp:
            out_dir = Path(tmp)
            downloader = GHCNDDownloader()

            with patch.object(GHCNDDownloader, "get_stations", return_value=stations):
                with patch("agrometflow.climate.ghcnd.requests.get", return_value=_FakeResponse(payload)):
                    downloader.download(
                        station_ids=["AGM00060360"],
                        start_date="2020-01-01",
                        end_date="2020-01-02",
                        variables=["TMAX", "TMIN", "PRCP"],
                        output_dir=out_dir,
                        max_workers=1,
                    )

            self.assertIsNotNone(downloader.data)
            self.assertIn("lat", downloader.data.columns)
            self.assertIn("lon", downloader.data.columns)
            self.assertIn("elevation", downloader.data.columns)
            self.assertEqual(downloader.data["lat"].iloc[0], 36.71)
            self.assertEqual(downloader.data["lon"].iloc[0], 3.25)
            self.assertEqual(downloader.data["elevation"].iloc[0], 24.0)

            saved = pd.read_csv(out_dir / "ghcnd_20200101_20200102.csv")
            self.assertEqual(saved["lat"].iloc[0], 36.71)
            self.assertEqual(saved["lon"].iloc[0], 3.25)
            self.assertEqual(saved["elevation"].iloc[0], 24.0)


if __name__ == "__main__":
    unittest.main()
