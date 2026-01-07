import unittest
from unittest.mock import patch, MagicMock
from agrometflow.climate.power import PowerDownloader
import pandas as pd

class TestPowerDownloader(unittest.TestCase):

    @patch("agrometflow.climate.nasapower.requests.get")
    def test_download_and_extract_point(self, mock_get):
        # Exemple de réponse POWER simulée
        fake_response = {
            "properties": {
                "parameter": {
                    "T2M": {"2022-01-01": 25.5, "2022-01-02": 26.0},
                    "PRECTOT": {"2022-01-01": 5.1, "2022-01-02": 0.0}
                }
            }
        }

        # Préparer le mock
        mock_resp = MagicMock()
        mock_resp.json.return_value = fake_response
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        # Créer une instance avec verbose désactivé
        downloader = PowerDownloader(verbose=False)

        # Appel de la méthode avec un point simple
        downloader.download(
            start_date="2022-01-01",
            end_date="2022-01-02",
            variables=["T2M", "PRECTOT"],
            output_dir="tests/output",
            points=[(12.34, -1.23)]
        )

        # Vérification que les données ont bien été enregistrées
        df = downloader.extract()

        self.assertIsInstance(df, pd.DataFrame)
        self.assertIn("T2M", df.columns)
        self.assertIn("PRECTOT", df.columns)
        self.assertEqual(len(df), 2)
        self.assertTrue((df["lat"] == 12.34).all())
        self.assertTrue((df["lon"] == -1.23).all())

if __name__ == "__main__":
    unittest.main()
