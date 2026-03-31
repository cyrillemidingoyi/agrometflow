import tempfile
import unittest
from pathlib import Path

import numpy as np
import pandas as pd

from agrometflow.dataquality.qc import climatic_outliers, run_qc_pipeline_from_config


class TestQcPipelineFromConfig(unittest.TestCase):
    def test_run_qc_pipeline_from_config_daily(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)

            # Minimal daily dataset with one duplicate date and one obvious out-of-range value.
            df = pd.DataFrame(
                {
                    "Year": [2020, 2020, 2020, 2020],
                    "Month": [1, 1, 1, 1],
                    "Day": [1, 2, 2, 3],
                    "Tx": [25.0, 26.0, 60.0, 27.0],
                    "Tn": [20.0, 21.0, 22.0, 23.0],
                }
            )

            data_csv = tmp_path / "meteo_daily.csv"
            summary_csv = tmp_path / "summary.csv"
            flags_csv = tmp_path / "all_flags.csv"
            out_dir = tmp_path / "qc_flags"
            cfg_path = tmp_path / "qc_config.yml"

            df.to_csv(data_csv, index=False)

            cfg_path.write_text(
                "\n".join(
                    [
                        'station_id: "station_test"',
                        f'data: "{data_csv.as_posix()}"',
                        'frequency: "daily"',
                        f'outpath: "{out_dir.as_posix()}"',
                        'variable_cols: ["Tx", "Tn"]',
                        'units_map:',
                        '  Tx: "C"',
                        '  Tn: "C"',
                        'tests:',
                        '  daily_out_of_range:',
                        '    tmax_upper: 45',
                        'outputs:',
                        f'  summary_csv: "{summary_csv.as_posix()}"',
                        f'  flags_csv: "{flags_csv.as_posix()}"',
                    ]
                ),
                encoding="utf-8",
            )

            result = run_qc_pipeline_from_config(cfg_path)

            self.assertIn("summary", result)
            self.assertIn("all_flags", result)
            self.assertFalse(result["all_flags"].empty)
            self.assertIn("daily_out_of_range", set(result["all_flags"]["Test"]))
            self.assertIn("duplicate_dates", set(result["all_flags"]["Test"]))

            self.assertTrue(summary_csv.exists())
            self.assertTrue(flags_csv.exists())

            # Intermediate flags file should also be created for at least Tx.
            tx_flag_file = out_dir / "qc_station_test_Tx_daily.txt"
            self.assertTrue(tx_flag_file.exists())

    def test_climatic_outliers_bplot_writes_pdf(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)

            n = 1826
            rng = np.random.default_rng(42)
            df = pd.DataFrame(
                {
                    "Year": np.repeat(2020, n),
                    "Month": (np.arange(n) % 12) + 1,
                    "Day": (np.arange(n) % 28) + 1,
                    "Tx": rng.normal(25.0, 4.0, size=n),
                }
            )

            out_pdf = tmp_path / "outliers_plot.pdf"
            out = climatic_outliers(
                df,
                var_name="Tx",
                station_id="station_test",
                units="C",
                bplot=True,
                outfile=out_pdf,
            )

            self.assertTrue(out_pdf.exists())
            self.assertGreater(out_pdf.stat().st_size, 0)
            self.assertIsInstance(out, pd.DataFrame)


if __name__ == "__main__":
    unittest.main()
