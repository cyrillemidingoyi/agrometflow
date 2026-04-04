import tempfile
import unittest
from pathlib import Path

import numpy as np
import pandas as pd

from agrometflow.dataquality.qc import (
    check_units,
    climatic_outliers,
    run_qc_pipeline_from_config,
    wmo_gross_errors,
    wmo_time_consistency,
)


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

    def test_climatic_outliers_filters_single_station_id_without_station_col(self):
        dates = pd.date_range("2015-01-01", "2019-12-31", freq="D")
        station_a = pd.DataFrame(
            {
                "station": "A",
                "Year": dates.year,
                "Month": dates.month,
                "Day": dates.day,
                "rr": 1.0,
            }
        )
        station_b = pd.DataFrame(
            {
                "station": "B",
                "Year": dates.year,
                "Month": dates.month,
                "Day": dates.day,
                "rr": 5.0,
            }
        )
        station_a.loc[
            (station_a["Year"] == 2019) & (station_a["Month"] == 1) & (station_a["Day"] == 15),
            "rr",
        ] = 10.0
        df = pd.concat([station_a, station_b], ignore_index=True)

        out = climatic_outliers(df, var_name="rr", station_id="A", units="mm")

        self.assertEqual(out["station"].tolist(), ["A"])
        self.assertEqual(out["Year"].tolist(), [2019])
        self.assertEqual(out["Month"].tolist(), [1])
        self.assertEqual(out["Day"].tolist(), [15])

    def test_climatic_outliers_accepts_station_id_list_without_station_col(self):
        dates = pd.date_range("2015-01-01", "2019-12-31", freq="D")
        station_a = pd.DataFrame(
            {
                "station": "A",
                "Year": dates.year,
                "Month": dates.month,
                "Day": dates.day,
                "rr": 1.0,
            }
        )
        station_b = pd.DataFrame(
            {
                "station": "B",
                "Year": dates.year,
                "Month": dates.month,
                "Day": dates.day,
                "rr": 5.0,
            }
        )
        station_a.loc[
            (station_a["Year"] == 2019) & (station_a["Month"] == 1) & (station_a["Day"] == 15),
            "rr",
        ] = 10.0
        station_b.loc[
            (station_b["Year"] == 2019) & (station_b["Month"] == 2) & (station_b["Day"] == 20),
            "rr",
        ] = 20.0
        df = pd.concat([station_a, station_b], ignore_index=True)

        out = climatic_outliers(df, var_name="rr", station_id=["A", "B"], units="mm")

        self.assertEqual(sorted(out["station"].tolist()), ["A", "B"])
        self.assertEqual(set(out["Month"]), {1, 2})


class TestWmoGrossErrors(unittest.TestCase):
    def test_check_units_converts_pressure_to_hpa(self):
        values = pd.Series([101325.0, 760.0, 30.0])

        converted_pa = check_units(values.iloc[[0]], "mslp", "Pa")
        converted_mmhg = check_units(values.iloc[[1]], "mslp", "mmHg")
        converted_in = check_units(values.iloc[[2]], "mslp", "in")

        self.assertEqual(converted_pa.iloc[0], 1013.2)
        self.assertEqual(converted_mmhg.iloc[0], 1013.2)
        self.assertEqual(converted_in.iloc[0], 1015.9)

    def test_wmo_gross_errors_ignores_invalid_months(self):
        df = pd.DataFrame(
            {
                "Year": [2020],
                "Month": [13],
                "Day": [1],
                "ta": [45.0],
            }
        )

        out = wmo_gross_errors(df, "ta", lat=60)

        self.assertTrue(out.empty)

    def test_wmo_gross_errors_uses_numeric_station_ids_with_lat_df(self):
        df = pd.DataFrame(
            {
                "station": [101, 101, 202, 202],
                "Year": [2020, 2020, 2020, 2020],
                "Month": [1, 7, 1, 7],
                "Day": [1, 1, 1, 1],
                "ta": [45.0, 45.0, 45.0, 45.0],
            }
        )
        lat_df = pd.DataFrame(
            {
                "station_id": [101, 202],
                "lat": [60.0, -60.0],
            }
        )

        out = wmo_gross_errors(df, "ta", station_col="station", lat_df=lat_df)

        self.assertEqual(sorted(out["station"].tolist()), [101, 101, 202, 202])
        self.assertEqual(set(out["Test"]), {"wmo_gross_errors"})


class TestWmoTimeConsistency(unittest.TestCase):
    def test_wmo_time_consistency_flags_both_endpoints_for_pressure_jump(self):
        df = pd.DataFrame(
            {
                "Year": [2020, 2020, 2020],
                "Month": [1, 1, 1],
                "Day": [1, 1, 1],
                "Hour": [0, 1, 2],
                "Minute": [0, 0, 0],
                "p": [1000.0, 1004.1, 1005.0],
            }
        )

        out = wmo_time_consistency(df, "p", units="hPa")

        self.assertEqual(len(out), 2)
        self.assertEqual(out["Hour"].tolist(), [0, 1])
        self.assertEqual(set(out["Test"]), {"wmo_time_consistency"})

    def test_wmo_time_consistency_uses_stepwise_temperature_tolerance(self):
        df = pd.DataFrame(
            {
                "Year": [2020, 2020, 2020],
                "Month": [1, 1, 1],
                "Day": [1, 1, 1],
                "Hour": [0, 2, 3],
                "Minute": [0, 0, 0],
                "ta": [10.0, 17.1, 18.0],
            }
        )

        out = wmo_time_consistency(df, "ta", units="C")

        self.assertEqual(len(out), 2)
        self.assertEqual(out["Hour"].tolist(), [0, 2])

    def test_wmo_time_consistency_ignores_changes_beyond_twelve_hours(self):
        df = pd.DataFrame(
            {
                "Year": [2020, 2020],
                "Month": [1, 1],
                "Day": [1, 1],
                "Hour": [0, 13],
                "Minute": [0, 0],
                "td": [5.0, 30.0],
            }
        )

        out = wmo_time_consistency(df, "td", units="C")

        self.assertTrue(out.empty)


if __name__ == "__main__":
    unittest.main()
