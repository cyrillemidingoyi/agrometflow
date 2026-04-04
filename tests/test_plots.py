import unittest
from unittest.mock import patch

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

from agrometflow.dataquality.plots import plot_daily


class TestPlotDailyMissingMarkers(unittest.TestCase):
    def tearDown(self):
        plt.close("all")

    def test_missing_markers_use_axis_relative_offset_for_small_precipitation(self):
        data = pd.DataFrame(
            {
                "date": pd.to_datetime(["2020-01-01", "2020-01-03", "2020-01-04"]),
                "value": [0.0, 0.2, 0.4],
            }
        )

        with patch("matplotlib.pyplot.close", lambda *args, **kwargs: None):
            plot_daily(
                data=data,
                date_col="date",
                value_col="value",
                var_name="rr",
                units="mm",
                show_missing=True,
                show=False,
            )

        fig = plt.gcf()
        ax = fig.axes[0]
        missing_offsets = ax.collections[0].get_offsets()
        missing_y = float(missing_offsets[0][1])

        self.assertGreater(missing_y, -0.2)
        self.assertLess(missing_y, 0.0)


if __name__ == "__main__":
    unittest.main()
