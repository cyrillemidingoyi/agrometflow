"""
Indicateurs d'intensité des précipitations.
"""

import numpy as np
import pandas as pd
import xarray as xr

from ..base import BaseIndicator


class SDII(BaseIndicator):
    """
    Simple Daily Intensity Index (SDII).

    Mean precipitation amount on wet days.

    SDII = sum(PR on wet days) / number of wet days

    A wet day is defined as PR >= threshold.

    Compatible with:
        - xarray.Dataset / xarray.DataArray
        - pandas.DataFrame
    """

    def __init__(
        self,
        threshold=1.0,
        variable="PR"
    ):
        super().__init__(
            name="SDII",
            description=(
                f"Mean precipitation intensity on wet days "
                f"(PR >= {threshold} mm)"
            ),
            unit="mm/day",
            variable=variable
        )

        self.threshold = threshold
        self._config["threshold"] = threshold

    # ==========================
    # XARRAY
    # ==========================

    def compute_xarray(self, data, **kwargs):
        self.validate_variable(data)

        da = (
            data[self.variable]
            if isinstance(data, xr.Dataset)
            else data
        )

        # Valid precipitation values
        valid = da.notnull()

        # Wet days: PR >= threshold and non-NaN
        wet_days = (da >= self.threshold) & valid

        # Precipitation on wet days
        wet_precip = da.where(wet_days)

        # Total precipitation on wet days
        total_wet_precip = wet_precip.sum(
            dim="time",
            skipna=True
        )

        # Number of wet days
        number_wet_days = wet_days.sum(
            dim="time"
        )

        # Avoid division by zero
        result = xr.where(
            number_wet_days > 0,
            total_wet_precip / number_wet_days,
            np.nan
        )

        result.name = self.name

        result.attrs.update({
            "long_name": self.description,
            "units": self.unit,
            "threshold": self.threshold,
            "method": "Simple Daily Intensity Index"
        })

        return result

    # ==========================
    # DATAFRAME
    # ==========================

    def compute_dataframe(self, data, **kwargs):
        self.validate_variable(data)

        df = data.copy()

        # Identify spatial dimensions
        group_cols = [
            col for col in ("lat", "lon")
            if col in df.columns
        ]

        # If no spatial dimensions, compute globally
        if not group_cols:
            values = df[self.variable].dropna()
            wet_days = values[values >= self.threshold]

            value = (
                wet_days.mean()
                if not wet_days.empty
                else np.nan
            )

            return pd.DataFrame({
                self.name: [value]
            })

        # Calculate SDII per point
        def calculate(group):
            values = group[self.variable].dropna()
            wet_days = values[values >= self.threshold]

            if wet_days.empty:
                return np.nan

            return wet_days.mean()

        result = (
            df.groupby(group_cols, observed=True)
            .apply(calculate)
            .reset_index(name=self.name)
        )

        return result