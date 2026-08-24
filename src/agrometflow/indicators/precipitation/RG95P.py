import numpy as np
import pandas as pd
import xarray as xr

from ..base import BaseIndicator

class R95p(BaseIndicator):
    """
    Precipitation on days > 95th percentile.

    R95p = sum(PR on days where PR > 95th percentile)

    Compatible with:
        - xarray.Dataset / xarray.DataArray
        - pandas.DataFrame
    """


    def __init__(
        self,
        percentile=95,
        variable="PR"
    ):
        super().__init__(
            name="R95p",
            description=(
                f"Total precipitation on days exceeding "
                f"the {percentile}th percentile"
            ),
            unit="mm",
            variable=variable
        )

        self.percentile = percentile
        self._config["percentile"] = percentile

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

        # Calculate percentile on valid values
        threshold = da.quantile(
            self.percentile / 100,
            dim="time",
            skipna=True
        )

        # Extreme days: PR > percentile and non-NaN
        extreme_days = (da > threshold) & da.notnull()

        # Total precipitation on extreme days
        result = da.where(extreme_days).sum(
            dim="time",
            skipna=True
        )

        # Return NaN if there are no extreme days
        count_extreme = extreme_days.sum(dim="time")

        result = xr.where(
            count_extreme > 0,
            result,
            np.nan
        )

        result.name = self.name

        result.attrs.update({
            "long_name": self.description,
            "units": self.unit,
            "percentile": self.percentile,
            "method": "R95p"
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
            threshold = df[self.variable].quantile(
                self.percentile / 100
            )

            extreme = df[
                df[self.variable] > threshold
            ]

            value = (
                extreme[self.variable].sum()
                if not extreme.empty
                else np.nan
            )

            return pd.DataFrame({
                self.name: [value]
            })

        # Calculate R95p per point
        def calculate(group):
            threshold = group[self.variable].quantile(
                self.percentile / 100
            )

            extreme = group[
                group[self.variable] > threshold
            ]

            return (
                extreme[self.variable].sum()
                if not extreme.empty
                else np.nan
            )

        result = (
            df.groupby(group_cols, observed=True)
            .apply(calculate)
            .reset_index(name=self.name)
        )

        return result