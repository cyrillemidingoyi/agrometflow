"""
Agroclimatic indicator:
Cumulative precipitation.
"""


import pandas as pd
import xarray as xr

from ..base import BaseIndicator



class CumulativeRainfall(BaseIndicator):
    """
    Compute cumulative precipitation.

    Input variable:
        PR

    Compatible with:
        - xarray.Dataset
        - xarray.DataArray
        - pandas.DataFrame


    Parameters
    ----------
    period : str
        Aggregation period:
            - monthly
            - annual

    variable : str
        Standardized precipitation variable.
        Default: precip
    """


    def __init__(
        self,
        period="monthly",
        variable="precip"
    ):

        super().__init__(
            name="PR_CUM",
            description=(
                f"Cumulative precipitation ({period})"
            ),
            unit="mm",
            variable=variable
        )

        self.period = period



    def compute(
        self,
        data
    ):
        print(f"all params cumulative {data}")

        if isinstance(
            data,
            (xr.Dataset, xr.DataArray)
        ):

            return self.compute_xarray(
                data
            )

        elif isinstance(
            data,
            pd.DataFrame
        ):

            return self.compute_dataframe(
                data
            )

        else:

            raise TypeError(
                "Input must be xarray Dataset/DataArray or pandas DataFrame"
            )



    def compute_xarray(
        self,
        data
    ):

        self.validate_variable(data)


        da = (
            data[self.variable]
            if isinstance(data, xr.Dataset)
            else data
        )


        if self.period == "monthly":

            result = (
                da
                .resample(time="1MS")
                .sum(skipna=True)
            )


        elif self.period == "annual":

            result = (
                da
                .resample(time="1YS")
                .sum(skipna=True)
            )


        else:

            raise ValueError(
                "period must be monthly or annual"
            )


        result.name = self.name

        result.attrs.update(
            {
                "long_name":
                    self.description,

                "units":
                    self.unit,

                "variable":
                    self.name
            }
        )

        return result




    def compute_dataframe(
        self,
        data
    ):

        df = data.copy()


        self.validate_variable(df)


        df["time"] = pd.to_datetime(
            df["time"]
        )


        if self.period == "monthly":

            df["period"] = (
                df["time"]
                .dt
                .to_period("M")
                .astype(str)
            )


        elif self.period == "annual":

            df["period"] = (
                df["time"]
                .dt.year
            )


        else:

            raise ValueError(
                "period must be monthly or annual"
            )



        group_cols = ["period"]


        if "lat" in df.columns:
            group_cols.append("lat")

        if "lon" in df.columns:
            group_cols.append("lon")



        result = (
            df
            .groupby(group_cols)[self.variable]
            .sum(min_count=1)
            .reset_index()
        )



        result = result.rename(
            columns={
                self.variable:
                    self.name
            }
        )


        result["unit"] = self.unit


        return result