"""
Indicateur agroclimatique :
Nombre de jours de pluie.

Un jour est considéré comme pluvieux lorsque :
PR >= threshold
"""


import pandas as pd
import xarray as xr

from ..base import BaseIndicator



class RainyDays(BaseIndicator):

    """
    Nombre de jours pluvieux.

    Compatible :
        - xarray.Dataset
        - pandas.DataFrame

    Parameters
    ----------
    threshold : float
        Seuil de pluie en mm/jour.

    variable : str
        Variable pluie.
    """


    def __init__(
        self,
        threshold=1.0,
        variable="PR"
    ):

        super().__init__(
            name="rainy_days",
            description=(
                f"Nombre de jours avec PR >= {threshold} mm"
            ),
            unit="days",
            variable=variable
        )

        self.threshold = threshold


        self._config = {
            "threshold": threshold
        }


    # ==========================
    # XARRAY
    # ==========================

    def compute_xarray(self, data, **kwargs):
        self.validate_variable(data)
        
        # ✅ Gérer le cas où data est une DataArray
        if isinstance(data, xr.DataArray):
            da = data
        else:
            da = data[self.variable]
        
        result = (da >= self.threshold).sum(dim="time", skipna=True)
        
        result.name = self.name
        result.attrs = {
            "long_name": self.description,
            "units": self.unit
        }
        
        return result


    # ==========================
    # DATAFRAME
    # ==========================

    def compute_dataframe(
        self,
        data,
        **kwargs
    ):

        df = data.copy()


        self.validate_variable(df)


        df["time"] = pd.to_datetime(
            df["time"]
        )


        df["is_rainy"] = (
            df[self.variable]
            >= self.threshold
        )


        group_cols = []


        if "lat" in df.columns:
            group_cols.append("lat")

        if "lon" in df.columns:
            group_cols.append("lon")


        result = (
            df
            .groupby(group_cols)
            ["is_rainy"]
            .sum()
            .reset_index()
        )


        result = result.rename(
            columns={
                "is_rainy":
                    self.name
            }
        )


        return result