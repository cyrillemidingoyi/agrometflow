"""
Classe de base pour les indicateurs agroclimatiques.

Compatible :
    - xarray.Dataset (NetCDF spatial)
    - xarray.DataArray
    - pandas.DataFrame (points)
"""


from abc import ABC, abstractmethod

import pandas as pd
import xarray as xr



class BaseIndicator(ABC):

    """
    Classe mère de tous les indicateurs agroclimatiques.
    """


    def __init__(
        self,
        name,
        description,
        unit,
        variable="PR"
    ):

        self.name = name
        self.description = description
        self.unit = unit
        self.variable = variable

        self._config = {}



    def compute(
        self,
        data,
        **kwargs
    ):
        """
        Point d'entrée unique.

        Oriente automatiquement vers :
            - xarray
            - pandas
        """


        if isinstance(
            data,
            (xr.Dataset, xr.DataArray)
        ):

            return self.compute_xarray(
                data,
                **kwargs
            )


        elif isinstance(
            data,
            pd.DataFrame
        ):

            return self.compute_dataframe(
                data,
                **kwargs
            )


        else:

            raise TypeError(
                f"Type non supporté : {type(data)}"
            )



    @abstractmethod
    def compute_xarray(
        self,
        data,
        **kwargs
    ):
        """
        Calcul sur données NetCDF/xarray.
        """
        pass



    @abstractmethod
    def compute_dataframe(
        self,
        data,
        **kwargs
    ):
        """
        Calcul sur données CSV/pandas.
        """
        pass



    def validate_variable(
        self,
        data
    ):
        """
        Vérifie la présence de la variable climatique.
        """


        if isinstance(
            data,
            xr.Dataset
        ):

            if self.variable not in data:

                raise ValueError(
                    f"La variable {self.variable} "
                    "est absente du Dataset"
                )


        elif isinstance(
            data,
            pd.DataFrame
        ):

            if self.variable not in data.columns:

                raise ValueError(
                    f"La colonne {self.variable} "
                    "est absente du DataFrame"
                )



    def get_config(self):

        return {

            "name": self.name,

            "description":
                self.description,

            "unit":
                self.unit,

            "variable":
                self.variable,

            "config":
                self._config
        }