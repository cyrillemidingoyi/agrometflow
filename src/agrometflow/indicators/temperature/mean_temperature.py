"""
Indicateurs de température de base.
"""

import pandas as pd
import xarray as xr

from ..base import BaseIndicator


class MeanTemperature(BaseIndicator):
    """
    Température moyenne agrégée sur une période donnée.

    Compatible avec :
        - xarray.Dataset / xarray.DataArray
        - pandas.DataFrame

    Paramètres
    ----------
    period : str, défaut="monthly"
        Période d'agrégation. Valeurs prises en charge :
        "daily", "monthly" et "annual".
    variable : str, défaut="T2M"
        Variable de température.
    """

    # Liste des périodes prises en charge
    SUPPORTED_PERIODS = {"daily", "monthly", "annual"}

    def __init__(
        self,
        period="monthly",
        variable="T2M"
    ):
        # Validation immédiate de la période
        if period not in self.SUPPORTED_PERIODS:
            raise ValueError(
                f"Période non prise en charge : {period}. "
                f"Périodes prises en charge : "
                f"{', '.join(sorted(self.SUPPORTED_PERIODS))}"
            )

        # Appel au constructeur parent
        super().__init__(
            name="temperature_moyenne",
            description=f"Température moyenne ({period})",
            unit="°C",
            variable=variable
        )

        self.period = period

        # Stockage de la configuration pour la traçabilité
        self._config["period"] = period

    # ==========================
    # XARRAY
    # ==========================

    def compute_xarray(self, data, **kwargs):
        self.validate_variable(data)

        # Extraction de la variable
        da = (
            data[self.variable]
            if isinstance(data, xr.Dataset)
            else data
        )

        # Correspondance entre les périodes et les fréquences xarray
        frequencies = {
            "daily": "1D",
            "monthly": "1MS",
            "annual": "1YS",
        }

        # Agrégation temporelle
        result = da.resample(
            time=frequencies[self.period]
        ).mean(skipna=True)

        # Nom et métadonnées du résultat
        result.name = self.name

        result.attrs.update({
            "long_name": self.description,
            "units": self.unit,
            "period": self.period,
            "method": "Température moyenne"
        })

        return result

    # ==========================
    # DATAFRAME
    # ==========================

    def compute_dataframe(self, data, **kwargs):
        self.validate_variable(data)

        # Vérification de la présence de la colonne temporelle
        if "time" not in data.columns:
            raise ValueError(
                "Le DataFrame doit contenir une colonne 'time'."
            )

        df = data.copy()

        # Conversion explicite en datetime
        df["time"] = pd.to_datetime(df["time"])

        # Identification des dimensions spatiales
        group_cols = [
            col for col in ("lat", "lon")
            if col in df.columns
        ]

        # Création de la colonne représentant la période d'agrégation
        if self.period == "daily":
            df["_period"] = df["time"].dt.floor("D")

        elif self.period == "monthly":
            df["_period"] = df["time"].dt.to_period("M")

        elif self.period == "annual":
            df["_period"] = df["time"].dt.to_period("Y")

        # Calcul de la moyenne par point spatial et par période
        result = (
            df.groupby(
                group_cols + ["_period"],
                observed=True
            )[self.variable]
            .mean()
            .reset_index(name=self.name)
        )

        # Renommage de la colonne de période
        result = result.rename(
            columns={"_period": "time"}
        )

        return result