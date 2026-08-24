"""
Indicateurs de température agricoles.
"""

import pandas as pd
import xarray as xr

from ..base import BaseIndicator


class GrowingDegreeDays(BaseIndicator):
    """
    Degrés-jours de croissance (GDD).

    Les degrés-jours de croissance représentent l'accumulation de
    température au-dessus d'une température de base sur une période donnée.

    GDD journalier = max(0, T_moyenne - T_base)

    Si une température plafond est définie :

    GDD journalier = min(
        max(0, T_moyenne - T_base),
        T_plafond - T_base
    )

    Compatible avec :
        - xarray.Dataset / xarray.DataArray
        - pandas.DataFrame

    Paramètres
    ----------
    t_base : float, défaut=10.0
        Température de base en dessous de laquelle le développement de la
        plante est considéré comme nul.
        Exemple : 10 °C pour le maïs, 0 °C pour certaines cultures de blé.

    t_upper : float, optionnel
        Température plafond utilisée pour limiter la contribution
        quotidienne au cumul des degrés-jours.
        Exemple : 30 °C pour le maïs.

    variable : str, défaut="T2M"
        Variable représentant la température moyenne journalière.
    """

    def __init__(
        self,
        t_base=10.0,
        t_upper=None,
        variable="T2M"
    ):
        # Validation des paramètres
        if t_upper is not None and t_upper <= t_base:
            raise ValueError(
                f"t_upper ({t_upper}) doit être supérieur à "
                f"t_base ({t_base})."
            )

        # Construction dynamique de la description
        description = (
            f"Degrés-jours de croissance (T_base={t_base} °C)"
        )

        if t_upper is not None:
            description += f", T_plafond={t_upper} °C"

        # Appel au constructeur parent
        super().__init__(
            name="degres_jours_croissance",
            description=description,
            unit="°C·jour",
            variable=variable
        )

        # Stockage des paramètres
        self.t_base = t_base
        self.t_upper = t_upper

        # Stockage de la configuration pour la traçabilité
        self._config["t_base"] = t_base
        self._config["t_upper"] = t_upper

    # ==========================
    # XARRAY
    # ==========================

    def compute_xarray(self, data, **kwargs):
        self.validate_variable(data)

        # Extraction de la variable de température
        da = (
            data[self.variable]
            if isinstance(data, xr.Dataset)
            else data
        )

        # Calcul des degrés-jours quotidiens
        #
        # GDD = max(0, T - T_base)
        gdd_daily = (da - self.t_base).clip(min=0)

        # Application de la température plafond si elle est définie
        #
        # GDD = min(GDD, T_plafond - T_base)
        if self.t_upper is not None:
            gdd_daily = gdd_daily.clip(
                max=self.t_upper - self.t_base
            )

        # Accumulation des degrés-jours sur la période temporelle
        result = gdd_daily.sum(
            dim="time",
            skipna=True
        )

        # Nom et métadonnées du résultat
        result.name = self.name

        result.attrs.update({
            "long_name": self.description,
            "units": self.unit,
            "t_base": self.t_base,
            "t_upper": self.t_upper,
            "method": "Degrés-jours de croissance"
        })

        return result

    # ==========================
    # DATAFRAME
    # ==========================

    def compute_dataframe(self, data, **kwargs):
        self.validate_variable(data)

        df = data.copy()

        # Calcul des degrés-jours quotidiens
        gdd_daily = (
            df[self.variable] - self.t_base
        ).clip(lower=0)

        # Application de la température plafond si elle est définie
        if self.t_upper is not None:
            gdd_daily = gdd_daily.clip(
                upper=self.t_upper - self.t_base
            )

        # Identification des dimensions spatiales
        group_cols = [
            col for col in ("lat", "lon")
            if col in df.columns
        ]

        # Calcul global en l'absence de coordonnées spatiales
        if not group_cols:
            value = gdd_daily.sum(skipna=True)

            return pd.DataFrame({
                self.name: [value]
            })

        # Accumulation des degrés-jours pour chaque point spatial
        result = (
            df.assign(_gdd=gdd_daily)
            .groupby(group_cols, observed=True)["_gdd"]
            .sum()
            .reset_index(name=self.name)
        )

        return result