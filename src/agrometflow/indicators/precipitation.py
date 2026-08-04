"""
Indicateurs agroclimatiques de précipitation
"""

from .base import BaseIndicator
import pandas as pd


class CumulativeRainfall(BaseIndicator):
    """Cumul de précipitations sur une période."""
    
    def __init__(self, period="monthly"):
        super().__init__(
            name="cumulative_rainfall",
            description=f"Cumul de précipitations ({period})",
            unit="mm"
        )
        self.period = period
        self._config = {"period": period}
    
    def compute(self, data, **kwargs):
        df = self._ensure_dataframe(data)
        df['time'] = pd.to_datetime(df['time'])
        
        if self.period == "monthly":
            df['period'] = df['time'].dt.to_period('M')
            result = df.groupby(['period', 'lat', 'lon'])[self.variable].sum().reset_index()
            result['period'] = result['period'].astype(str)
            result = result.rename(columns={self.variable: self.name})
            return result
        elif self.period == "yearly":
            df['period'] = df['time'].dt.year
            result = df.groupby(['period', 'lat', 'lon'])[self.variable].sum().reset_index()
            result = result.rename(columns={self.variable: self.name})
            return result
        else:
            raise ValueError(
                f"Période invalide : '{self.period}'. "
                "Les valeurs autorisées sont 'monthly' ou 'yearly'."
            )


class NumberOfRainyDays(BaseIndicator):
    """
    Nombre de jours pluvieux.

    Définition climatologique :
        PR >= threshold

    Hypothèses d'entrée :
    - données journalières
    - précipitations en mm/jour
    - coordonnées standardisées (lat, lon)
    - temps au format datetime
    """

    def __init__(self, threshold=1.0, period="monthly"):

        super().__init__(
            name="rainy_days",
            description=(
                f"Nombre de jours avec pluie >= {threshold} mm ({period})"
            ),
            unit="days"
        )

        self.threshold = threshold
        self.period = period

        self._config = {
            "threshold": threshold,
            "period": period
        }


    def compute(self, data, **kwargs):

        df = self._ensure_dataframe(data).copy()


        # Jour pluvieux :
        # - valeur non manquante
        # - pluie >= seuil
        df["rainy"] = (
            df[self.variable].notna()
            &
            (df[self.variable] >= self.threshold)
        )


        # Définition de la période d'agrégation
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
                .dt
                .year
            )


        else:
            raise ValueError(
                "period doit être 'monthly' ou 'annual'"
            )


        result = (
            df
            .groupby(
                [
                    "period",
                    "lat",
                    "lon"
                ]
            )["rainy"]
            .sum()
            .reset_index()
        )


        result = result.rename(
            columns={
                "rainy": self.name
            }
        )


        return result

class ConsecutiveDryDays(BaseIndicator):
    """
    Nombre maximal de jours secs consécutifs (CDD).

    Définition climatologique :
        PR < threshold

    Hypothèses d'entrée :
    - données journalières
    - précipitations en mm/jour
    - coordonnées standardisées (lat, lon)
    - temps au format datetime
    """

    def __init__(self, threshold=1.0, period="annual"):

        super().__init__(
            name="consecutive_dry_days",
            description=(
                f"Nombre maximal de jours secs consécutifs (PR < {threshold} mm)"
            ),
            unit="days"
        )

        self.threshold = threshold
        self.period = period

        self._config = {
            "threshold": threshold,
            "period": period
        }


    def compute(self, data, **kwargs):

        df = self._ensure_dataframe(data).copy()


        # Tri temporel indispensable
        df = df.sort_values(
            [
                "lat",
                "lon",
                "time"
            ]
        )


        # Jour sec :
        # - valeur non manquante
        # - pluie < seuil
        df["dry"] = (
            df[self.variable].notna()
            &
            (df[self.variable] < self.threshold)
        ).astype(int)


        def max_consecutive(series):

            max_count = 0
            current = 0


            for value in series:

                if value == 1:

                    current += 1

                    max_count = max(
                        max_count,
                        current
                    )

                else:

                    current = 0


            return max_count


        # Définition de la période
        if self.period == "annual":

            df["period"] = (
                df["time"]
                .dt
                .year
            )


        elif self.period == "monthly":

            df["period"] = (
                df["time"]
                .dt
                .to_period("M")
                .astype(str)
            )


        else:
            raise ValueError(
                "period doit être 'monthly' ou 'annual'"
            )


        result = (
            df
            .groupby(
                [
                    "period",
                    "lat",
                    "lon"
                ]
            )["dry"]
            .apply(max_consecutive)
            .reset_index()
        )


        result = result.rename(
            columns={
                "dry": self.name
            }
        )


        return result

