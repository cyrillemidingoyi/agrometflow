"""
Indicateur agroclimatique :
Longueur maximale d'une séquence sèche.

Un jour est considéré sec lorsque :
PR < threshold

Compatible :
    - xarray.Dataset / DataArray (NetCDF spatial)
    - pandas.DataFrame (points CSV)

Sortie :
    - xarray.DataArray : longueur maximale de sécheresse par pixel
    - pandas.DataFrame : longueur maximale par point
"""

import pandas as pd
import xarray as xr
import numpy as np

from ..base import BaseIndicator


class DrySpell(BaseIndicator):
    """
    Calcul de la plus longue période sèche consécutive.

    Parameters
    ----------
    threshold : float
        Seuil de pluie journalier (mm/jour).

    variable : str
        Variable standardisée de précipitation.
        Par défaut : PR

    min_periods : int
        Nombre minimal de jours nécessaires pour calculer l'indicateur.
    """

    def __init__(
        self,
        threshold=1.0,
        variable="PR",
        min_periods=30
    ):
        super().__init__(
            name="dry_spell",
            description=(
                f"Durée maximale période sèche "
                f"(PR < {threshold} mm/jour)"
            ),
            unit="days",
            variable=variable
        )

        self.threshold = threshold
        self.min_periods = min_periods

    # ==================================================
    # POINT D'ENTREE
    # ==================================================

    def compute(self, data, **kwargs):
        if isinstance(data, (xr.Dataset, xr.DataArray)):
            return self.compute_xarray(data, **kwargs)
        elif isinstance(data, pd.DataFrame):
            return self.compute_dataframe(data, **kwargs)
        else:
            raise TypeError("Input must be xarray or pandas DataFrame")

    # ==================================================
    # XARRAY / NETCDF
    # ==================================================

    def compute_xarray(self, data, **kwargs):
        self.validate_variable(data)

        if isinstance(data, xr.Dataset):
            da = data[self.variable]
        else:
            da = data

        if "time" not in da.dims:
            raise ValueError("Dimension 'time' obligatoire")

        # ============================================================
        # CORRECTION : Gestion des chunks Dask
        # ============================================================
        
        # Vérifier si les données sont chunkées
        if hasattr(da.data, 'chunks'):
            # Rechunker time en un seul bloc pour l'application de la fonction
            da = da.chunk(dict(time=-1))
        
        # Sécurité temporelle
        da = da.sortby("time")

        def longest_dry_period(values):
            values = np.asarray(values)

            # Suppression valeurs manquantes
            values = values[~np.isnan(values)]

            if len(values) < self.min_periods:
                return np.nan

            dry = values < self.threshold

            max_run = 0
            current = 0

            for day in dry:
                if day:
                    current += 1
                    max_run = max(max_run, current)
                else:
                    current = 0

            return max_run

        # ============================================================
        # CORRECTION : allow_rechunk=True
        # ============================================================
        
        result = xr.apply_ufunc(
            longest_dry_period,
            da,
            input_core_dims=[["time"]],
            vectorize=True,
            dask="parallelized",
            output_dtypes=[float],
            dask_gufunc_kwargs={
                "allow_rechunk": True  # ← Permet le rechunking automatique
            }
        )

        result.name = self.name

        result.attrs.update(
            {
                "long_name": self.description,
                "units": self.unit,
                "threshold_mm_day": self.threshold,
                "method": "maximum consecutive dry days",
                "indicator_category": "agroclimatic drought"
            }
        )

        return result

    # ==================================================
    # DATAFRAME / POINTS
    # ==================================================

    def compute_dataframe(self, data, **kwargs):
        df = data.copy()

        self.validate_variable(df)

        df["time"] = pd.to_datetime(df["time"])
        df = df.sort_values("time")
        df["dry"] = df[self.variable] < self.threshold

        # Identification des points
        group_cols = [c for c in ["lat", "lon"] if c in df.columns]

        if not group_cols:
            df["_point"] = 0
            group_cols = ["_point"]

        results = []

        for keys, group in df.groupby(group_cols):
            if len(group) < self.min_periods:
                value = np.nan
            else:
                value = self._max_run(group["dry"])

            record = {}

            if isinstance(keys, tuple):
                for col, val in zip(group_cols, keys):
                    if col != "_point":
                        record[col] = val
            else:
                if "_point" not in group_cols:
                    record[group_cols[0]] = keys

            record[self.name] = value
            results.append(record)

        return pd.DataFrame(results)

    # ==================================================
    # UTILITAIRE
    # ==================================================

    @staticmethod
    def _max_run(values):
        max_run = 0
        current = 0

        for value in values:
            if value:
                current += 1
                max_run = max(max_run, current)
            else:
                current = 0

        return max_run