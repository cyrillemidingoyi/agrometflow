"""
Classe de base pour les indicateurs agroclimatiques
"""

from abc import ABC, abstractmethod
import pandas as pd
import xarray as xr


class BaseIndicator(ABC):
    """
    Classe de base pour tous les indicateurs agroclimatiques.
    """
    
    def __init__(self, name, description, unit, variable="PR"):
        self.name = name
        self.description = description
        self.unit = unit
        self.variable = variable
        self._config = {}
    
    @abstractmethod
    def compute(self, data, **kwargs):
        """
        Calcule l'indicateur.
        
        Parameters
        ----------
        data : pandas.DataFrame
            Données avec colonnes : time, lat, lon, PR (ou autre)
        **kwargs : paramètres supplémentaires
        
        Returns
        -------
        pandas.DataFrame
            Données avec l'indicateur calculé
        """
        pass
    
    def get_config(self):
        """Retourne la configuration de l'indicateur."""
        return {
            "name": self.name,
            "description": self.description,
            "unit": self.unit,
            "variable": self.variable,
            "config": self._config
        }
    
    def _ensure_dataframe(self, data):
        """Convertit en DataFrame si nécessaire."""
        if isinstance(data, (xr.DataArray, xr.Dataset)):
            return data.to_dataframe().reset_index()
        return data