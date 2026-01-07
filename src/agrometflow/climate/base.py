from abc import ABC, abstractmethod

class ClimateSource(ABC):
    """
    Abstract base class for all climate data sources.
    Each subclass must implement a download() and extract() method.
    """

    @abstractmethod
    def download(self, **kwargs):
        """
        Télécharge les données climatiques à partir d'une configuration.
        Doit lever une erreur si des arguments obligatoires sont manquants.
        """
        pass

    @abstractmethod
    def extract(self, variables=None, start_date=None, end_date=None, as_long=False, **kwargs):
        """
        Extract and return a filtered version of the dataset.

        Parameters
        ----------
        variables : list of str, optional
            List of variables to retain. If None, return all.
        start_date : str, optional
            Filter start date
        end_date : str, optional
            Filter end date
        as_long : bool, optional
            If True, return data in long format
        **kwargs :
            Optional filter or formatting options
        """
        pass
