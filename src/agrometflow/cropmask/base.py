from abc import ABC, abstractmethod

class MaskSource(ABC):
    """
    Abstract base class for all soil data sources.

    Each subclass should implement the logic to:
    - Download soil data for a given region
    - Extract or process variables of interest
    """

    @abstractmethod
    def download(self, bbox, output_dir):
        """
        Download soil data for the specified bounding box.

        Parameters
        ----------
        bbox : tuple
            Bounding box as (min_lon, min_lat, max_lon, max_lat)
        output_dir : str or Path
            Directory where the downloaded files should be saved
        """
        pass

    @abstractmethod
    def extract(self, variables, resolution=None):
        """
        Extract and optionally resample soil variables.

        Parameters
        ----------
        variables : list of str
            List of variable names to extract (e.g., 'sand', 'clay', 'organic_carbon')
        resolution : float or tuple, optional
            Desired output spatial resolution in degrees
        """
        pass
