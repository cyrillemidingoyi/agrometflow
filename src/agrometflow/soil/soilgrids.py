from .base import SoilSource

class SoilGridsFetcher(SoilSource):
    def download(self, **kwargs):
        # Implémentation spécifique à SoilGrids (API ou FTP)
        pass

    def extract(self, variables, depth=None, resolution=None):
        # Traitement de GeoTIFF ou NetCDF
        pass
