"""Téléchargeur pour les produits de la famille PERSIANN
    Produits supportés :
    - persiann : PERSIANN standard (0.25°, 480x1440)
    - persiann_ccs: PERSIANN-CCS (0.04°, 3000x9000)
    - persiann_cdr : PERSIANN-CDR (0.25°, 480x1440)
    - persiann_ccs_cdr_v2 : PERSIANN-CCS-CDR V2 (0.04°, 3000x9000)
    - pdirnow : PDIR-Now (0.04°, 3000x9000)
    - persiann_v3 : PERSIANN V3 / PUnet (0.04°, 3000x9000)
    - persiann_cdr_v3 : PERSIANN-CDR V3 / PUnetCDR (0.04°, 3000x9000)
    """
import numpy as np
import xarray as xr
import requests
import gzip
import shutil
import urllib.request
from pathlib import Path
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from agrometflow.utils import get_logger


class PersiannDownloader:
   # Dictionnaire des produits disponibles
    PRODUCTS = {
        "persiann": {
            "base_url": "http://persiann.eng.uci.edu/CHRSdata/PERSIANN/daily",
            "prefix": "ms6s4",
            "rows": 480,
            "cols": 1440,
            "lat_start": 59.875,
            "lat_end": -59.875,
            "lon_start": 0.125,
            "lon_end": 359.875,
            "resolution": "0.25°",
            "description": "PERSIANN standard (daily precipitation)",
            "filename_format": "prefix_dYYDDD",
            "endian":">f4",  # big-endian
        },
        "persiann_ccs": {
            "base_url": "http://persiann.eng.uci.edu/CHRSdata/PERSIANN-CCS/daily",
            "prefix": "rgccs1d",
            "rows": 3000,
            "cols": 9000,
            "lat_start": 59.98,
            "lat_end": -59.98,
            "lon_start": 0.02,
            "lon_end": 359.98,
            "resolution": "0.04° (4km)",
            "description": "PERSIANN-CCS (high resolution, daily precipitation)",
            "filename_format": "prefixYYDDD",
            "endian": ">f4",  # big-endian
        },
        "persiann_cdr": {
            "base_url": "http://persiann.eng.uci.edu/CHRSdata/PERSIANN-CDR/daily",
            "prefix": "aB1",
            "rows": 480,
            "cols": 1440,
            "lat_start": 59.875,
            "lat_end": -59.875,
            "lon_start": 0.125,
            "lon_end": 359.875,
            "resolution": "0.25°",
            "description": "PERSIANN-CDR (climate data record)",
            "filename_format": "prefix_dYYDDD",
            "endian": ">f4",  # big-endian
        },
         "persiann_ccs_cdr_v2_b1": {
            "base_url": "https://persiann.eng.uci.edu/CHRSdata/PCCSCDR_B1/daily",
            "year_in_url": True,
            "prefix": "PCCSCDR",
            "rows": 3000,
            "cols": 9000,
            "lat_start": 59.98,
            "lat_end": -59.98,
            "lon_start": 0.02,
            "lon_end": 359.98,
            "resolution": "0.04° (4km)",
            "description": "PERSIANN-CCS-CDR V2.0 B1 (long record, 1983-2000, lower performance)",
            "filename_format": "prefix_XXYYMMDDhh",
            "endian": "<f4",  # little-endian (contrairement aux autres produits)
            "accumulation": "1d",  # journalier par défaut
        },
        "persiann_ccs_cdr_v2_cpc": {
            "base_url": "https://persiann.eng.uci.edu/CHRSdata/PCCSCDR_CPC/daily",
            "prefix": "PCCSCDR",
            "year_in_url": True,
            "rows": 3000,
            "cols": 9000,
            "lat_start": 59.98,
            "lat_end": -59.98,
            "lon_start": 0.02,
            "lon_end": 359.98,
            "resolution": "0.04° (4km)",
            "description": "PERSIANN-CCS-CDR V2.0 CPC (better performance, 2000-present)",
            "filename_format": "prefix_XXYYMMDDhh",
            "endian": "<f4",
            "accumulation": "1d",
        },
        "pdirnow": {
            "base_url": "https://persiann.eng.uci.edu/CHRSdata/PDIRNow/PDIRNowdaily",
            "prefix": "pdirnow",
            "rows": 3000,
            "cols": 9000,
            "lat_start": 59.98,
            "lat_end": -59.98,
            "lon_start": 0.02,
            "lon_end": 359.98,
            "resolution": "0.04° (4km)",
            "description": "PDIR-Now (high resolution, near real-time, 2000-present)",
            "filename_format": "prefixXXYYMMDDhh",
            "endian": "<f4",  # little-endian (V11 et après)
            "accumulation": "1d",  # journalier par défaut
            "special_cases": {
                "1h": {
                    "dtype": "int16",
                    "scale": 100  # diviser par 100 pour obtenir mm/hr
                    }
                }
        },
        "persiann_v3": {
            "base_url": "http://persiann.eng.uci.edu/CHRSdata/PUnet/PUnetdaily",
            "prefix": "PUnet",
            "rows": 3000,
            "cols": 9000,
            "lat_start": 59.98,
            "lat_end": -59.98,
            "lon_start": 0.02,
            "lon_end": 359.98,
            "resolution": "0.04° (4km)",
            "description": "PERSIANN V3 / PUnet (deep learning, 2000-present). Note: 1-hour data are int16 scaled by 100.",
            "filename_format": "prefixXXYYMMDDhh",
            "endian": "<f4",
            "accumulation": "1d",
            "special_cases": {
                "1h": {
                    "dtype": "int16",
                    "scale": 100
                    }
                }
        },
        "persiann_cdr_v3": {
            "base_url": "http://persiann.eng.uci.edu/CHRSdata/PUnetCDR/PUnetCDR1d",
            "prefix": "punetcdr",
            "rows": 3000,
            "cols": 9000,
            "lat_start": 59.98,
            "lat_end": -59.98,
            "lon_start": 0.02,
            "lon_end": 359.98,
            "resolution": "0.04° (4km)",
            "description": "PERSIANN-CDR V3 / PUnetCDR (climate data record, 1980-present)",
            "filename_format": "prefixXXYYMMDDhh",
            "endian": "<f4",
            "accumulation": "1d",  # journalier par défaut
        }
    }
      
    def __init__(self, product="persiann", output_dir="data/persiann", log_file=None, verbose=False, max_workers=6):
        """
        Initialise le téléchargeur PERSIANN pour un produit donné.

        Parameters
        ----------
        product : str
            Nom du produit ('persiann', 'persiann_ccs', 'persiann_cdr', ...)
        output_dir : str
            Dossier de sortie
        log_file : str, optional
            Fichier de log
        verbose : bool, optional
            Mode verbeux
        max_workers : int, optional
            Nombre de téléchargements parallèles
        """
        # Vérifier que le produit existe
        if product not in self.PRODUCTS:
            raise ValueError(f"Produit '{product}' non supporté. Choisir parmi : {list(self.PRODUCTS.keys())}")

        # Charger la configuration du produit
        self.product = product
        self.config = self.PRODUCTS[product]
        
        # Extraire les paramètres de la configuration
        self.base_url = self.config["base_url"]
        self.prefix = self.config["prefix"]
        self.rows = self.config["rows"]
        self.cols = self.config["cols"]
        self.lat_start = self.config["lat_start"]
        self.lat_end = self.config["lat_end"]
        self.lon_start = self.config["lon_start"]
        self.lon_end = self.config["lon_end"]
        self.filename_format = self.config.get("filename_format", "prefix_dYYDDD")
        self.endian = self.config.get("endian", ">f4")
        self.accumulation = self.config.get("accumulation", "1d")
        self.special_cases = self.config.get("special_cases", {})
        
        # Initialiser les dossiers
        self.output_dir = Path(output_dir)
        self.raw_dir = self.output_dir / "bin"
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.max_workers = max_workers
        
        # Initialiser le logger
        self.logger = get_logger(f"agrometflow.persiann.{product}", log_file=log_file, verbose=verbose)
        
        # Loguer les informations de configuration
        self.logger.info(f"🔧 Initialisation du produit PERSIANN : {product}")
        self.logger.info(f"   Description : {self.config.get('description', 'Aucune description')}")
        self.logger.info(f"   Résolution : {self.config.get('resolution', 'Inconnue')}")
        self.logger.info(f"   Format de nommage : {self.filename_format}")
        self.logger.info(f"   Endian : {self.endian}")
        self.logger.info(f"   Accumulation par défaut : {self.accumulation}")

    def build_filename(self, date):
        """
        Construit le nom du fichier selon la convention du produit.
        
        Formats supportés :
        - prefix_dYYDDD  → ms6s4_d24001.bin.gz (PERSIANN standard)
        - prefix_YYDDD   → rgccs1d_24001.bin.gz (PERSIANN-CCS)
        - prefix_XXYYMMDDhh → PCCSCDR1d24010100.bin.gz (PERSIANN-CCS-CDR V2)
        - prefixXXYYMMDDhh  → pdirnow1d240101.bin.gz (PDIR-Now, PUnet, PUnetCDR)
        """
        # Extraire les composants de la date
        year = date.strftime("%y")      # "24" pour 2024
        doy = date.strftime("%j")       # "001" pour 1er janvier
        yyyy = date.strftime("%Y")      # "2024"
        mm = date.strftime("%m")        # "01" pour janvier
        dd = date.strftime("%d")        # "01" pour le 1er

        format_type = self.filename_format

        if format_type == "prefix_dYYDDD":
            # Ex: ms6s4_d24001.bin.gz
            return f"{self.prefix}_d{year}{doy}.bin.gz"

        elif format_type == "prefix_YYDDD":
            # Ex: rgccs1d_24001.bin.gz
            return f"{self.prefix}_{year}{doy}.bin.gz"

        elif format_type == "prefixYYDDD":
            # Ex: rgccs1d24001.bin.gz (PERSIANN-CCS)
            return f"{self.prefix}{year}{doy}.bin.gz"

        elif format_type == "prefix_XXYYMMDDhh":
            # Ex: PCCSCDR1d24010100.bin.gz
            acc = self.accumulation
            if acc == "1d":
                # Pour daily, pas de hh
                return f"{self.prefix}{acc}{yyyy[2:]}{mm}{dd}.bin.gz"
            else:
                # Pour 3h, 6h, etc., ajouter hh = 00
                return f"{self.prefix}{acc}{yyyy[2:]}{mm}{dd}00.bin.gz"

        elif format_type == "prefixXXYYMMDDhh":
            # Ex: pdirnow1d240101.bin.gz
            acc = self.accumulation
            if acc == "1d":
                # Pour daily, pas de hh
                return f"{self.prefix}{acc}{yyyy[2:]}{mm}{dd}.bin.gz"
            else:
                # Pour 3h, 6h, etc. : ajouter hh = 00
                return f"{self.prefix}{acc}{yyyy[2:]}{mm}{dd}00.bin.gz"

        else:
            # Fallback
            self.logger.warning(f"Format de nommage inconnu: {format_type}")
            return f"{self.prefix}_d{year}{doy}.bin.gz"
            """
            Construit le nom du fichier selon la convention du produit.
            
            Formats supportés :
            - prefix_dYYDDD  → ms6s4_d24001.bin.gz (PERSIANN standard)
            - prefix_YYDDD   → rgccs1d_24001.bin.gz (PERSIANN-CCS)
            - prefix_XXYYMMDDhh → PCCSCDR1d24010100.bin.gz (PERSIANN-CCS-CDR V2)
            - prefixXXYYMMDDhh  → pdirnow1d240101.bin.gz (PDIR-Now, PUnet, PUnetCDR)
            """
            # Extraire les composants de la date
            year = date.strftime("%y")      # "24" pour 2024
            doy = date.strftime("%j")       # "001" pour 1er janvier
            yyyy = date.strftime("%Y")      # "2024"
            mm = date.strftime("%m")        # "01" pour janvier
            dd = date.strftime("%d")        # "01" pour le 1er

            format_type = self.filename_format

            if format_type == "prefix_dYYDDD":
                # Ex: ms6s4_d24001.bin.gz
                return f"{self.prefix}_d{year}{doy}.bin.gz"

            elif format_type == "prefix_YYDDD":
                # Ex: rgccs1d_24001.bin.gz
                return f"{self.prefix}_{year}{doy}.bin.gz"

            elif format_type == "prefix_XXYYMMDDhh":
                acc = self.accumulation
                if acc == "1d":
                    # Pour daily, pas de hh
                    return f"{self.prefix}{acc}{yyyy[2:]}{mm}{dd}.bin.gz"
                else:
                    # Pour 3h, 6h, etc., ajouter hh = 00
                    return f"{self.prefix}{acc}{yyyy[2:]}{mm}{dd}00.bin.gz"

            elif format_type == "prefixXXYYMMDDhh":
                # Ex: pdirnow1d240101.bin.gz
                acc = self.accumulation
                if acc == "1d":
                    # Pour daily, pas de hh
                    return f"{self.prefix}{acc}{yyyy[2:]}{mm}{dd}.bin.gz"
                else:
                    # Pour 3h, 6h, etc. : ajouter hh = 00
                    return f"{self.prefix}{acc}{yyyy[2:]}{mm}{dd}00.bin.gz"

            else:
                # Fallback
                self.logger.warning(f"Format de nommage inconnu: {format_type}")
                return f"{self.prefix}_d{year}{doy}.bin.gz"
            
    def build_url(self, date):
        """
        Construit l'URL complète pour un fichier donné.
        Si le produit a `year_in_url=True`, l'année est insérée dans le chemin.
        """
        # Si le produit nécessite l'année dans l'URL
        if self.config.get("year_in_url", False):
            year = date.strftime("%Y")
            return f"{self.base_url}/{year}/{self.build_filename(date)}"
        else:
            return f"{self.base_url}/{self.build_filename(date)}"

    def _download_and_extract(self, date):
        url = self.build_url(date)
        gz_filename = self.build_filename(date)
        gz_path = self.raw_dir / gz_filename
        bin_path = self.raw_dir / gz_filename.replace(".gz", "")

        if bin_path.exists():
            return bin_path

        try:
            # Télécharger avec urllib (gère FTP et HTTP)
            urllib.request.urlretrieve(url, gz_path)
            
            # Décompresser
            with gzip.open(gz_path, "rb") as f_in, open(bin_path, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)

            self.logger.info(f"✅ Downloaded and extracted: {gz_filename}")
            return bin_path
        except Exception as e:
            self.logger.error(f"❌ Error downloading {gz_filename}: {e}")
            return None

    def _daterange(self, start_date, end_date):
        while start_date <= end_date:
            yield start_date
            start_date += timedelta(days=1)

    def convert_bin_to_xarray(self, bin_file, date):
        """
        Convertit un fichier binaire (.bin) en DataArray xarray.
        Utilise np.memmap pour lire les gros fichiers sans tout charger en mémoire.
        """
        import numpy as np
        
        rows = self.rows
        cols = self.cols
        nodata = -9999
        
        acc = self.accumulation
        special = self.special_cases.get(acc, {})
        
        if special:
            dtype = special.get("dtype", "int16")
            scale = special.get("scale", 1)
            # Utiliser memmap pour lire sans tout charger
            data = np.memmap(bin_file, dtype=dtype, mode='r', shape=(rows, cols))
            data = np.array(data)  # Convertir en array pour les opérations
            data = data / scale
            self.logger.debug(f"🔧 Cas spécial appliqué : {acc} (dtype={dtype}, scale={scale})")
        else:
            dtype = self.endian
            data = np.memmap(bin_file, dtype=dtype, mode='r', shape=(rows, cols))
            data = np.array(data)
        
        data = np.where(data == nodata, np.nan, data)
        
        lats = np.linspace(self.lat_start, self.lat_end, rows)
        lons = np.linspace(self.lon_start, self.lon_end, cols)
        
        da = xr.DataArray(
            data,
            dims=["lat", "lon"],
            coords={"lat": lats, "lon": lons},
            name="precip"
        )
        da = da.expand_dims(time=[np.datetime64(date)])
        
        da = da.assign_coords(lon=(((da.lon + 180) % 360) - 180))
        da = da.sortby("lon")
        
        return da

    def convert_downloaded_to_netcdf(self, bin_files_by_year):
        """
        Assemble les fichiers binaires d'une année en un seul fichier NetCDF.
        Utilise des chunks pour éviter de tout charger en mémoire.
        """
        for year, file_date_pairs in bin_files_by_year.items():
            # Inclure le nom du produit dans le fichier NetCDF
            output_nc = self.output_dir / f"persiann_{self.product}_{year}.nc"
            
            if output_nc.exists():
                self.logger.info(f"⏩ Skipping {year}, NetCDF already exists.")
                continue

            arrays = []
            for bin_file, date in sorted(file_date_pairs, key=lambda x: x[1]):
                try:
                    da = self.convert_bin_to_xarray(bin_file, date)
                    arrays.append(da)
                except Exception as e:
                    self.logger.error(f"❌ Failed to convert {bin_file.name}: {e}")

            if arrays:
                combined = xr.concat(arrays, dim="time")
                
                # 🔑 AJOUT : Définir des chunks pour l'écriture
                # Cela évite de tout charger en mémoire
                # On découpe en blocs de 500x500 pour la latitude/longitude
                # et 1 pour le temps (pour traiter jour par jour)
                combined = combined.chunk({"time": 1, "lat": 500, "lon": 500})
                
                # Ajouter des métadonnées pour garder la traçabilité
                combined.attrs["product"] = self.product
                combined.attrs["source"] = "PERSIANN"
                combined.attrs["resolution"] = self.config.get("resolution", "unknown")
                combined.attrs["description"] = self.config.get("description", "")
                combined.attrs["accumulation"] = self.accumulation
                
                combined.to_netcdf(output_nc)
                self.logger.info(f"💾 Saved NetCDF: {output_nc}")

    def download(self, start_date, end_date, output_dir=None, **kwargs):
        # Si output_dir est passé, on le prend en compte
        if output_dir is not None:
            self.output_dir = Path(output_dir)
            self.raw_dir = self.output_dir / "bin"
            self.raw_dir.mkdir(parents=True, exist_ok=True)
        start = datetime.strptime(start_date, "%Y-%m-%d") if isinstance(start_date, str) else start_date
        end = datetime.strptime(end_date, "%Y-%m-%d") if isinstance(end_date, str) else end_date
        dates = list(self._daterange(start, end))

        self.logger.info(f"🚀 Downloading PERSIANN '{self.product}' data from {start.date()} to {end.date()} using {self.max_workers} workers.")

        bin_files_by_year = {}

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {executor.submit(self._download_and_extract, date): date for date in dates}
            for future in as_completed(futures):
                bin_path = future.result()
                if bin_path:
                    try:
                        fname = bin_path.stem
                        
                        # Extraire l'année et le jour selon le format du produit
                        yy, doy = self._parse_date_from_filename(fname)
                        
                        year = 2000 + yy if yy < 50 else 1900 + yy
                        date = datetime.strptime(f"{year}-{doy:03}", "%Y-%j")
                        bin_files_by_year.setdefault(year, []).append((bin_path, date))
                    except Exception as e:
                        self.logger.warning(f"Failed to parse date from {bin_path.name}: {e}")

        self.convert_downloaded_to_netcdf(bin_files_by_year)

    def _parse_date_from_filename(self, fname):
        """
        Extrait l'année (yy) et le jour (doy) du nom de fichier.
        """
        format_type = self.filename_format
        
        if format_type == "prefix_dYYDDD":
            # Ex: ms6s4_d24001 → yy=24, doy=1
            yy = int(fname[7:9])
            doy = int(fname[9:12])
        
        elif format_type == "prefix_YYDDD":
            # Ex: rgccs1d_24001 → yy=24, doy=1
            parts = fname.split('_')
            if len(parts) > 1:
                yy = int(parts[1][0:2])
                doy = int(parts[1][2:5])
            else:
                raise ValueError(f"Format inattendu pour {fname}")
            
        elif format_type == "prefixYYDDD":
            # Ex: rgccs1d24001 → yy=24, doy=1
            yy = int(fname[8:10])   # Après le préfixe (ex: rgccs1d = 8 caractères)
            doy = int(fname[10:13]) # Les 3 chiffres suivants

        elif format_type in ["prefix_XXYYMMDDhh", "prefixXXYYMMDDhh"]:
            # Ex: PCCSCDR1d24010100 ou pdirnow1d240101
            import re
            match = re.search(r'(\d{6})', fname)  # Cherche 6 chiffres consécutifs
            if match:
                date_str = match.group(1)  # "240101"
                yy = int(date_str[0:2])    # "24"
                # Convertir YYMMDD en jour de l'année (DOY)
                doy = datetime.strptime(f"{date_str}", "%y%m%d").timetuple().tm_yday
            else:
                raise ValueError(f"Format inattendu pour {fname}")
        
        else:
            # Fallback (normalement jamais atteint)
            self.logger.warning(f"Format de nommage inconnu: {format_type}, utilisation du fallback")
            yy = int(fname[7:9])
            doy = int(fname[9:12])
        
        return yy, doy
    
    def extract(self, variables=None, start_date=None, end_date=None, as_long=False, **kwargs):
        """
        Extrait et filtre les données NetCDF existantes.

        Parameters
        ----------
        variables : list of str, optional
            Liste des variables à conserver (pour PERSIANN, seulement 'precip')
        start_date : str, optional
            Date de début de l'extraction (YYYY-MM-DD)
        end_date : str, optional
            Date de fin de l'extraction (YYYY-MM-DD)
        as_long : bool, optional
            Si True, retourne les données en format long (time, point)
        **kwargs : paramètres supplémentaires

        Returns
        -------
        xarray.Dataset or None
            Dataset filtré, ou None si aucun fichier trouvé
        """
        # Rechercher les fichiers NetCDF disponibles
        pattern = f"persiann_{self.product}_*.nc"
        nc_files = list(self.output_dir.glob(pattern))
        
        if not nc_files:
            self.logger.warning(f"Aucun fichier NetCDF trouvé dans {self.output_dir} avec le motif {pattern}")
            return None
        
        # Charger tous les fichiers
        ds = xr.open_mfdataset(nc_files, combine="by_coords")
        
        # Filtrer par temps si demandé
        if start_date:
            start = datetime.strptime(start_date, "%Y-%m-%d") if isinstance(start_date, str) else start_date
            ds = ds.sel(time=slice(start, None))
        if end_date:
            end = datetime.strptime(end_date, "%Y-%m-%d") if isinstance(end_date, str) else end_date
            ds = ds.sel(time=slice(None, end))
        
        # Filtrer par variables si demandé
        if variables:
            available_vars = [v for v in variables if v in ds.data_vars]
            if available_vars:
                ds = ds[available_vars]
            else:
                self.logger.warning(f"Aucune variable demandée trouvée dans le jeu de données.")
        
        # Convertir en format long (time, point) si demandé
        if as_long:
            ds = ds.stack(point=("lat", "lon")).reset_index("point")
        
        return ds
