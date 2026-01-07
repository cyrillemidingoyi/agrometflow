
import pandas as pd
metadata = {
    "climate": {
        "T2M": {
            "unit": "°C",
            "description": {
                "en": "Mean air temperature at 2 meters"
            },
            "products": {
                "nasapower": {
                    "sources": {
                        "power": {
                                    "name": "T2M",
                                    "unit": "°C",
                                    "description": "Mean air temperature at 2 meters"
                        }
                    }
                },  
                "era5": {
                    "sources": {
                        "cds": {
                                    "name": dict(variable="2m_temperature", statistic ="24_hour_mean"),
                                    "unit": "K",
                                    "description": "Mean air temperature at 2 meters",
                                    "convert": lambda x: x - 273.15
                        }
                    }
                }
            }
        },
        "TMAX": {
            "unit": "°C",
            "description": {
                "en": "Maximum air temperature at 2 meters"
            },
            "products": {
                "nasapower": {
                    "sources": {
                        "power": {
                                    "name": "T2M_MAX",
                                    "unit": "°C",
                                    "description": "Maximum air temperature at 2 meters"
                        }
                    }
                },
                "era5": {
                    "sources": {
                        "cds": {
                            "name": dict(variable="2m_temperature", statistic ="24_hour_maximum"),
                            "unit": "K",
                            "description": "Maximum air temperature at 2 meters",
                            "convert": lambda x: x - 273.15
                        }
                    }
                }
            }
        },
        "TMIN": {
            "unit": "°C",
            "description": {
                "en": "Minimum air temperature at 2 meters"
            },
            "products": {
                "nasapower": {
                    "sources": {
                        "power": {
                            "name": "T2M_MIN",
                            "unit": "°C",
                            "description": "Minimum air temperature at 2 meters"
                        }
                    }
                },
                "era5": {
                    "sources": {
                        "cds": {
                            "name": dict(variable="2m_temperature", statistic ="24_hour_minimum"),
                            "unit": "K",
                            "description": "Minimum air temperature at 2 meters",
                            "convert": lambda x: x - 273.15
                        }
                    }
                }
            }
        },
        "TDEW": {
            "unit": "°C",
            "description": {
                "en": "Dew point temperature at 2 meters"
            },
            "products": {
                "nasapower": {
                    "sources": {
                        "power": {
                                "name": "T2MDEW",
                                "unit": "°C",
                                "description": "Dew point temperature at 2 meters"
                        }
                    }
                },
                "era5": {
                    "sources": {
                        "cds": {
                                "name": dict(variable="2m_dewpoint_temperature", statistic ="24_hour_mean"),
                                "unit": "K",
                                "description": "Mean dewpoint temperature at a height of 2 metres above the surface over the period 00h-24h local time.",
                                "convert": lambda x: x - 273.15
                        }
                    }
                }
            }
        },
        "RH2M": {
            "unit": "%",
            "description": {
                "en": "Relative humidity at 2 meters"
            },
            "products": {
                "nasapower": {
                    "sources": {
                        "power": {
                            "name": "RH2M",
                            "unit": "%",
                            "description": "Relative humidity at 2 meters"
                        }
                    }
                },
                "era5": {
                    "sources": {
                        "cds": {
                            "name": dict(variable="2m_relative_humidity", statistic ="24_hour_mean"),
                            "unit": "%",
                            "description": "Mean relative humidity at a height of 2 metres above the surface over the period 00h-24h local time.",
                        }
                    }
                }
            }
        },
        "VPD": {
            "unit": "hPa",
            "description": {
                "en": "Vapor pressure deficit"
            },
            "products": {
                "nasapower": {
                    "sources": {
                        "power": {
                            "name": "VPD",
                            "unit": "hPa",
                            "description": "Vapor pressure deficit"
                        }
                    }
                },
                "era5": {
                    "sources": {
                        "cds": {
                            "name": dict(variable="vapor_pressure_deficit", statistic ="24_hour_mean"),
                            "unit": "hPa",
                            "description": "Mean vapor pressure deficit at a height of 2 metres above the surface over the period 00h-24h local time.",
                        }
                    }
                }
            }
        },
        "WS10M": {
            "unit": "m/s",
            "description": {
                "en": "Wind speed at 10 meters"
            },
            "products": {
                "nasapower": {
                    "sources": {
                        "power": {
                            "name": "WS10M",
                            "unit": "m/s",
                            "description": "Wind speed at 10 meters"
                        }
                    }
                },
                "era5": {
                    "sources": {
                        "cds": {
                            "name": dict(variable="10m_u_component_of_wind", statistic ="24_hour_mean"),
                            "unit": "m/s",
                            "description": "Mean u-component of wind speed at a height of 10 metres above the surface over the period 00h-24h local time.",
                        }
                    }
                }
            }
        },
        "PR": {
            "unit": "mm/day",
            "description": {
                "en": "Total precipitation"
            },
            "products": {
                "nasapower": {
                    "sources": {
                        "power": {
                                "name": "PRECTOTCORR",
                                "unit": "mm/day",
                                "description": "The average MERRA-2 bias corrected total precipitation at the surface of the earth.",
                                "resolution": "0.5*0.625"
                        }
                    }
                },
                "era5": {
                    "sources": {
                        "cds": {
                                "name": dict(variable="precipitation_flux"),
                                "unit": "mm/day",
                                "description": "Total volume of liquid water (mm3) precipitated over the period 00h-24h local time per unit of area (mm2), per day",
                                "resolution": "0.1*0.1"
                        }
                    }
                },
                "chirps": {
                    "sources": {
                        "chc_ucsb_ftp": {
                                "name": "PR",
                                "unit": "mm/day",
                                "description": "Total daily precipitation",
                                "resolution": "0.05*0.05"
                        }
                    }
                },
                "tamsat": {
                    "sources": {
                        "jasmin_http": {
                                "name": "PR",
                                "unit": "mm/day",
                                "description": "Total daily precipitation",
                                "resolution": "0.05*0.05"
                        }
                    }
                },
                "rfe2": {
                    "sources": {
                        "noaa_cpc_ftp": {
                                "name": "PR",
                                "unit": "mm/day",
                                "description": "Total daily precipitation",
                                "resolution": "0.05*0.05"
                        }
                    }
                },
                "persiann": {
                    "sources": {
                        "chrs_ftp": {                    
                                "name": "PR",
                                "unit": "mm/day",
                                "description": "Total daily precipitation",
                                "resolution": "0.05*0.05"
                        }
                    }
                },
                "mswep": {
                    "sources": {
                        "gloh2o_drive": { 
                                "name": "PR",
                                "unit": "mm/day",
                                "description": "Total daily precipitation",
                                "resolution": "0.05*0.05"
                        }
                    }
                },
                "cmorphv1": {
                    "sources": {
                        "noaa_cpc_ftp": { 
                                "name": "PR",
                                "unit": "mm/day",
                                "description": "Total daily precipitation",
                                "resolution": "0.05*0.05"
                        }
                    }
                },
                "arc2": {
                    "sources": {
                        "noaa_cpc_ftp": { 
                                "name": "PR",
                                "unit": "mm/day",
                                "description": "Total daily precipitation",
                                "resolution": "0.05*0.05"
                        }
                    }
                },
                "imerg": {
                    "sources": {
                        "nasa_gesdisc_http": { 
                                "name": "IMERG_PRECTOT",
                                "unit": "mm/day",
                                "description": "Daily mean precipitation rate (combined microwave-IR) estimate",
                                "resolution": "0.1*0.1"
                        }
                    }
                },
                
            }
        },
        "WS2M": {
            "unit": "m/s",
            "description": {
                "en": "Wind speed at 2 meters"
            },
            "products": {
                "nasapower": {
                    "sources": {
                        "power": {
                                "name": "WS2M",
                                "unit": "m/s",
                                "description": "Wind speed at 2 meters"
                        }
                    }
                },
                "era5": {
                    "sources": {
                        "cds": {
                                "name": dict(variable="10m_wind_speed", statistic="24_hour_mean"),
                                "unit": "m/s",
                                "description": "Mean wind speed at a height of 10 metres above the surface over the period 00h-24h local time",
                                "convert": lambda x: x * 0.75
                        }
                    }
                }
            }
        },
        "SRAD": {
            "unit": "MJ m-2 day-1",
            "description": {
                "en": "Surface solar radiation"
            },
            "products": {
                "nasapower": {
                    "sources": {
                        "power": {
                                "name": "ALLSKY_SFC_SW_DWN",
                                "unit": "W/m²",
                                "description": "All-sky surface shortwave downward radiation",
                        }
                    }
                },
                "era5": {
                    "sources": {
                        "cds": {
                                "name": dict(variable="solar_radiation_flux"),
                                "unit": "J m-2 day-1",
                                "description": "Total amount of energy provided by solar radiation at the surface over the period 00-24h local time per unit area and time",
                                "convert": lambda x: x / 1000000
                        }
                    }
                }
            }
        },
        "SURFPRES": {
            "unit": "Pa",
            "description": {
                "en": "Surface pressure"
            },
            "products": {
                "nasapower": {
                    "sources": {
                        "power": {
                                "name": "PS",
                                "unit": "hPa",
                                "description": "Surface pressure",
                                "convert": lambda x: x * 100
                        }
                    }
                },
                "era5": {
                    "sources": {
                        "cds": {
                                "name": dict(variable="vapour_pressure, statistic=24_hour_mean"),
                                "unit": "hPa",
                                "description": "Contribution to the total atmospheric pressure provided by the water vapour over the period 00-24h local time per unit of time",
                                "convert": lambda x: x * 100
                        }
                    }
                }
            }
        }
    },
    "soil": {
        "sand": {
            "unit": "%",
            "description": {
                "en": "Sand content"
            },
            "sources": {
                "soilgrids": {
                    "name": "sand",
                    "unit": "%",
                    "description": "Sand fraction"
                }
            }
        },
        "clay": {
            "unit": "%",
            "description": {
                "en": "Clay content"
            },
            "sources": {
                "soilgrids": {
                    "name": "clay",
                    "unit": "%",
                    "description": "Clay fraction"
                }
            }
        },
        "ocd": {
            "unit": "g/kg",
            "description": {
                "en": "Organic carbon content"
            },
            "sources": {
                "soilgrids": {
                    "name": "ocd",
                    "unit": "g/kg",
                    "description": "Organic carbon density"
                }
            }
        }
    }
}

products = {
    "nasapower": {
        "name": "NASA POWER",
        "description": "NASA Prediction of Worldwide Energy Resources (POWER) project",
        "url": "https://power.larc.nasa.gov/"
        
    },
    "era5": {
        "name": "ERA5",
        "description": "ECMWF Reanalysis 5th Generation",
        "url": "https://www.ecmwf.int/en/forecasts/datasets/reanalysis-datasets/era5"
    },
    "chirps": {
        "name": "CHIRPS",
        "description": "Climate Hazards Group InfraRed Precipitation with Station data",
        "url": "https://www.chc.ucsb.edu/data/chirps"
    },
    "tamsat": {
        "name": "TAMSAT",
        "description": "Tropical Applications of Meteorology using SATellite data",
        "url": "https://www.tamsat.org.uk/"
    },
    "rfe2": {
        "name": "RFE2",
        "description": "Rainfall Estimation Algorithm version 2",
        "url": "https://www.cpc.ncep.noaa.gov/products/janowiak/rfe/"
    },
    "persiann": {
        "name": "PERSIANN",
        "description": "Precipitation Estimation from Remotely Sensed Information using Artificial Neural Networks",
        "url": "http://persiann.eng.uci.edu/"
    },
    "mswep": {
        "name": "MSWEP",
        "description": "Multi-Source Weighted-Ensemble Precipitation",
        "url": "https://www.gloh2o.org/mswep/"
    },
    "cmorphv1": {
        "name": "CMORPH V1",
        "description": "CPC Morphing technique for precipitation estimation",  
        "url": "https://www.cpc.ncep.noaa.gov/products/janowiak/cmorph/"
    },
    "arc2": {
        "name": "ARC2",
        "description": "African Rainfall Climatology version 2",
        "url": "https://www.chc.ucsb.edu/data/arc2"
    },
    "soilgrids": {
        "name": "SoilGrids",
        "description": "Global soil information system",
        "url": "https://soilgrids.org/"
    },
    "imerg": {
        "name": "IMERG",
        "description": "Integrated Multi-satellite Retrievals for GPM",
        "url": "https://pmm.nasa.gov/GPM/imerg"
    },
    "persiann_ccs": {
        "name": "PERSIANN-CC",
        "description": "PERSIANN Climate Data",
        "url": "http://persiann.eng.uci.edu/"
    },
    "persiann_ccscdr": {
        "name": "PERSIANN-CCS CDR",
        "description": "PERSIANN Climate Data V1",
        "url": "http://persiann.eng.uci.edu/"
    },
    "imergF": {
        "name": "IMERG V07 Final",
        "description": "IMERG V06 Early",
        "url": "https://pmm.nasa.gov/GPM/imerg"
    },
    "imergL": {
        "name": "IMERG V07 Late",
        "description": "IMERG V06 Late",
        "url": "https://pmm.nasa.gov/GPM/imerg"
    }
}

varCMIP = {
    "pr": lambda x: x*86400,
    "sfcWind": lambda x: x*0.75,
    "tas": lambda x: x - 273.15,
    "tasmin" : lambda x: x - 273.15,
    "tasmax" : lambda x: x - 273.15,
    "tdps":  lambda x: x - 273.15,
    "rsds" : "", # W/m2
    "ps" : "" # Pa
}   

# Function to retrieve the list of available climate products from metadata
def list_clim_products():
    """
    Return a sorted list of available climate products.

    Returns
    -------
    list
        Sorted list of product names.
    """
    products = set()
    for var_info in metadata.get("climate", {}).values():
        products.update(var_info.get("products", {}).keys())
    return sorted(products)


# Function to retrieve the list of available climate sources and products from a given variables
def list_sources_for_variable(variable_name):
    """
    Return a dictionary listing all sources for a given climate product.

    Parameters:
        variable_name (str): The climate variable (e.g., "T2M", "PR").

    Returns:
        dict: A dict with {product: [source list]} format or an empty dict if not found.
    """
    product_data = metadata.get("climate", {}).get(variable_name)
    if not product_data:
        return {}

    sources = {}
    for product_key, product_info in product_data.get("products", {}).items():
        for source in product_info.get("sources", {}).keys():
            sources.setdefault(product_key, []).append(source)

    return sources


def get_convert_func(var_name, product, source):
    """
    Retrieve the convert function for a given variable name from metadata.
    """
    if "convert" in metadata.get("climate", {}).get(var_name, {}).get("products", {}).get(product, {}).get("sources", {}).get(source, {}):
        return metadata["climate"][var_name]["products"][product]["sources"][source]["convert"]
    return None

# Function to retrieve the list of available soil sources
def list_soil_sources():
    sources = set()
    for var_info in metadata.get("soil", {}).values():
        sources.update(var_info.get("sources", {}).keys())
    return sorted(sources)


# Function to retrieve the list of available variables for a given climate source  in a dataframe format
import pandas as pd

def _get_variables_by_source(metadata, source, category="climate"):
    """
    Return a DataFrame of available climate variables and their metadata
    for a given source. Columns are inferred dynamically.

    Parameters
    ----------
    metadata : dict
        Structured metadata dictionary.
    source : str
        Name of the climate source (e.g., "nasapower", "era5").

    Returns
    -------
    pd.DataFrame
        DataFrame with dynamically inferred columns including 'variable'.
    """
    records = []

    for var_name, var_info in metadata.get(category, {}).items():
        source_info = var_info.get("sources", {}).get(source)
        if source_info:
            record = {"variable": var_name}
            record.update(source_info)  # merge all available fields
            records.append(record)

    if not records:
        raise ValueError(f"No {category} variables found for source '{source}'.")

    return pd.DataFrame(records)


def get_variables_clim_by_source(source):
    return _get_variables_by_source(metadata, source, category="climate")

# Function to retrieve the list of available variables for a given soil source
def get_variables_soil_by_source(source):
    return _get_variables_by_source(metadata, source, category="soil")

# find all available sources for a given variable
def get_sources_for_specific_variable(metadata, variable, category):
    """
    Return a list of available sources for a given climate variable.

    Parameters
    ----------
    metadata : dict
        Structured metadata dictionary.
    variable : str
        Name of the climate variable (e.g., "T2M", "PR").

    Returns
    -------
    list
        Sorted list of source names that provide this variable.
    """
    climate_data = metadata.get(category, {})
    if variable not in climate_data:
        raise ValueError(f"Variable '{variable}' not found in climate metadata.")

    sources = climate_data[variable].get("sources", {})
    return sorted(sources.keys())

def get_sources_for_clim_variable(variable):
    return get_sources_for_specific_variable(metadata, variable, category="climate")

def get_sources_for_soil_variable(variable):
    return get_sources_for_specific_variable(metadata, variable, category="soil")