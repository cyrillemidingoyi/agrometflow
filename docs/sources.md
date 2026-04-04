# Sources de données climatiques

## 1) Observations in situ

### Description

Mesures directes considérées comme référence:

- stations synoptiques
- stations automatiques
- postes pluviométriques

### Forces

- référence terrain pour la validation
- utiles pour la calibration locale

### Limites

- faible densité spatiale dans plusieurs régions
- métadonnées parfois incomplètes
- ruptures de séries et changements d'instruments

### Infrastructures d'accès

- GHCN-Daily: https://www.ncei.noaa.gov/access/search/dataset-search
- GSOD: https://www.ncei.noaa.gov/access/search/dataset-search
- Portails NMHS (souvent avec contraintes d'accès)

## 2) Télédétection

### Description

Mesures indirectes satellitaires ou radar, généralement fusionnées avec des observations in situ.

### Forces

- très bonne couverture spatiale
- utile pour le suivi régional de la pluie

### Limites

- incertitudes selon climat, saison et surface
- biais régionaux persistants
- limites sur les convections intenses

### Produits de référence

- CHIRPS: https://www.chc.ucsb.edu/data/chirps
- GPM-IMERG: https://gpm.nasa.gov/data/imerg
- PERSIANN: https://persiann.eng.uci.edu/CHRSdata/
- CMORPH: https://www.ncei.noaa.gov/products/climate-data-records/precipitation-cmorph
- RFE2: https://earlywarning.usgs.gov/fews/product/48/
- TAMSAT: https://research.reading.ac.uk/tamsat/data-access/
- MSWEP: https://www.gloh2o.org/
- ARC2: https://ftp.cpc.ncep.noaa.gov/fews/fewsdata/africa/arc2/

## 3) Réanalyses et projections

### Description

- Réanalyses: assimilation des observations pour reconstruire un état cohérent du passé.
- Projections: évolution possible du climat selon différents scénarios.

### Limites

- biais dépendants des variables et des régions
- résolution souvent trop grossière pour certains usages locaux
- incertitudes liées aux objectifs de l'étude

### Produits à citer

- ERA5: https://cds.climate.copernicus.eu/datasets/reanalysis-era5-single-levels
- ERA5-Land: https://cds.climate.copernicus.eu/datasets/reanalysis-era5-land
- MERRA-2: https://gmao.gsfc.nasa.gov/reanalysis/MERRA-2/
- JRA-55: https://jra.kishou.go.jp/JRA-55/index_en.html
- NCEP/NCAR: https://psl.noaa.gov/data/gridded/data.ncep.reanalysis.html
- CFSR/CFSv2: https://psl.noaa.gov/data/gridded/data.cfsr.html

## Conclusion

La pratique recommandée en formation et en projet opérationnel est une combinaison des sources, en explicitant les incertitudes et les biais.
