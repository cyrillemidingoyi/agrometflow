# Sources de donnees climatiques

## Importance des donnees climatiques

Les donnees climatiques sont la base des decisions agroclimatiques: suivi saisonnier, gestion du risque, conseil aux producteurs et calibration des modeles.

Sans une base de donnees fiable et bien documentee, les analyses peuvent conduire a des recommandations inadaptees.

## 1) Typologie des donnees

<!-- markdownlint-disable MD024 -->

### 1.1 Observations in situ

#### Description

Mesures directes considerees comme reference:

- stations synoptiques
- stations automatiques
- postes pluviometriques

#### Forces

- reference terrain pour la validation
- utiles pour la calibration locale

#### Limites

- faible densite spatiale dans plusieurs regions
- metadonnees parfois incompletes
- ruptures de series et changements d'instruments

### 1.2 Teledetection

#### Description

Mesures indirectes satellitaires ou radar, generalement fusionnees avec des observations in situ.

#### Forces

- tres bonne couverture spatiale
- utile pour le suivi regional de la pluie

#### Limites

- incertitudes selon climat, saison et surface
- biais regionaux persistants
- limites sur les convections intenses

### 1.3 Reanalyses et projections

#### Description

- Reanalyses: assimilation des observations pour reconstruire un etat coherent du passe.
- Projections: evolution possible du climat selon differents scenarios.

#### Forces

- series coherentes sur de longues periodes
- couverture spatiale et temporelle continue
- approche multi-variables utile pour l'analyse agroclimatique

#### Limites

- biais dependants des variables et des regions
- resolution souvent trop grossiere pour certains usages locaux
- incertitudes liees aux objectifs de l'etude

<!-- markdownlint-enable MD024 -->

## 2) Infrastructures d'acces aux donnees

L'infrastructure d'acces ne correspond pas a une typologie de donnees: c'est le moyen de recuperer les jeux de donnees (portails, API, catalogues institutionnels).

### 2.1 Portails et catalogues pour observations in situ

- GHCN-Daily: <https://www.ncei.noaa.gov/access/search/dataset-search>
- GSOD: <https://www.ncei.noaa.gov/access/search/dataset-search>
- Portails NMHS (souvent avec contraintes d'acces)

### 2.2 Portails et catalogues pour produits de teledetection

- CHIRPS: <https://www.chc.ucsb.edu/data/chirps>
- GPM-IMERG: <https://gpm.nasa.gov/data/imerg>
- PERSIANN: <https://persiann.eng.uci.edu/CHRSdata/>
- CMORPH: <https://www.ncei.noaa.gov/products/climate-data-records/precipitation-cmorph>
- RFE2: <https://earlywarning.usgs.gov/fews/product/48/>
- TAMSAT: <https://research.reading.ac.uk/tamsat/data-access/>
- MSWEP: <https://www.gloh2o.org/>
- ARC2: <https://ftp.cpc.ncep.noaa.gov/fews/fewsdata/africa/arc2/>

### 2.3 Portails et catalogues pour reanalyses/projections

- ERA5: <https://cds.climate.copernicus.eu/datasets/reanalysis-era5-single-levels>
- ERA5-Land: <https://cds.climate.copernicus.eu/datasets/reanalysis-era5-land>
- MERRA-2: <https://gmao.gsfc.nasa.gov/reanalysis/MERRA-2/>
- JRA-55: <https://jra.kishou.go.jp/JRA-55/index_en.html>
- NCEP/NCAR: <https://psl.noaa.gov/data/gridded/data.ncep.reanalysis.html>
- CFSR/CFSv2: <https://psl.noaa.gov/data/gridded/data.cfsr.html>

## 3) Lien avec le controle de qualite

Une fois la source et l'infrastructure choisies, la prochaine etape est le controle de qualite: detection des anomalies, verification a la source, correction tracable et documentation des flags.

## Conclusion

La pratique recommandee en formation et en projet operationnel est une combinaison des sources, en explicitant les incertitudes et les biais, puis en appliquant un controle de qualite systematique.
