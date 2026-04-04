# Sources de données climatiques et contrôle qualité avec agrometflow

Support de formation pour l'atelier au Cameroon, structuré pour une lecture rapide sur GitHub Pages.

## Contexte

Les services climatiques dépendent de la qualité des données. Selon les recommandations OMM, la production de données météorologiques fiables nécessite un système de gestion de la qualité explicite et documenté.

## Ce que couvre la formation

1. Typologies de données:
   - observations in situ
   - télédétection
   - réanalyses et projections climatiques
2. Infrastructures d'accès:
   - GHCN-D, GSOD, NASA POWER, CHIRPS, IMERG, ARC2, ERA5, ERA5-Land, MERRA-2
3. Contrôle qualité:
   - détection
   - vérification à la source
   - correction traçable
   - relance des tests
   - ajout des flags

## Messages clés

- Les stations restent la référence, mais la couverture est souvent insuffisante.
- Les produits satellitaires améliorent fortement la couverture spatiale, avec des biais à documenter.
- Les réanalyses apportent des séries cohérentes multi-variables, mais avec des limites de résolution locale.
- L'approche recommandée est hybride: stations + satellite + réanalyse + correction locale des biais.

## Parcours conseillé

1. [Sources de données climatiques](sources.md)
2. [Contrôle qualité](controle-qualite.md)
3. [Notebooks de formation](notebooks.md)
4. [Annexe RFE et ARC2](rfe-arc2.md)
