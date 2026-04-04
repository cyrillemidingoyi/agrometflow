# Annexe - RFE et ARC2

Cette annexe synthétise les slides techniques sur les estimateurs de pluie utilisés dans le support.

## RFE (Rainfall Estimator)

## Entrées principales

- Gauge (GTS)
- Infrared (GPI)
- Passive microwave (SSM/I, AMSU-B)

## Caractéristiques

- Analyse journalière (06Z-06Z)
- Résolution spatiale 0.1 deg
- Domaine principal de formation: Afrique

## Logique de fusion

Le signal satellite décrit surtout la forme spatiale de la pluie, tandis que les stations contraignent l'amplitude locale. La combinaison réduit l'erreur aléatoire via des méthodes de type maximum de vraisemblance.

## Points d'attention

- Les stations restent la référence locale, mais la couverture est incomplète.
- Le GPI représente bien la structure spatiale, mais peut sous-estimer certaines pluies convectives.
- Les capteurs micro-ondes améliorent les estimations des événements intenses, avec une couverture temporelle plus faible.

## ARC2 (African Rainfall Climatology, version 2)

## Positionnement

ARC2 est utilisé pour le suivi climatique opérationnel en Afrique avec une série longue et homogène.

## Entrées

- Gauge (GTS)
- Infrared (GPI)

## Caractéristiques

- Résolution 0.1 deg
- Journalier (06Z-06Z)
- Période longue (depuis 1983)

## Pourquoi comparer ARC2 et RFE

- RFE capture mieux certaines intensités locales (grâce au micro-onde).
- ARC2 est plus homogène sur le long terme grâce à des entrées plus constantes.

En pratique, le choix dépend de l'usage:
- suivi d'anomalies climatiques sur longues périodes: ARC2
- analyse d'événements intenses: RFE ou produits multi-capteurs récents
