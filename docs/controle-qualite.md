# Contrôle qualité des données climatiques

## Pourquoi un système QC explicite

Le contrôle qualité doit être systématique et cohérent. L'absence de système de gestion de la qualité peut conduire à la perte de données d'origine ou à l'introduction d'erreurs supplémentaires.

## Chaîne de contrôle recommandée

1. Formater les données et appliquer les tests QC.
2. Vérifier les valeurs suspectes dans la source d'origine.
3. Corriger dans une copie de travail (pas dans le brut).
4. Reconvertir le fichier dans le format commun.
5. Relancer les tests.
6. Affiner les sorties: retirer les faux positifs et ajouter les cas non détectés.
7. Ajouter les indicateurs (flags) dans le jeu final.

## Tests standards

## 1. Erreurs grossières

### 1.1 duplicate_dates / duplicate_times

- But: détecter les dates ou instants dupliqués.
- Variables: toutes.
- Sorties: texte (lignes flaggées).

### 1.2 daily_repetition / subdaily_repetition

- But: détecter des séquences de valeurs consécutives identiques.
- Variables: toutes.
- Sorties: texte.

### 1.3 daily_out_of_range / subdaily_out_of_range

- But: détecter les valeurs hors seuils définis.
- Variables usuelles: Tx, Tn, rr, dd, w, sc, sd.
- Sorties: texte.

### 1.4 wmo_gross_errors

- But: détecter les valeurs hors bornes OMM selon latitude et saison.
- Variables usuelles: ta, td, w, p, mslp.
- Sorties: texte.

## 2. Tolérance statistique

### climatic_outliers

- But: détecter des valeurs rares ou éloignées de la distribution attendue.
- Variables: Tx, Tn, ta, rr, sc, sd, fs.
- Principe: p25 - n*IQR < X < p75 + n*IQR.
- Sorties: texte + figures (boxplots).

## 3. Cohérence temporelle

### temporal_coherence

- But: détecter des sauts improbables entre jours successifs.
- Variables usuelles: Tx, Tn, w, sd.
- Sorties: texte.

## Message pédagogique à rappeler

Un flag est un signal d'investigation, pas une suppression automatique de la donnée.
