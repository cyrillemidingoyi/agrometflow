# README2 - Import et utilisation du module VBA de contrôle qualité climatique

Ce guide explique comment importer le module VBA et exécuter les contrôles qualité sur des données climatiques journalières dans Excel.

## 1. Fichier VBA à utiliser

Le module prêt à l'emploi est :

- `examples/qc_excel_vba.bas`

Ce module contient la macro principale `RunQC_Daily`.

## 2. Préparer le fichier Excel

1. Créez ou ouvrez un classeur Excel.
2. Enregistrez-le au format **.xlsm** (classeur prenant en charge les macros).
3. Créez une feuille nommée exactement `Data`.
4. Mettez les en-têtes en ligne 1.

En-têtes obligatoires :

- `Year`
- `Month`
- `Day`

Variables recommandées (colonnes optionnelles mais supportées) :

- `Tx`, `Tn`, `rr`, `w`, `dd`, `sd`, `fs`, `sc`

Les données doivent commencer à la ligne 2.

## 3. Importer le module VBA dans Excel

1. Ouvrez l'éditeur VBA avec `Alt + F11`.
2. Dans l'éditeur : **Fichier > Importer un fichier...**
3. Sélectionnez le fichier `examples/qc_excel_vba.bas`.
4. Vérifiez que le module `QCClimate` apparaît dans votre projet VBA.
5. Sauvegardez le classeur.

## 4. Autoriser l'exécution des macros

Si les macros sont bloquées :

1. Excel > **Fichier > Options > Centre de gestion de la confidentialité**.
2. Ouvrez **Paramètres du Centre de gestion de la confidentialité**.
3. Dans **Paramètres des macros**, autorisez les macros (ou utilisez un emplacement approuvé).
4. Réouvrez le classeur si nécessaire.

## 5. Exécuter le contrôle qualité

1. Revenez à Excel.
2. Ouvrez **Développeur > Macros**.
3. Sélectionnez `RunQC_Daily`.
4. Cliquez **Exécuter**.

À la fin, une feuille `QC_Flags` est créée (ou réinitialisée) avec les anomalies détectées.

## 6. Résultats produits

La feuille `QC_Flags` contient les colonnes :

- `Var`
- `Year`
- `Month`
- `Day`
- `Hour`
- `Minute`
- `Value`
- `Test`

`Test` indique le type de contrôle déclenché.

## 7. Contrôles implémentés dans le module VBA

- `duplicate_dates`
- `daily_out_of_range`
- `daily_repetition`
- `temporal_coherence`
- `internal_consistency`
- `climatic_outliers` (IQR mensuel, avec seuil `outrange` dépendant de la variable)

## 8. Dépannage rapide

### Erreur "Missing header: Year/Month/Day"

Vérifiez les noms exacts des en-têtes en ligne 1 de la feuille `Data`.

### Aucune ligne dans `QC_Flags`

- Soit aucune anomalie n'a été détectée,
- soit les variables attendues ne sont pas présentes,
- soit des colonnes sont mal nommées.

### Macro introuvable

- Vérifiez que le module a bien été importé,
- et que le classeur est bien en `.xlsm`.

### Exécution lente

Sur de gros volumes, filtrez la période ou traitez les données en plusieurs passes.

## 9. Notes

- Le module est conçu pour des données journalières.
- Les colonnes `Hour` et `Minute` sont conservées dans la sortie pour compatibilité de format, mais vides dans ce flux daily.
