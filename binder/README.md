# Utiliser agrometflow sans notebook

Ce dossier Binder installe une version volontairement légère d'agrometflow pour la formation.
Les participants peuvent lancer les téléchargements depuis un fichier YAML, sans écrire de Python.

## 1. Ouvrir un terminal Binder

Dans JupyterLab:

`File > New > Terminal`

## 2. Renseigner les identifiants LSA SAF

```bash
export LSASAF_USERNAME="votre_identifiant"
export LSASAF_PASSWORD="votre_mot_de_passe"
```

## 3. Lancer un téléchargement sur points

```bash
agrometflow-run examples/lsasaf_etp_points.yml --max-workers 2
```

## 4. Lancer un téléchargement sur bbox

```bash
agrometflow-run examples/lsasaf_etp_bbox.yml --max-workers 2
```

## Modifier la zone ou les points

Les participants modifient uniquement le fichier YAML:

- `start_date` et `end_date` pour la période.
- `points` pour une liste de coordonnées `[lon, lat]`.
- `bbox` pour une emprise `[lon_min, lat_min, lon_max, lat_max]`.
- `max_workers` pour le nombre de téléchargements parallèles.

## Recommandation Binder

Garder `max_workers` entre `1` et `2` dans Binder. Les fichiers LSA SAF sont lourds et
un parallélisme plus élevé peut saturer la mémoire du serveur Binder.

Les détails des URL sont écrits dans le fichier de log défini par `global.log_file`.
Le terminal affiche seulement les messages importants et la progression.
