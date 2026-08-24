"""
Pipeline principal - Orchestration des étapes :
1. Téléchargement des données
2. Calcul des indicateurs
3. Agrégation des résultats
"""

import sys
import yaml
import pandas as pd
from pathlib import Path
from datetime import datetime

# Ajouter le chemin du projet
sys.path.append(str(Path(__file__).parent.parent.parent))

from agrometflow.climate.tamsat import TamsatDownloader
from agrometflow.climate.persiann import PersiannDownloader
from agrometflow.indicators import (
    CumulativeRainfall,
    RainyDays,
    DryDays,
    DrySpell,
    SDII,
    R95p,
)


def run_pipeline(config):
    """
    Exécute le pipeline complet à partir d'une configuration.
    
    Parameters
    ----------
    config : dict
        Configuration du pipeline
    
    Returns
    -------
    dict
        Résultats du pipeline
    """
    
    print("\n" + "=" * 60)
    print("🚀 PIPELINE AGRISCALE")
    print("=" * 60)
    
    # ============================================================
    # 1. TÉLÉCHARGEMENT DES DONNÉES
    # ============================================================
    
    print("\n📥 1. TÉLÉCHARGEMENT DES DONNÉES...")
    print("-" * 40)
    
    climate = config.get("climate", {})
    source = climate.get("source", "tamsat")
    
    print(f"   Source : {source}")
    print(f"   Période : {climate.get('start_date')} → {climate.get('end_date')}")
    print(f"   Variables : {climate.get('variables')}")
    
    # Sélectionner le downloader
    if source == "tamsat":
        from agrometflow.climate.tamsat import TamsatDownloader
        downloader = TamsatDownloader(
            product=source,
            output_dir=climate.get("output_dir", "data/climate/tamsat"),
            verbose=config.get("global", {}).get("verbose", True)
        )
    elif source == "persiann":
        from agrometflow.climate.persiann import PersiannDownloader
        downloader = PersiannDownloader(
            product=source,
            output_dir=climate.get("output_dir", "data/climate/persiann"),
            verbose=config.get("global", {}).get("verbose", True)
        )
    else:
        raise ValueError(f"Source non supportée : {source}")
    
    # Télécharger
    downloader.download(
        start_date=climate.get("start_date"),
        end_date=climate.get("end_date"),
        bbox=climate.get("bbox")
    )
    
    # Extraire
    ds = downloader.extract(
        variables=climate.get("variables", ["PR"]),
        start_date=climate.get("start_date"),
        end_date=climate.get("end_date")
    )
    
    if ds is None:
        raise ValueError("❌ Échec du téléchargement des données")
    
    print(f"   ✅ Données chargées : {dict(ds.dims)}")
    
    # ============================================================
    # 2. EXTRACTION DES POINTS (VERSION CORRIGÉE)
    # ============================================================
    
    print("\n📍 2. EXTRACTION DES POINTS...")
    print("-" * 40)
    
    points = climate.get("points", [])
    point_data = {}
    
    if not points:
        print("   ⚠️ Aucun point spécifié, utilisation du premier pixel")
        points = [{"lat": float(ds.lat.values[0]), "lon": float(ds.lon.values[0])}]
    
    for i, point in enumerate(points):
        if isinstance(point, dict):
            lat = point.get("lat")
            lon = point.get("lon")
        else:
            lat, lon = point
        
        # ✅ CORRECTION : Utiliser isel au lieu de sel
        # Trouver les index les plus proches
        lat_idx = abs(ds.lat.values - lat).argmin()
        lon_idx = abs(ds.lon.values - lon).argmin()
        
        # Extraire avec isel
        da = ds.PR.isel(lat=lat_idx, lon=lon_idx)
        da.name = 'PR'
        
        point_data[f"point_{i}"] = {
            "lat": float(ds.lat.values[lat_idx]),
            "lon": float(ds.lon.values[lon_idx]),
            "data": da
        }
        
        print(f"   ✅ Point {i} : {point_data[f'point_{i}']['lat']:.2f}°N, {point_data[f'point_{i}']['lon']:.2f}°E")
    
    # ============================================================
    # 3. CALCUL DES INDICATEURS
    # ============================================================
    
    print("\n📊 3. CALCUL DES INDICATEURS...")
    print("-" * 40)
    
    indicator_classes = {
        "cumulative_rainfall": CumulativeRainfall,
        "rainy_days": RainyDays,
        "dry_days": DryDays,
        "dry_spell": DrySpell,
        "sdii": SDII,
        "r95p": R95p,
    }
    
    indicators_config = config.get("indicators", {})
    
    if not indicators_config:
        indicators_config = {
            "cumulative_rainfall": {"period": "monthly"},
            "rainy_days": {"threshold": 1.0},
            "sdii": {"threshold": 1.0},
        }
        print("   ⚠️ Aucun indicateur spécifié, utilisation des indicateurs par défaut")
    
    results = {}
    
    for indicator_name, params in indicators_config.items():
        if indicator_name not in indicator_classes:
            print(f"   ⚠️ Indicateur inconnu : {indicator_name}")
            continue
        
        # S'assurer que 'variable' est présent
        if "variable" not in params:
            params["variable"] = "PR"
        
        try:
            indicator = indicator_classes[indicator_name](**params)
        except TypeError:
            # Certains indicateurs n'acceptent pas 'variable'
            filtered_params = {k: v for k, v in params.items() if k != "variable"}
            indicator = indicator_classes[indicator_name](**filtered_params)
        
        point_results = {}
        for point_id, point_info in point_data.items():
            try:
                result = indicator.compute(point_info["data"])
                
                if hasattr(result, 'values'):
                    if result.ndim == 0:
                        value = float(result.values.item())
                    else:
                        value = float(result.mean().values)
                else:
                    value = float(result)
                
                point_results[point_id] = value
                
            except Exception as e:
                print(f"   ❌ Erreur pour {indicator_name} sur {point_id} : {e}")
                point_results[point_id] = None
        
        results[indicator_name] = point_results
        
        for point_id, value in point_results.items():
            if value is not None:
                print(f"   ✅ {indicator_name} ({point_id}) : {value:.2f}")
    
    # ============================================================
    # 4. CRÉATION DU DATAFRAME
    # ============================================================
    
    print("\n📊 4. CRÉATION DU TABLEAU FINAL...")
    print("-" * 40)
    
    df_results = pd.DataFrame(results).T
    df_results.index.name = "Indicateur"
    print(df_results)
    
    # ============================================================
    # 5. SAUVEGARDE
    # ============================================================
    
    print("\n💾 5. SAUVEGARDE DES RÉSULTATS...")
    print("-" * 40)
    
    project_name = config.get("global", {}).get("project_name", "pipeline_results")
    results_dir = Path("results") / project_name
    results_dir.mkdir(parents=True, exist_ok=True)
    
    csv_path = results_dir / "indicators_results.csv"
    df_results.to_csv(csv_path)
    print(f"   ✅ CSV sauvegardé : {csv_path}")
    
    config_path = results_dir / "config.yml"
    with open(config_path, 'w') as f:
        yaml.dump(config, f, default_flow_style=False)
    print(f"   ✅ Configuration sauvegardée : {config_path}")
    
    print("\n" + "=" * 60)
    print("✅ PIPELINE TERMINÉ")
    print("=" * 60)
    
    print(f"\n📁 Résultats dans : {results_dir}")
    print(f"📊 Indicateurs calculés : {len(df_results)}")
    print(f"📍 Points : {len(point_data)}")
    
    return {
        "data": ds,
        "points": point_data,
        "indicators": results,
        "dataframe": df_results,
        "results_dir": results_dir
    }


def run_pipeline_from_yaml(config_file):
    """
    Exécute le pipeline à partir d'un fichier YAML.
    
    Parameters
    ----------
    config_file : str
        Chemin vers le fichier YAML
    
    Returns
    -------
    dict
        Résultats du pipeline
    """
    with open(config_file, 'r') as f:
        config = yaml.safe_load(f)
    
    return run_pipeline(config)