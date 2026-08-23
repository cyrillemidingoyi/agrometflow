"""
DÉMONSTRATION DU PIPELINE
"""

import sys
from pathlib import Path

# Ajouter le chemin du projet
sys.path.append(str(Path(__file__).parent / "src"))

from agrometflow.pipeline.pipeline import run_pipeline

print("\n" + "=" * 60)
print("🌾 DEMO PIPELINE AGRISCALE")
print("=" * 60)

# Configuration
config = {
    "global": {
        "project_name": "Dakar_2020",
        "verbose": True,
    },
    "climate": {
        "source": "tamsat",
        "product": "tamsat",
        "start_date": "2020-08-01",
        "end_date": "2020-08-31",
        "variables": ["PR"],
        "bbox": [-5, 25, -20, 20],
        "points": [{"lat": 14.7167, "lon": -17.4677}],
        "output_dir": "data/climate/tamsat"
    },
    "indicators": {
        "cumulative_rainfall": {"period": "monthly"},
        "rainy_days": {"threshold": 1.0, "variable": "PR"},
        "dry_days": {"threshold": 1.0, "variable": "PR"},
        "sdii": {"threshold": 1.0, "variable": "PR"},
        "r95p": {"percentile": 95, "variable": "PR"}
    }
}

print("\n📝 Configuration :")
print(f"   Source : {config['climate']['source']}")
print(f"   Période : {config['climate']['start_date']} → {config['climate']['end_date']}")
print(f"   Indicateurs : {list(config['indicators'].keys())}")
print(f"   Point : {config['climate']['points'][0]}")

# Exécuter le pipeline
results = run_pipeline(config)

# Afficher les résultats
print("\n📊 Résultats (DataFrame) :")
print(results["dataframe"])

print("\n" + "=" * 60)
print("✅ DÉMONSTRATION TERMINÉE")
print("=" * 60)