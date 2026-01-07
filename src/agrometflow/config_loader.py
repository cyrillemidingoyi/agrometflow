import yaml
import os


def load_config(config_path):
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    # Validation minimale
    if "climate" not in config and "soil" not in config and "projections" not in config:
        raise ValueError("Config must contain at least one of: 'climate', 'soil'.")

    # Valeurs par défaut globales
    config.setdefault("global", {})
    config["global"].setdefault("verbose", False)
    config["global"].setdefault("log_file", None)
    config["global"].setdefault("project_name", "agromet_project")

    return config
