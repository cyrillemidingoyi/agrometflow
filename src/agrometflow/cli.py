import argparse
import sys
from pathlib import Path

from agrometflow.config_loader import load_config
from agrometflow.pipeline import run_pipeline


def main(argv=None):
    parser = argparse.ArgumentParser(
        prog="agrometflow-run",
        description="Run agrometflow downloads from a YAML configuration file.",
    )
    parser.add_argument("config", help="Path to the YAML configuration file.")
    parser.add_argument(
        "--max-workers",
        type=int,
        default=None,
        help="Number of parallel downloads for compatible sources.",
    )
    parser.add_argument(
        "--log-file",
        default=None,
        help="Write detailed logs to this file.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable debug messages in the log file.",
    )
    parser.add_argument(
        "--lsasaf-username",
        default=None,
        help="LSA SAF username. Prefer environment variable LSASAF_USERNAME for shared machines.",
    )
    parser.add_argument(
        "--lsasaf-password",
        default=None,
        help="LSA SAF password. Prefer environment variable LSASAF_PASSWORD for shared machines.",
    )
    parser.add_argument(
        "--force-parallel",
        action="store_true",
        help="Allow parallel downloads even in notebook-like environments.",
    )

    args = parser.parse_args(argv)
    config_path = Path(args.config)
    if not config_path.exists():
        parser.error(f"Configuration file not found: {config_path}")

    config = load_config(config_path)
    config.setdefault("global", {})

    if args.log_file:
        config["global"]["log_file"] = args.log_file
    if args.verbose:
        config["global"]["verbose"] = True

    climate_cfg = config.get("climate")
    if climate_cfg:
        if args.max_workers is not None:
            climate_cfg["max_workers"] = args.max_workers
        if args.force_parallel:
            climate_cfg["force_parallel"] = True
        if args.lsasaf_username:
            climate_cfg["username"] = args.lsasaf_username
        if args.lsasaf_password:
            climate_cfg["password"] = args.lsasaf_password

    try:
        run_pipeline(config)
    except Exception as exc:
        print(f"[ERROR] agrometflow failed: {exc}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
