from pathlib import Path
from agrometflow.utils import get_logger
from agrometflow.config_loader import load_config

class BaseProjectionDownloader:
    def __init__(self, scenario="ssp585", model=None, period="2041-2060", output_dir="data/projections",log_file=None, verbose=False):
        self.scenario = scenario
        self.model = model
        self.period = period
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.logger = get_logger("agrometflow.projections", log_file, verbose)

    def download(self, variables, bbox=None, resolution=None):
        raise NotImplementedError

    def postprocess(self):
        raise NotImplementedError