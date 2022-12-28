from dagster import load_assets_from_package_module, repository

from CountMoney_orchestration import assets
from CountMoney_orchestration.jobs.load_history_data import load_history


@repository
def CountMoney_orchestration():
    definitions = [load_history]
    return definitions
