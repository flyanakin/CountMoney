from dagster import load_assets_from_package_module, repository

from CountMoney_orchestration1 import assets


@repository
def CountMoney_orchestration():
    return [load_assets_from_package_module(assets)]
