from setuptools import find_packages, setup

setup(
    name="CountMoney_orchestration",
    packages=find_packages(exclude=["CountMoney_orchestration_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "pandas",
        "sqlalchemy",
        "tushare",
    ],
    extras_require={"dev": ["dagit", "pytest"]},
)
