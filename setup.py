from setuptools import find_packages, setup

setup(
    name="CountMoney_orchestration",
    packages=find_packages(exclude=["CountMoney_orchestration_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "pandas",
        "sqlalchemy==1.4.32",
        "tushare",
        "psycopg2",
    ],
    extras_require={"dev": ["dagit", "pytest"]},
)
