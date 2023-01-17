from setuptools import find_packages, setup

setup(
    name="CountMoney_orchestration",
    packages=find_packages(exclude=["CountMoney_orchestration_tests"]),
    install_requires=[
        "dagster==1.1.7",
        "dagster-cloud",
        "dagster-dbt==0.17.7",
        "dbt-core==1.3.1",
        "dbt-postgres==1.3.1",
        "pandas==1.3.5",
        "sqlalchemy==1.4.32",
        "tushare",
        "psycopg2-binary==2.9.3",
        "pyairtable==1.4.0",
    ],
    extras_require={"dev": ["dagit", "pytest"]},
)
