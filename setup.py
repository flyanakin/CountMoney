from setuptools import find_packages, setup

setup(
    name="CountMoney_orchestration1",
    packages=find_packages(exclude=["CountMoney_orchestration_tests"]),
    install_requires=[
        "dagster",
    ],
    extras_require={"dev": ["dagit", "pytest"]},
)
