import pandas as pd


def report_date_generate(years: list[str]) -> list[str]:
    result = []
    for i in range(len(years)):
        q1 = years[i] + '0331'
        q2 = years[i] + '0630'
        q3 = years[i] + '0930'
        q4 = years[i] + '1231'
        period = [q1, q2, q3, q4]
        result = result + period
    return result
