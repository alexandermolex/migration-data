import pandas as pd
import numpy as np
import os

inflow_05_09 = pd.read_excel("C:/Users/alexo/Documents/GitHub/migration-data/migration.data/county-to-county-current-residence-sort(inflow 05-09).xls")
outflow_05_09 = pd.read_excel("C:/Users/alexo/Documents/GitHub/migration-data/migration.data/county-to-county-previous-residence-sort(outflow 05-09).xls")

print(inflow_05_09.head(10))
print(outflow_05_09.head(10))