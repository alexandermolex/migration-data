import tkinter as tk
from tkinter import filedialog
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import altair as alt 
import os
import pathlib
from pathlib import Path
import time as tm

alt.theme.enable('dark')

def load_file_path():
    dir_path = Path(input("Please input file path: "))
    if dir_path.exists() and dir_path.is_dir():
        files = dir_path.glob('*.csv')
        for file in files:
            print(file)      
    else:
        print(f"Directory {dir_path} does not exist")

def batch_visuals():
    dir_path = Path("migration.data/post.processed.census.files/outflow")
    if dir_path.exists() and dir_path.is_dir():
        files = dir_path.glob('*.csv')
        for file in files:
            print(file)
            state_name = str(file).split('_')[-2]
            year = str(file).split('_')[-4] + '_' + str(file).split('_')[-3]
            df = pd.read_csv(file)
            inflow_count_pop = df[['origin_fips_state', 'origin_fips_county', 'origin_state_name', 'origin_county_name', 'county_of_current_residence_estimate',"origin_col_7_moe"]]
            uniq_inflow_county_pop = inflow_count_pop.drop_duplicates().reset_index(drop=True)
            uniq_inflow_county_pop_chart = alt.Chart(uniq_inflow_county_pop, title = f'Population by County Name in {state_name} from {year}').mark_bar().encode(
                alt.X('origin_county_name',axis=alt.Axis(labelAngle=45)).title("County Name").sort(field='county_of_current_residence_estimate', order = 'descending'),
                alt.Y('county_of_current_residence_estimate').title('County Resident Population Estimate')
            )
            chart_file_name = f'outflow_{year}_{state_name}_county_population_chart.html'
            uniq_inflow_county_pop_chart.save("visuals/" + chart_file_name)            
    else:
        print(f"Directory {dir_path} does not exist")

inflow = pd.read_csv("migration.data/post.processed.census.files/inflow/inflow_05_09_Michigan_processed.csv")
inflow_count_pop = inflow[['origin_fips_state', 'origin_fips_county', 'origin_state_name', 'origin_county_name', 'county_of_current_residence_estimate',"origin_col_7_moe"]]
uniq_inflow_county_pop = inflow_count_pop.drop_duplicates().reset_index(drop=True)
uniq_inflow_county_pop_chart = alt.Chart(uniq_inflow_county_pop, title = 'Population by County Name').mark_bar().encode(
    alt.X('origin_county_name',axis=alt.Axis(labelAngle=45)).title("County Name").sort(field='county_of_current_residence_estimate', order = 'descending'),
    alt.Y('county_of_current_residence_estimate').title('County Resident Population Estimate')
)
#uniq_inflow_county_pop_chart.save('uniq_inflow_county_pop_chart.html')
#load_file_path()
batch_visuals()

