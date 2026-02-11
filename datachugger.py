import tkinter as tk
from tkinter import filedialog
import pandas as pd
import numpy as np
import os

def select_file():
    filepath = filedialog.askopenfilename(filetypes=[("CSV files", "*.csv"), ("Excel files", "*.xlsx;*.xls")])
    if filepath.split('.')[-1] in ['xlsx', 'xls']:
        df = pd.read_excel(filepath)
        # You can display the DataFrame here, or do something else with it
        print(df.head()) # Print the first few rows of the DataFrame
    else:
        df = pd.read_csv(filepath)
        # You can display the DataFrame here, or do something else with it
        print(df.head()) # Print the first few rows of the DataFrame


inflow_05_09 = select_file()
outflow_05_09 = select_file()
