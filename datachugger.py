import tkinter as tk
from tkinter import filedialog
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import altair as alt 
import os

def select_file(prompt = "Select a file"):
    filepath = filedialog.askopenfilename(filetypes=[("CSV files", "*.csv"), ("Excel files", "*.xlsx;*.xls")], title = prompt )
    if filepath.split('.')[-1] in ['xlsx', 'xls']:
        df = pd.read_excel(filepath)
        # You can display the DataFrame here, or do something else with it
        print(df.head()) # Print the first few rows of the DataFrame
    else:
        df = pd.read_csv(filepath)
        # You can display the DataFrame here, or do something else with it
        print(df.head()) # Print the first few rows of the DataFrame

inflow_05_09 = select_file("Select the inflow data file for 05-09")
outflow_05_09 = select_file("Select the outflow data file for 05-09")
