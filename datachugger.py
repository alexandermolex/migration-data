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

def load_file_path():
    dir_path = Path(input("Please input file path: "))
    if dir_path.exists() and dir_path.is_dir():
        files = dir_path.glob('*.csv')
        for file in files:
            print(file)
            tm.sleep(.2)
    else:
        print(f"Directory {dir_path} does not exist")

inflow = pd.read_csv("migration.data/post.processed.census.files/inflow/inflow_05_09_Alabama_processed.csv")
print(inflow.head())