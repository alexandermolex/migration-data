import pandas as pd
import pathlib
from pathlib import Path

def batch_stack(state):
    data = []
    dir_path = Path("migration.data/post.processed.census.files/inflow")
    
    if dir_path.exists() and dir_path.is_dir():
        files = dir_path.glob(f'inflow_*_*_{state}_processed.csv')
        for file in files:
            df = pd.read_csv(file)
            # Remove footnote rows (they have text footnotes and null values in data columns)
            # Keep only rows that have valid origin_fips_state (which should be numeric)
            df = df[pd.to_numeric(df['origin_fips_state'], errors='coerce').notna()]
            data.append(df)
    return pd.concat(data, ignore_index=True)
 
def state_list(): 
    states = []
    dir_path = Path("migration.data/post.processed.census.files/inflow")   
    if dir_path.exists() and dir_path.is_dir():
        files = dir_path.glob(f'inflow_05_09_*_processed.csv')
        for file in files:
            states.append(str(file).split("_")[-2])
    return states
states = state_list()
print(states)