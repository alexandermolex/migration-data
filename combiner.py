import pandas as pd
import pathlib
from pathlib import Path

def batch_stack(state):
    data = []
    #Change the path for inflow/outflow
    dir_path = Path("migration.data/post.processed.census.files/inflow")
    if dir_path.exists() and dir_path.is_dir():
        #Change the file name to reflect where it is being processed
        files = dir_path.glob(f'inflow_*_*_{state}_processed.csv')
        for file in files:
            df = pd.read_csv(file)
            data.append(df)
    return pd.concat(data, ignore_index=True)
 
def state_list(): 
    states = []
    #Change the path for inflow/outflow
    dir_path = Path("migration.data/post.processed.census.files/inflow")   
    if dir_path.exists() and dir_path.is_dir():
        #Change it here too for inflow/outflow
        files = dir_path.glob(f'inflow_05_09_*_processed.csv')
        for file in files:
            states.append(str(file).split("_")[-2])
    return states

states = state_list()
states.sort()

for item in states:
    df = batch_stack(item)
    #Change the path here as well for the control flow
    dir_path = Path("migration.data/post.processed.census.files/inflow")
    if str(dir_path).split("/")[-1] == "inflow":
        df.to_csv(f"migration.data/post.processed.census.files/concat/{item}_full_inflow.csv")
    else:
        df.to_csv(f"migration.data/post.processed.census.files/concat/{item}_full_outflow.csv")
    print(f"{item} has been concatenated and saved")