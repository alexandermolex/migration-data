import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

# Load the DATABASE_URL from your .env file
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

# Read the slim CSV we exported earlier
print("Reading CSV...")
df = pd.read_csv("inflow_2016_2020.csv")
print(f"Loaded {len(df)} rows")

# Connect to Supabase PostgreSQL
print("Connecting to database...")
engine = create_engine(DATABASE_URL) # type: ignore

# Load the data into a table called 'inflow'
# if_exists='replace' means it will drop and recreate the table each time
print("Loading data into Supabase...")
df.to_sql("inflow", engine, if_exists="replace", index=False)
print("Done!")