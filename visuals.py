import sqlite3
import pandas as pd
import pathlib
from pathlib import Path
# Setup to connect to the Full Database of Clean Data
with sqlite3.connect('Full_Migration.db') as conn:
    cursor = conn.cursor()
    
    #This SQL command selects all the relevant data for states population data
    # To default for now it is set to michgian but I plan to expand it to a loop
    # That way I can run it to make visuals for every state

    cursor.execute(
        """select DISTINCT origin_state_name, origin_county_name, county_of_current_residence_estimate, origin_nonmovers_estimate, origin_movers_us_estimate, year
        from inflow 
        where origin_state_name = "Michigan"
        order by year,  origin_fips_county, dest_fips_state,dest_fips_county desc;
        """)
    pop_est = cursor.fetchall()
    

