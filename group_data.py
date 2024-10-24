from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import numpy as np
import os
import pandas as pd

load_dotenv()

db_host = os.getenv('DB_HOST')
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')
db_name = os.getenv('DB_NAME')


table_1 = "vervotech_hotel_map_new"
table_2 = "vervotech_hotel_map_update"
table_3 = "vervotech_mapping"
table_4 = "vervotech_update_data_info"

connection_url = f"mysql+pymysql://{db_user}:{db_password}@{db_host}/{db_name}"
engine = create_engine(connection_url)



def new_group_data(table, engine):
    query = f"""
    SELECT ProviderFamily, COUNT(*) AS value_count
    FROM {table}
    WHERE DATE(created_at) = (
        SELECT DATE(MAX(created_at)) 
        FROM {table}
    )
    GROUP BY ProviderFamily;
    """
    df = pd.read_sql(query, engine)
    return df  # Returning the dataframe with grouped data


name = new_group_data(table='vervotech_hotel_map_new', engine=engine)

print(name)
