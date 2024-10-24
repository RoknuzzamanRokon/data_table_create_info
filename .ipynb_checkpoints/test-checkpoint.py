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


def total_data_count(table, engine):
    query = f"SELECT COUNT(*) FROM {table}"
    df = pd.read_sql(query, engine)
    total_data = df.iloc[0, 0]
    return total_data


def new_total_data_count_using_create_at(table, engine):
    query = f"SELECT COUNT(*) FROM {table} WHERE created_at = (SELECT MAX(created_at) FROM {table});"
    df = pd.read_sql(query, engine)
    total_data = df.iloc[0, 0]
    return total_data

def new_total_data_count_vervotech_mapping_using_last_update(table, engine):
    query = f"SELECT COUNT(*) FROM {table} WHERE last_update = (SELECT MAX(last_update) FROM {table});"
    df = pd.read_sql(query, engine)
    total_data = df.iloc[0, 0]
    return total_data


def data_insert_infoTable(data_dict, engine):
    """
    Insert data into the table `vervotech_update_data_info` with multiple columns in one row.
    
    Args:
    - data_dict (dict): Dictionary where keys are column names and values are the corresponding data to insert.
    """
    try:
        table = "vervotech_update_data_info"
        
        # Prepare columns and values for the query
        columns = ', '.join(data_dict.keys())
        values = ', '.join([f":{key}" for key in data_dict.keys()])
        
        insert_query = text(f"INSERT INTO {table} ({columns}) VALUES ({values})")
        
        # Convert numpy int64 to native Python int if necessary
        for key, value in data_dict.items():
            if isinstance(value, (np.integer, np.int64)):
                data_dict[key] = int(value)

        with engine.begin() as conn:
            conn.execute(insert_query, data_dict)
            print("Insert successful")
            
    except Exception as e:
        print(f"An error occurred: {e}")


# Gathering data and inserting it into a single row

data = {}

# New mapping data information section -------------------------------------------- (vervotech_hotel_map_new Table)
data['vh_new_total'] = total_data_count(table="vervotech_hotel_map_new", engine=engine)
data['vh_new_newFile'] = new_total_data_count_using_create_at(table="vervotech_hotel_map_new", engine=engine)

# Update mapping data information section ---------------------------------------- (vervotech_hotel_map_update Table)
data['vh_update_total'] = total_data_count(table="vervotech_hotel_map_update", engine=engine)
data['vh_update_newFile'] = new_total_data_count_using_create_at(table="vervotech_hotel_map_update", engine=engine)

# vervotech mapping table information section ------------------------------------- (vervotech_mapping Table)
data['vh_mapping_total'] = total_data_count(table="vervotech_mapping", engine=engine)
data['vh_mapping_newFile'] = new_total_data_count_vervotech_mapping_using_last_update(table="vervotech_mapping", engine=engine)

# Insert all the data into a single row in the table
data_insert_infoTable(data, engine)
