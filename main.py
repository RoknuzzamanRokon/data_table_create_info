from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
import pandas as pd

load_dotenv()

db_host = os.getenv('DB_HOST')
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')
db_name = os.getenv('DB_NAME')

table_1 = "vervotech_hotel_map_new"

table_1 = "vervotech_hotel_map_new"
table_2 = "vervotech_hotel_map_update"
table_3 = "vervotech_mapping"
table_4 = "vervotech_update_data_info"

connection_url = f"mysql+pymysql://{db_user}:{db_password}@{db_host}/{db_name}"
engine = create_engine(connection_url)


def total_data_count(table_name, engine):
    query = f"SELECT COUNT(*) FROM {table_name}"
    df = pd.read_sql(query, engine)
    total_data = df.iloc[0, 0]
    return total_data


def insert_data_info_table(column, table, engine):
    try:
        total_data_count_fetch_with_function = total_data_count(table, engine)

        # column = "vh_new_total"
        print(f"{table} this table has {total_data_count_fetch_with_function} data")

        insert_value_query = text(f"INSERT INTO {table_4} ({column}) VALUES (:total_data_count_fetch_with_function)")

        with engine.begin() as conn: 
            conn.execute(insert_value_query, {'total_data_count_fetch_with_function': total_data_count_fetch_with_function})
            print("Insert successful")

    except Exception as e:
        print(f"An error occurred: {e}")


column_1 = "vh_new_total"
insert_data_info_table(column=column_1, table=table_1, engine=engine)


column_2 = "vh_update_total"
insert_data_info_table(column=column_2, table=table_2, engine=engine)


column_3 = "vh_mapping_total"
insert_data_info_table(column=column_3, table=table_3, engine=engine)