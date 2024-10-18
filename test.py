from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
import pandas as pd

# Load environment variables
load_dotenv()

# Database credentials
db_host = os.getenv('DB_HOST')
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')
db_name = os.getenv('DB_NAME')

table_1 = "vervotech_hotel_map_new"
table_4 = "vervotech_update_data_info"

# Create database connection
connection_url = f"mysql+pymysql://{db_user}:{db_password}@{db_host}/{db_name}"
engine = create_engine(connection_url)

try:
    # Fetch the count of rows from table_1
    query = f"SELECT COUNT(*) FROM {table_1};"
    df = pd.read_sql(query, engine)
    total_data = df.iloc[0, 0]
    print(f"{table_1} this table has {total_data} data")

    # Prepare the insert query with parameter binding
    insert_value_query = text(f"INSERT INTO {table_4} (vh_new_total) VALUES (:total_data)")

    # Explicitly handle the transaction and commit the changes
    with engine.begin() as conn:  # This automatically handles commit/rollback
        conn.execute(insert_value_query, {'total_data': total_data})
        print("Insert successful")

except Exception as e:
    # Print the error for debugging purposes
    print(f"An error occurred: {e}")
