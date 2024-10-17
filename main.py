from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

load_dotenv()

db_host = os.getenv('DB_hosty')

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

try: 
    with engine.connect() as connection:
        result = connection.execute(text(f"SHOW TABLES"))
        for row in result:
            print(row)
except Exception as e:
    print(f"Error: {e}")
