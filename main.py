from sqlalchemy import create_engine, text
from 



table_1 = "vervotech_hotel_map_new"
table_2 = "vervotech_hotel_map_update"
table_3 = "vervotech_mapping"
table_4 = "vervotech_update_data_info"



connection_url = f"mysql+pymysql://{db_user}:{db_password}@{db_password}/{db_name}"
engine = create_engine(connection_url)

with engine.connect() as connection:
    result = connection.execute(text(f"SHOW TABLES"))
    for row in result:
        print(row)

