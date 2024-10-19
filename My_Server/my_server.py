import http.server 
import socketserver
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from pandas import pd

load_dotenv()

db_host = os.getenv('DB_HOST')
db_user = os.getenv('DB_USER')
db_pass = os.getenv('DB_PASSWORD')
db_name = os.getenv('DB_NAME')

DATABASE_URL = f"mysql+pymysql://{db_user}:{db_pass}@{db_host}/{db_name}"
engine = create_engine(DATABASE_URL)

PORT = 2424

def fetch_latest_record(engine):
    try:
        query = text("SELECT * FROM vervotech_update_data_info ORDER BY created_at DESC LIMIT 1;")

        with engine.connect() as connection:
            result = connection.execute(query)
            latest_record = result.fetchone()

            if latest_record:
                return dict(latest_record.items())
            else:
                return None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None
    


def live_data_uploading_function(table, engine):
    try:
        query = f"SELECT COUNT(*) FROM {table} WHERE content_update_status = 'Done';"
        with engine.connect() as connection:
            result = connection.execute(query)
            latest_record = result.fetchone() 

            if latest_record:
                return dict(latest_record.items())
            else:
                return None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None
    

def generate_html_content(record):
    if record:
        record_html = '<dev class="record"><h2>Record Details:</h2><ul>'
        for key, value in record.items():
            record_html += f'<li><strong>{key}:</strong> {value}</li>'
        record_html += '</ul></div>'
        
    else:
        record_html = '<p>No record found. </p>'
    return record_html
    
