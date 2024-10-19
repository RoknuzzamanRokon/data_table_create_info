from flask import Flask, render_template
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)

db_host = os.getenv('DB_HOST')
db_user = os.getenv('DB_USER')
db_pass = os.getenv('DB_PASSWORD')
db_name = os.getenv('DB_NAME')

DATABASE_URL = f"mysql+pymysql://{db_user}:{db_pass}@{db_host}/{db_name}"
engine = create_engine(DATABASE_URL)

@app.route('/')
def index():
    latest_record = fetch_latest_record(engine)
    live_data = live_data_uploading_function('vervotech_mapping', engine)

    record_html = generate_html_content(latest_record)
    if live_data is not None and isinstance(live_data, dict):
        record_html += f'<p>Count of Done Records: {live_data["count"]}</p>'
    else:
        record_html += '<p>No count data found.</p>'

    return render_template('index.html', record_html=record_html)

def fetch_latest_record(engine):
    try:
        query = text("SELECT * FROM vervotech_update_data_info ORDER BY created_at DESC LIMIT 1;")
        
        with engine.connect() as connection:
            result = connection.execute(query)
            latest_record = result.fetchone()  # Fetch the first (and only) result
            
            print("Latest Record Fetched:", latest_record)  # Debugging output
            
            if latest_record is not None:
                record_dict = {
                    'Id': latest_record[0],
                    'vh_new_total': latest_record[1],
                    'vh_new_newFile': latest_record[2],
                    'vh_new_newFile_updateSuccess': latest_record[3],
                    'vh_new_newFile_updateSkipping': latest_record[4],
                    'vh_new_newFile_lastUpdate_dateTime': latest_record[5],
                    'vh_update_total': latest_record[6],
                    'vh_update_newFile': latest_record[7],
                    'vh_update_newFile_updateSuccess': latest_record[8],
                    'vh_update_newFile_updateSkipping': latest_record[9],
                    'vh_update_newFile_lastUpdate_dateTime': latest_record[10],
                    'vh_mapping_total': latest_record[11],
                    'vh_mapping_newFile': latest_record[12],
                    'created_at': latest_record[13],
                    'ModifiedOn': latest_record[14],
                    'contentUpdatingStatus': latest_record[15]
                }
                return record_dict
            else:
                print("No records found.")
                return None
    except Exception as e:
        print(f"An error occurred while fetching latest record: {e}")
        return None

def live_data_uploading_function(table, engine):
    try:
        query = text(f"SELECT COUNT(*) FROM {table} WHERE content_update_status = 'Done';")
        with engine.connect() as connection:
            result = connection.execute(query)
            latest_record = result.fetchone()

            if latest_record:
                return {'count': latest_record[0]}  # Use a dictionary with a key
            else:
                return None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

def generate_html_content(record):
    if record:
        record_html = '<div class="record"><h2>Record Details:</h2><ul>'
        for key, value in record.items():
            record_html += f'<li><strong>{key}:</strong> {value}</li>'
        record_html += '</ul></div>'
    else:
        record_html = '<p>No records found.</p>'
    
    return record_html

if __name__ == "__main__":
    app.run(port=2424)
