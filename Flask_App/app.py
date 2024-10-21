from flask import Flask, render_template
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

app = Flask(__name__)
load_dotenv()

# Database connection details
db_host = os.getenv('DB_HOST')
db_user = os.getenv('DB_USER')
db_pass = os.getenv('DB_PASSWORD')
db_name = os.getenv('DB_NAME')

DATABASE_URL = f"mysql+pymysql://{db_user}:{db_pass}@{db_host}/{db_name}"
engine = create_engine(DATABASE_URL)

@app.route('/')
def index():
    latest_record = fetch_latest_record(engine)
    latest_record['vh_new_newFile_updateSuccess'] = int(latest_record['vh_new_newFile_updateSuccess'])
    latest_record['vh_new_newFile_updateSkipping'] = int(latest_record['vh_new_newFile_updateSkipping'])
    latest_record['vh_update_newFile_updateSuccess'] = int(latest_record['vh_update_newFile_updateSuccess'])
    latest_record['vh_update_newFile_updateSkipping'] = int(latest_record['vh_update_newFile_updateSkipping'])
    latest_record['vh_mapping_newFile'] = int(latest_record['vh_mapping_newFile'])
    
    live_updates = live_data_uploading_function(table='vervotech_mapping', engine=engine) 
    return render_template('index.html', latest_record=latest_record, live_updates=live_updates)

@app.route('/live_updates')
def live_updates_route():
    live_updates = live_data_uploading_function(table='vervotech_mapping', engine=engine)
    return {
        'count': live_updates['count'],
        'last_update': live_updates['last_update']
    }

def fetch_latest_record(engine):
    try:
        query = text("SELECT * FROM vervotech_update_data_info ORDER BY created_at DESC LIMIT 1;")
        
        with engine.connect() as connection:
            result = connection.execute(query)
            latest_record = result.fetchone()  
            
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
                return None
    except Exception as e:
        print(f"An error occurred while fetching the latest record: {e}")
        return None

def live_data_uploading_function(table, engine):
    try:
        query = text(f"SELECT COUNT(*) as count, NOW() as last_update FROM {table} WHERE content_update_status = 'Done';")
        with engine.connect() as connection:
            result = connection.execute(query)
            latest_record = result.fetchone() 

            if latest_record:
                return {
                    'count': latest_record[0], 
                    'last_update': latest_record[1].strftime('%Y-%m-%d %H:%M:%S')  
                }
            else:
                return {'count': 0, 'last_update': None}
    except Exception as e:
        print(f"An error occurred: {e}")
        return None



if __name__ == '__main__':
    app.run(debug=True, port=2424)
