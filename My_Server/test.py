import http.server
import socketserver
import os
from sqlalchemy import create_engine, text

# Database connection settings
DATABASE_URI = "mysql+pymysql://username:password@localhost/yourdatabase"
engine = create_engine(DATABASE_URI)

PORT = 8080  # The port where the server will run

# Define a function to fetch the latest record from the database
def fetch_latest_record(engine):
    """
    Fetch the most recent record from the 'vervotech_update_data_info' table based on the 'created_at' column.
    
    Args:
    - engine: SQLAlchemy engine object to connect to the database.
    
    Returns:
    - A dictionary containing the latest row's data.
    """
    try:
        query = text("SELECT * FROM vervotech_update_data_info ORDER BY created_at DESC LIMIT 1;")
        
        with engine.connect() as connection:
            result = connection.execute(query)
            latest_record = result.fetchone()  # Fetch the first (and only) result

            if latest_record:
                # Convert the result to a dictionary
                return dict(latest_record.items())
            else:
                return None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

# Define the HTML content with placeholders for the dynamic content
HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Latest Record</title>
    <link rel="stylesheet" href="/style.css">
</head>
<body>
    <div class="container">
        <h1>Latest Record from Database</h1>
        {record_html}
    </div>
</body>
</html>
"""

# Function to generate the HTML content with the latest record
def generate_html_content(record):
    if record:
        record_html = '<div class="record"><h2>Record Details:</h2><ul>'
        for key, value in record.items():
            record_html += f'<li><strong>{key}:</strong> {value}</li>'
        record_html += '</ul></div>'
    else:
        record_html = '<p>No records found.</p>'
    
    return HTML_TEMPLATE.format(record_html=record_html)

# Create a request handler to serve the HTML page
class MyHttpRequestHandler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/':
            # Fetch the latest record from the database
            latest_record = fetch_latest_record(engine)

            # Generate the HTML content with the latest record
            html_content = generate_html_content(latest_record)

            # Send the response
            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.end_headers()

            # Write the HTML content to the response
            self.wfile.write(bytes(html_content, "utf8"))
        else:
            # Serve static files like CSS
            super().do_GET()

# Create the server
with socketserver.TCPServer(("", PORT), MyHttpRequestHandler) as httpd:
    print(f"Serving on port {PORT}")
    httpd.serve_forever()






































import http.server
import socketserver
import os
from sqlalchemy import create_engine, text # type: ignore

# Database connection settings
DATABASE_URI = "mysql+pymysql://username:password@localhost/yourdatabase"
engine = create_engine(DATABASE_URI)

PORT = 8080  # The port where the server will run

# Define a function to fetch the latest record from the database
def fetch_latest_record(engine):
    """
    Fetch the most recent record from the 'vervotech_update_data_info' table based on the 'created_at' column.
    
    Args:
    - engine: SQLAlchemy engine object to connect to the database.
    
    Returns:
    - A dictionary containing the latest row's data.
    """
    try:
        query = text("SELECT * FROM vervotech_update_data_info ORDER BY created_at DESC LIMIT 1;")
        
        with engine.connect() as connection:
            result = connection.execute(query)
            latest_record = result.fetchone()  # Fetch the first (and only) result

            if latest_record:
                # Convert the result to a dictionary
                return dict(latest_record.items())
            else:
                return None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

# Function to generate the HTML content with the latest record
def generate_html_content(record):
    if record:
        record_html = '<div class="record"><h2>Record Details:</h2><ul>'
        for key, value in record.items():
            record_html += f'<li><strong>{key}:</strong> {value}</li>'
        record_html += '</ul></div>'
    else:
        record_html = '<p>No records found.</p>'
    
    return record_html

# Create a request handler to serve the HTML page
class MyHttpRequestHandler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/':
            # Fetch the latest record from the database
            latest_record = fetch_latest_record(engine)

            # Load the index.html file
            try:
                with open('index.html', 'r') as file:
                    html_content = file.read()

                # Insert the dynamic content into the HTML
                record_html = generate_html_content(latest_record)
                final_html = html_content.replace('{record_html}', record_html)

                # Send the response
                self.send_response(200)
                self.send_header('Content-type', 'text/html')
                self.end_headers()

                # Write the final HTML content to the response
                self.wfile.write(bytes(final_html, "utf8"))
            except FileNotFoundError:
                self.send_response(404)
                self.end_headers()
                self.wfile.write(b"404 Not Found")
        else:
            # Serve static files like CSS
            super().do_GET()

# Create the server
with socketserver.TCPServer(("", PORT), MyHttpRequestHandler) as httpd:
    print(f"Serving on port {PORT}")
    httpd.serve_forever()
