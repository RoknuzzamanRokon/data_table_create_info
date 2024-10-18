from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
import panel as pn
import pandas as pd
import hvplot.pandas

# Load environment variables
load_dotenv()

# Database connection information from environment variables
db_host = os.getenv('DB_HOST')
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')
db_name = os.getenv('DB_NAME')

# Table names
table_1 = "vervotech_hotel_map_new"

# Create the connection URL
connection_url = f"mysql+pymysql://{db_user}:{db_password}@{db_host}/{db_name}"
engine = create_engine(connection_url)

# Query the database
query = f"SELECT * FROM {table_1}"
df = pd.read_sql(query, engine)

# Initialize the Panel extension
pn.extension(sizing_mode="stretch_both")  # Enable full screen sizing

# Create the dashboard layout with a heading and padding
dashboard = pn.Column(
    pn.pane.Markdown("## Table all information", align="center", margin=(0, 0, 30, 0)),  # Add heading with 30px bottom margin
    pn.Spacer(height=30),  # Spacer for 30px padding between heading and table
    pn.Row(
        df.hvplot.table(width=800, height=600, border=0)  # Create an hvplot table without default border
    ),
    sizing_mode="stretch_both" 
)

# Custom CSS for background color (watermark effect) and table border
css = """
.bk.pn-Column {
    background-color: rgba(0, 128, 255, 0.1);  /* Light blue background with transparency */
    padding: 20px;
}

.hvplot-table {
    border-collapse: collapse;  /* Ensure borders are collapsed */
    border: 2px solid black;  /* Add border to the entire table */
}

.hvplot-table th, .hvplot-table td {
    border: 1px solid black;  /* Add border to the header and data cells */
    padding: 10px;  /* Optional: Add some padding for better appearance */
    text-align: center;  /* Optional: Center-align text in the table cells */
}
"""

# Apply custom CSS
pn.config.raw_css.append(css)

# Serve the dashboard
dashboard.show()  # This will open it in the default browser
