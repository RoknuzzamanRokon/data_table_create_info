from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
import pandas as pd
import matplotlib.pyplot as plt
from pandas.plotting import table

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

# Check if DataFrame is empty
if df.empty:
    print("No data returned from the query.")
else:
    print("Data fetched successfully:")
    print(df.head())  # Print the first few rows of the DataFrame

    # Create a Matplotlib figure and axis
    fig, ax = plt.subplots(figsize=(12, 8))  # Adjust size as needed
    ax.axis('off')  # Turn off the axis

    # Add table to the plot
    table(ax, df, loc='center', cellLoc='center', colWidths=[0.1] * len(df.columns))

    # Set the title of the table
    plt.title("Table all information", fontsize=16, fontweight='bold', loc='center', pad=20)

    # Display the plot
    plt.show()
