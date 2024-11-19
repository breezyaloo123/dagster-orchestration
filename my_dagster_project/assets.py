from dagster import asset
import pandas as pd
import pyodbc
from sqlalchemy import create_engine
from dagster import MaterializeResult, MetadataValue

@asset(group_name="etl_schedule")
def load_data() -> pd.DataFrame:
    """Load the raw data."""
    # Use the file ID from the Google Drive shareable link
    file_id = '1oFIXJItPRIALdqKKGu4Uqgc13e4fAzt3'
    url = f'https://drive.google.com/uc?export=download&id={file_id}'

    # Load the CSV file into a DataFrame
    dataframe = pd.read_csv(url)
    #Load the raw data
    #https://drive.google.com/file/d/1oFIXJItPRIALdqKKGu4Uqgc13e4fAzt3/view?usp=drive_link
    #C:/Users/Lenovo/OneDrive - wasoko.com/Documents/Dagster/my-dagster-project/my_dagster_project/supermarket_sales.csv
    #dataframe = pd.read_csv('https://docs.google.com/spreadsheets/d/1zvP0FIChyyxIqntQKHmdkvnCRYOZ7CSvkwhSoaLE5Pw/edit?usp=sharing')
    return dataframe



# %%
#Cleaning data
@asset(deps=["load_data"],group_name="etl_schedule")
def cleaning_data() -> pd.DataFrame:
    """Cleaning raw data."""
    #change the date column to YYYY-MM-DD format
    df=load_data()
    df['Date'] = pd.to_datetime(df['Date'], dayfirst=False).dt.strftime('%Y-%m-%d')
    #Change columns names
    df=df.rename(columns={'Invoice ID':'Invoice_ID','Customer type':'Customer_type','Product line':'Product_line','Unit price':'Unit_price','Tax 5%':'Tax_5pct','gross margin percentage':'Gross_marginpct',
    'gross income':'gross_income'})
    return df

# %%
#Send data to Snowflake
@asset(deps=["cleaning_data"],group_name="etl_schedule")
def send_data_SQLSERVER() -> MaterializeResult:
    """Storing the cleaned data to SQL SERVER."""
    # Define the connection string
    df=cleaning_data()
      
    #connection_string = "mssql+pyodbc://@DESKTOP-1JCIH4T\SQLEXPRESS/Supermarket_DW?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server"
    #connection_string = "mssql+pyodbc://DataAnalyst:DataAnalyst@DESKTOP-1JCIH4T\SQLEXPRESS/Supermarket_DW?driver=ODBC+Driver+17+for+SQL+Server"
    # Create the engine
    #engine = create_engine("mssql+pyodbc://DataAnalyst:DataAnalyst@DESKTOP-1JCIH4T\SQLEXPRESS/Supermarket_DW?driver=ODBC+Driver+17+for+SQL+Server")
       # SQL Server connection
    conn = pyodbc.connect(
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=DESKTOP-1JCIH4T\SQLEXPRESS;"
        "DATABASE=Supermarket_DW;"
        "UID=DataAnalyst;"
        "PWD=DataAnalyst"
    )
    cursor = conn.cursor()

    # Generate column names and placeholders dynamically
    columns = ", ".join(df.columns)
    placeholders = ", ".join(["?"] * len(df.columns))
    insert_query = f"INSERT INTO Sales_etl ({columns}) VALUES ({placeholders})"

    #Prepare data as a list of tuples for executemany
    data_to_insert = [tuple(row) for row in df.to_numpy()]
    #Convert DataFrame rows into list of tuples
    data_to_insert = [tuple(row) for row in df.itertuples(index=False, name=None)]

    # # Insert each row
    # for row_data in data_to_insert:
    #     cursor.execute(insert_query, row_data)
    # #Convert DataFrame rows into list of tuples
    # data_to_insert = [tuple(row) for row in df.itertuples(index=False, name=None)]

    # Debug: Print insert query and sample data
    print("Insert Query:", insert_query)
    print("Sample Data:", data_to_insert[0])

    # Insert rows using executemany
    cursor.executemany(insert_query, data_to_insert)
    # # Insert data into SQL Server table
    # #cursor.executemany(insert_query, data_to_insert)

    # Commit and close the connection
    conn.commit()
    cursor.close()
    conn.close()

    #return "Data saved successfully"
    # Send the data to SQL Server
    #df.to_sql('Sales_etl', con=engine, if_exists='replace', index=False)
    print("Data sent to SQL SERVER successfully!")
    return MaterializeResult(
        metadata={
            "num_records": len(data_to_insert),
            "preview": MetadataValue.md(str(df.to_markdown())),
        }
    )