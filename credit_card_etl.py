
#Created by Astrid Rodriguez
#Creation date: Feb 12th 2022

import sqlite3
import pandas as pd
from sqlalchemy import create_engine



#Extracting the data
#def extract(path):
   #df = pd.read_excel( r'hosts/host_app/app_data.xlsx')
#   df = pd.read_excel(path)
#   return df

def extract_validate_load(path) -> bool:
    #-----Extract----------#
    df = pd.read_excel(path)

    #-----Validations------#
    #Check if dataframe is empty
    if df.empty:
        print("0 rows read. Finishng execution")
        return False
    print(df)

    #Primary Key validation
    if pd.Series(df['customer_id']).is_unique:
        pass
    else:
        raise Exception("Duplicate Primary Key found")

    # Nulls validation
    if df.isnull().values.any():
        raise Exception("Null values found")

    #----Load to database ----#
    #engine= sqlalchemy.create_engine(DATABASE_LOCATION)
    engine = create_engine('sqlite:///credit_card.sqlite', echo = True)
    conn = sqlite3.connect('credit_card.sqlite')
    cursor =  conn.cursor()

    sql_query = """
        CREATE TABLE IF NOT EXISTS app_data(
            customer_id INT(20) ,
            customer_name VARCHAR(200),
            click_events INT(15),
            date DATE,
            CONSTRAINT primary_key_contraint PRIMARY KEY (customer_id)
        )
    """

    cursor.execute(sql_query)
    print("Opened database successfully")

    try:
        df.to_sql("app_data", engine, index=False, if_exists='append')
    except:
        print("Data already exists in the database")

    conn.close()
    print("Close databse successfully")


extract_validate_load('hosts/host_app/app_data.xlsx')