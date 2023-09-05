#!/usr/bin/env python
# coding: utf-8

# In[ ]:


#!/usr/bin/env python
# coding: utf-8
# author: ranjeetha.r.virdi@gmail.com
# In[2]:


from urllib.request import urlretrieve
from pathlib import Path
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import schedule
import time
import datetime


def job():
        """ 
        Job:
            The script that retrieves data from API for the "BBK01.SUD231"
            It then write the data into PostgresSQL,locally hosted
        
        """
        
        
        # Path to save data
        path_data = Path('data')
        path_data.mkdir(exist_ok = True)
        path_csv = path_data / 'data.csv'

        # Retrieve the data
        URL = 'https://api.statistiken.bundesbank.de/rest/download/BBK01/SUD231?format=csv&lang=de'
        urlretrieve(URL, path_csv)

        df = pd.read_csv(
                filepath_or_buffer = path_csv,
                sep = ';'
        )

        #drop last column 'BBK01.SUD231_FLAGS'
        df.drop(
                ['BBK01.SUD231_FLAGS'], 
                axis = 1, 
                inplace = True
        )

        #drop first 7 rows of data discription
        df.drop(
                index = df.index[:7], 
                axis = 0, 
                inplace = True
        )

        # Rename columns
        df = df.rename(
                columns = {'Unnamed: 0': 'date', 'BBK01.SUD231': 'volume'}) 

        # Reset Index
        df = df.reset_index(drop = True)
        df.drop(
            index = df.index[-1],
            axis = 0,
            inplace = True
        )
        
        df["volume"] = pd.to_numeric(df["volume"])

        #convert date column into year and month integer format
        new_df = df['date'].str.split('-',expand = True)
        
        
        new_df = new_df.rename(
                        columns = {0: 'closed_year', 1: 'closed_month'}
        ) 
        
        new_df["closed_year"] = pd.to_numeric(new_df["closed_year"])
        new_df["closed_month"] = pd.to_numeric(new_df["closed_month"])

        #merging the two data frames with the correct dataformat

        df = pd.merge(
                new_df, 
                df, 
                left_index = True, 
                right_index = True
                ).drop('date', axis = 1)
        
        df.head()


        """
        creating a connection to postgresql server using psycopg2 and sqlalchemy and storing data on it
        variable:
                conn_string: create a string with parameters required to authenticate and connect to
                PostgresSql server
      
        """

        conn_string = 'postgresql://postgres:preet@localhost/postgres'

        db = create_engine(conn_string)
        conn = db.connect()

        df.to_sql(
            'data', 
             con = conn, 
             if_exists = 'replace')
        
        conn = psycopg2.connect(conn_string)
        conn.autocommit = True
        cursor = conn.cursor()
        sql1 = '''select * from data;'''
        cursor.execute(sql1)
        
        for i in cursor.fetchall():
            print(i)

        # conn.commit()
        conn.close()
        
        # print the current timestamp
        ts = datetime.datetime.now()
        print("I am executing every 1 minute at: ",ts)



schedule.every(1).minutes.until("16:00").do(job)

"""
Schedule will refresh the required data in the database with the new updated data by pulling 
the data from the API.We can schedule a job at time as per requirement. 
Here for demo purpose its set to 5 seconds.
        
Returns: Pulls data from API and pushes the data into posgressql after specific amount of time.
syntax:
#schedule.every(5).minutes.until("08:00").do(collect_data_from_api)
#schedule.every().friday.at(time_str).do(collect_data_from_api)
"""       
while True:
    schedule.run_pending()
    time.sleep(1)





# In[ ]:




