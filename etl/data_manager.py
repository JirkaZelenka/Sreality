from datetime import datetime
import pandas as pd
import re

import sqlite3
import os
from dotenv import load_dotenv


class DataManager:
    
    def __init__(self) -> None: 
        
        load_dotenv()
        self.db_path = os.getenv("db_path") 

    def get_connection(self, database_name):
        try:
            conn = sqlite3.connect(f"{self.db_path}/{database_name}")
            return conn
        except sqlite3.Error as e:
            print("Error connecting to database:", e)
            return None

    def load_all_estates(self, conn):
        
        try:
            query = "SELECT * FROM estate_detail"
            df = pd.read_sql_query(query, conn)
            conn.close()
            return df
        
        except sqlite3.Error as e:
            print("Error performing operations:", e)
    
    def filter_new_estates(self, df_all, df_new):
        all_estates = set(df_all["code"])
        
        return df_new[~df_new["code"].isin(all_estates)]
        
    
    def insert_new_estate(self, conn, offer_df, table_name):
        try:
            cursor = conn.cursor()
            
            data_to_upload = []
            for i in range(10):
                r = offer_df.iloc[i]
                data_to_upload.append([
                    r['code'],
                    r['name'],
                    r['category_main_cb'], 
                    r['category_type_cb'], 
                    r['category_sub_cb'], 
                    r.loc['rooms'],
                    r.loc['size_meters'],
                    r.loc['locality'],
                    "region",
                    "district",
                    r.loc['latitude'],
                    r.loc['longitude'],
                    r.loc["timestamp"],
                    r.loc["timestamp"],  ### last data, again the same
                    r.loc['price'],
                    r.loc['price'],
                    r.loc['price'],
                    r.loc['price'],
                    ])

            query = f"""INSERT INTO {table_name} (
                code, name, category_main_cb, category_type_cb, category_sub_cb, 
                rooms, size_meters, locality, region, district, latitude, longitude,
                first_date, last_date, first_price, last_price, max_price, min_price ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?)"""

            cursor.executemany(query, data_to_upload)
            conn.commit()

        except sqlite3.Error as e:
            print("Error inserting offer:", e)
            conn.rollback()
            
        conn.close()
    
    def update_current_estates(self):
        pass
    
    def get_regions_and_districs(self):
        pass
    