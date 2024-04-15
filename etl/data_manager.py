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
        self.db_name = os.getenv("db_name")
        self.table_definitions ={
            "batch_detail": 
                    """ 
                    CREATE TABLE IF NOT EXISTS batch_detail (
                    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                    timestamp datetime NOT NULL,
                    type_of_building VARCHAR(255) NOT NULL,
                    type_of_deal VARCHAR(255) NOT NULL,
                    size_of_batch INTEGER NOT NULL
                    ); 
                    """,
                    
            "estate_detail": 
                    """ 
                    CREATE TABLE IF NOT EXISTS estate_detail (
                    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                    code VARCHAR(255) NOT NULL,
                    name VARCHAR(255) NOT NULL,
                    category_main_cb INTEGER NOT NULL,
                    category_type_cb INTEGER NOT NULL,
                    category_sub_cb INTEGER NOT NULL,
                    rooms VARCHAR(255) NOT NULL,
                    size_meters FLOAT NOT NULL,
                    locality VARCHAR(255) NOT NULL,
                    region VARCHAR(255) NOT NULL,
                    district VARCHAR(255) NOT NULL,
                    latitude FLOAT NOT NULL,
                    longitude FLOAT NOT NULL,
                    first_date datetime NOT NULL,
                    last_date datetime NOT NULL,
                    first_price INTEGER NOT NULL,
                    last_price INTEGER NOT NULL,
                    max_price INTEGER NOT NULL,
                    min_price INTEGER NOT NULL
                    ); 
                    """,
            "scraped_prices": 
                    """ 
                    CREATE TABLE IF NOT EXISTS scraped_prices (
                    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                    estate_id INTEGER NOT NULL,
                    FOREIGN KEY (estate_id) REFERENCES estate_detail (id),
                    batch_id INTEGER NOT NULL,
                    FOREIGN KEY (batch_id) REFERENCES batch_detail (id),
                    price INTEGER NOT NULL
                    ); 
                    """                
        }
        
    def _get_connection(self):
        
        try:
            conn = sqlite3.connect(f"{self.db_path}/{self.db_name}")
            return conn
        except sqlite3.Error as e:
            print("Error connecting to database:", e)
            return None

    def load_all_rows(self, table_name):
        
        conn = self._get_connection()        
        try:
            query = f"SELECT * FROM {table_name}"
            df = pd.read_sql_query(query, conn)
            conn.close()
            return df
        
        except sqlite3.Error as e:
            print("Error performing operations:", e)
        
    def clear_table(self, table_name):
        
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(f"DELETE FROM {table_name}")
            conn.commit()
            cursor.close()
        except sqlite3.Error as e:
            print("Error deleting rows:", e)
            
    def drop_table(self, table_name):
        
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(f"DROP table {table_name}")
            conn.commit()
            cursor.close()
        except sqlite3.Error as e:
            print("Error dropping table:", e)
            
            
    def create_table(self, table_name):
        
        conn = self._get_connection()
        
        table = self.table_definitions[table_name]
        try:
            cursor = conn.cursor()
            cursor.execute(table)
            conn.commit()
            cursor.close()
        except sqlite3.Error as e:
            print("Error Creating table:", e)
            
    def filter_new_estates(self, df_new):
        
        df_all = self.load_all_rows("estate_detail")
        all_estates = set(df_all["code"])
        
        return df_new[~df_new["code"].isin(all_estates)]
        
    
    def insert_new_estate(self, offer_df):
        
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            
            data_to_upload = []
            for i in range(0, len(offer_df)):
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

            query = f"""
                    INSERT INTO estate_detail (
                    code, name, category_main_cb, category_type_cb, category_sub_cb, 
                    rooms, size_meters, locality, region, district, latitude, longitude,
                    first_date, last_date, first_price, last_price, max_price, min_price ) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                            ?, ?, ?, ?, ?, ?, ?, ?)
                    """
            cursor.executemany(query, data_to_upload)
            conn.commit()

        except sqlite3.Error as e:
            print("Error inserting offer:", e)
            conn.rollback()
            
        conn.close()
        
        
    def insert_new_batch(self, data_to_upload):
        
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            data_to_upload = [data_to_upload]
            query = f"""
                    INSERT INTO batch_detail (
                    timestamp, type_of_building, type_of_deal, size_of_batch)
                    VALUES (?, ?, ?, ?)
                    """
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
    