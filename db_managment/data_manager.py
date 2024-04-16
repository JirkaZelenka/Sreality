from datetime import datetime
import pandas as pd
import re

import sqlite3
import os
from dotenv import load_dotenv


class DataManager:
    
    def __init__(self) -> None: 
        
        load_dotenv()
        self.project_path = os.getenv("project_path") 
        self.db_name = os.getenv("db_name")
        self.data_folder = os.getenv("data_folder")
        self.type_of_building = os.getenv("type_of_building")
        
        
        self.table_definitions ={
            "batch_detail": 
                    """ 
                    CREATE TABLE IF NOT EXISTS batch_detail (
                    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                    timestamp datetime NOT NULL,
                    created_at datetime NOT NULL
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
                    type_of_building VARCHAR(255) NOT NULL,
                    type_of_deal VARCHAR(255) NOT NULL,
                    type_of_rooms VARCHAR(255) NOT NULL,
                    size_meters FLOAT NOT NULL,
                    locality VARCHAR(255) NOT NULL,
                    region VARCHAR(255) NOT NULL,
                    district VARCHAR(255) NOT NULL,
                    latitude FLOAT NOT NULL,
                    longitude FLOAT NOT NULL,
                    first_update datetime NOT NULL,
                    last_update datetime NOT NULL,
                    first_price INTEGER NOT NULL,
                    last_price INTEGER NOT NULL,
                    max_price INTEGER NOT NULL,
                    min_price INTEGER NOT NULL,
                    created_at datetime NOT NULL
                    ); 
                    """,
            "scraped_prices": 
                    """ 
                    CREATE TABLE IF NOT EXISTS scraped_prices (
                    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                    estate_id INTEGER NOT NULL,
                    batch_id INTEGER NOT NULL,
                    price INTEGER NOT NULL,
                    created_at datetime NOT NULL,
                    FOREIGN KEY (estate_id) REFERENCES estate_detail (id),
                    FOREIGN KEY (batch_id) REFERENCES batch_detail (id)
                    ); 
                    """                
        }
        
    def _get_connection(self):
        
        try:
            conn = sqlite3.connect(f"{self.project_path}/{self.db_name}")
            return conn
        except sqlite3.Error as e:
            print("Error connecting to database:", e)
            return None

    def get_all_rows(self, table_name):
        
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
            
    def insert_new_estate(self, df):
        
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            
            data_to_upload = []
            for i in range(len(df)):
                r = df.iloc[i]
                data_to_upload.append([
                    r['code'],
                    r['name'],
                    r['category_main_cb'], 
                    r['category_type_cb'], 
                    r['category_sub_cb'], 
                    r['type_of_building'], 
                    r['type_of_deal'], 
                    r['type_of_rooms'], 
                    r['size_meters'],
                    r['locality'],
                    r['region'],
                    r['district'],
                    r['latitude'],
                    r['longitude'],
                    r["timestamp"],
                    r["timestamp"],  ### last date = equals first date for now
                    r['price'],      ### all prices are equal now.
                    r['price'],
                    r['price'],
                    r['price'],
                    r["created_at"]
                    ])

            query = f"""
                    INSERT INTO estate_detail (
                    code, name, 
                    category_main_cb, category_type_cb, category_sub_cb,
                    type_of_building, type_of_deal, type_of_rooms, 
                    size_meters, locality, region, district, latitude, longitude,
                    first_date, last_date, first_price, last_price, max_price, min_price, created_at ) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                            ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """             
            cursor.executemany(query, data_to_upload)
            conn.commit()

        except sqlite3.Error as e:
            print("Error inserting offer:", e)
            conn.rollback()
            
        conn.close()
        
    def insert_new_batch(self, df):
        
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
        
            data_to_upload = []
            for i in range(len(df)):
                r = df.iloc[i]
                data_to_upload.append([r['timestamp'], r["created_at"]])
                
            query = f"""
                    INSERT INTO batch_detail (timestamp, created_at)
                    VALUES (?, ?)
                    """
            cursor.executemany(query, data_to_upload)
            conn.commit()

        except sqlite3.Error as e:
            print("Error inserting offer:", e)
            conn.rollback()
            
        conn.close()
    
    def insert_new_price(self, df):
        
        conn = self._get_connection()
        try:
            #? only here foreing keys are applied and need to be actively enforced
            conn.execute("PRAGMA foreign_keys = ON")
            cursor = conn.cursor()
            
            data_to_upload = []
            for i in range(len(df)):
                r = df.iloc[i]
                data_to_upload.append([r['estate_id'],
                                       r["batch_id"],
                                       r["price"],
                                       r["created_at"]
                                       ])
                
            query = f"""
                    INSERT INTO scraped_prices (
                    estate_id, batch_id, price, created_at)
                    VALUES (?, ?, ?, ?)
                    """
            cursor.executemany(query, data_to_upload)
            conn.commit()

        except sqlite3.Error as e:
            print("Error inserting offer:", e)
            conn.rollback()
            
        conn.close()
   
    def filter_new_estates(self, df_new):
        
        df_all = self.get_all_rows("estate_detail")
        all_estates = set(df_all["code"])
        
        return df_new[~df_new["hash_id"].isin(all_estates)]
    
    def get_regions_and_districs(self):
        pass
     
    def translate_type_of_building(code):
        pass
    
    def translate_type_of_deal(code):
        pass
    
    def translate_type_of_rooms(code):
        pass
    
    def update_existing_estate(self):
        #TODO:
        pass
    

