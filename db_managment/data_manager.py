from datetime import datetime
import pandas as pd
from tqdm import tqdm
import re

import sqlite3
import os

from config import Config 
 
class DataManager:
    """
    All operations available on the daatabase tables.
    Inserting new estate, updating estate info in estate_detail table.
    Adding new prices to scraped_prices table.
    """
    
    def __init__(self) -> None: 
        self.cf = Config()      
        
    def _get_connection(self):
        
        try:
            conn = sqlite3.connect(f"{self.cf.project_path}/{self.cf.db_name}")
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
        
        table = self.cf.table_definitions[table_name]
        try:
            cursor = conn.cursor()
            cursor.execute(table)
            conn.commit()
            cursor.close()
        except sqlite3.Error as e:
            print("Error Creating table:", e)
                 
    def _insert_new_estate(self, df, timestamp):
        
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            
            data_to_upload = []
            for i in range(len(df)):
                r = df.iloc[i]
                data_to_upload.append([
                    str(r['code']),
                    r['name'],
                    str(r['category_main_cb']), 
                    str(r['category_type_cb']), 
                    str(r['category_sub_cb']), 
                    r['type_of_building'], 
                    r['type_of_deal'], 
                    # this is same or better compared to type_of_flat directly scraped
                    r['type_of_rooms'], 
                    r['size_meters'],
                    r['locality'],
                    "region",
                    "district",
                    #r['region'],
                    #r['district'],
                    r['latitude'],
                    r['longitude'],
                    r["timestamp"],  
                    r["timestamp"],  ### last date = equals first date for now ^^^
                    str(r['price']),      ### all prices are equal now. vvv
                    str(r['price']),
                    str(r['price']),
                    str(r['price']),
                    timestamp
                    ])

            query = f"""
                    INSERT INTO estate_detail (
                    code, name, 
                    category_main_cb, category_type_cb, category_sub_cb,
                    type_of_building, type_of_deal, type_of_rooms, 
                    size_meters, locality, region, district, latitude, longitude,
                    first_update, last_update, first_price, last_price, max_price, min_price, created_at ) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """             
            cursor.executemany(query, data_to_upload)
            conn.commit()

        except sqlite3.Error as e:
            print("Error inserting offer:", e)
            conn.rollback()
            
        conn.close()
        
    def _update_estate(self, df, df_all):
        
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            
            data_to_upload = []
            for i in tqdm(range(len(df))):
                r = df.iloc[i]

                previous_offer = df_all[df_all["code"] == r['code']]
                #print(f"dosavadní maximum: {previous_offer['max_price'].iloc[0]}, nová cena ke zvážení: {r['price']}")
                max_price = max(previous_offer['max_price'].iloc[0], r['price'])
                min_price = min(previous_offer['min_price'].iloc[0], r['price'])
                #print(f"new max a min price: {max_price}, {min_price}")
                
                data_to_upload.append([
                    r["timestamp"],  # last_update should be changed every time the estate occurs in a new batch. chronologically
                    str(max_price),
                    str(min_price),
                    str(r['price']),
                    str(r['code']), 
                    ])

            query = f"""
                    UPDATE estate_detail SET 
                        last_update = ?, 
                        max_price = ?,
                        min_price = ?,
                        last_price = ?
                    WHERE code = ?
                    """          
            cursor.executemany(query, data_to_upload)
            conn.commit()

        except sqlite3.Error as e:
            print("Error updating offer:", e)
            conn.rollback()
            
        conn.close()
    
    def insert_new_price(self, df, timestamp):
        
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
                                       timestamp
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
   
    

    

