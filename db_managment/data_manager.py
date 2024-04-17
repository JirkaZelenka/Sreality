from datetime import datetime
import pandas as pd
import re

import sqlite3
import os

from config import Config 
 
class DataManager:
    
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
        raise NotImplementedError
     
    def translate_type_of_building(self, code_category_main_cb):
        return self.cf.type_of_building[str(code_category_main_cb)]
    
    def translate_type_of_deal(self, code_category_type_cb):
        return self.cf.type_of_deal[str(code_category_type_cb)]
    
    def translate_type_of_rooms(self, code_category_sub_cb):
        return self.cf.type_of_rooms[str(code_category_sub_cb)]
    
    def update_existing_estate(self):
        #TODO:
        raise NotImplementedError
    

