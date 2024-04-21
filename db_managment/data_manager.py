from datetime import datetime
import pandas as pd
from tqdm import tqdm
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
          
    def process_new_estates(self, df_new, timestamp):
        """
        Checks if estate codes are already existing in DB.
        Translates info codes into descriptions.
        Generates the data for all three tables (batch, estate, prices)
        """
        
        print(f"Starting to process {len(df_new)} potentially new estates")
        df_all = self.get_all_rows("estate_detail")
        all_estate_codes = set(df_all["code"])
        df_new["code"] = df_new["code"].astype(str)
        
        ######## Inserting new offer ########
        df = df_new[~df_new["code"].isin(all_estate_codes)].copy()
        print(f"There are {len(df)} new estates to CREATE")
        
        df["type_of_deal"] = df["category_type_cb"].apply(self.translate_type_of_deal)
        df["type_of_building"] = df["category_main_cb"].apply(self.translate_type_of_building)
        df["type_of_rooms"] = df["category_sub_cb"].apply(self.translate_type_of_rooms)

        df["check"] = df.apply(lambda row: 1 if row["type_of_rooms"] == row['rooms'] else 0, axis=1)
        if len(df[(df["check"]==0) & 
                  (df["rooms"] != "-")]) > 0:
            raise ValueError("There might be mismatch between type of flat scraped and translated")
        
        #TODO: zapiš i batch a zapiš i cenu. Toto jsou "jen" nové estate_details
        
        self._insert_new_estate(df, timestamp)
        print(f"DONE: processing new estates")
        
        ######## Updating existing offer ########
        df_upd = df_new[df_new["code"].isin(all_estate_codes)].copy()
        print(f"There are {len(df_upd)} estates to UPDATE")
        
        if len(df_upd) > 0:
            self._update_estate(df_upd, df_all)
            print(f"DONE: Updating existing estates")
        else:
            print(f"we do not update anything")
        
          
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
   
    
    def get_regions_and_districs(self):
        raise NotImplementedError
     
    def translate_type_of_building(self, code_category_main_cb):
        return self.cf.type_of_building[str(code_category_main_cb)]
    
    def translate_type_of_deal(self, code_category_type_cb):
        return self.cf.type_of_deal[str(code_category_type_cb)]
    
    def translate_type_of_rooms(self, code_category_sub_cb):
        return self.cf.type_of_rooms[str(code_category_sub_cb)]
    
    def process_all_files(self):
        #TODO: přečte všechny soubory csv ve složce, podle jména = chronologcky, a nahraje.
        raise NotImplementedError
    

