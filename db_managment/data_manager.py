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
                    str(r['description']),
                    str(r['meta_description']),
                    str(r['category_main_cb']),
                    str(r['category_type_cb']),
                    str(r['category_sub_cb']),
                    str(r['broker_id']),
                    str(r['broker_company']),
                    str(r['furnished']),
                    str(r['locality_gps_lat']),
                    str(r['locality_gps_lon']),
                    str(r['locality']), 
                    str(r['city']), 
                    str(r['district']), 
                    str(r['region']),
                    str(r['object_type']),
                    str(r['parking_lots']),
                    str(r['locality_street_id']),
                    str(r['locality_district_id']),
                    str(r['locality_ward_id']),
                    str(r['locality_region_id']),
                    str(r['locality_quarter_id']),
                    str(r['locality_municipality_id']),
                    str(r['locality_country_id']),
                    str(r['terrace']),
                    str(r['balcony']),
                    str(r['loggia']),
                    str(r['basin']),
                    str(r['cellar']),
                    str(r['building_type']),
                    str(r['object_kind']),
                    str(r['ownership']),
                    str(r['low_energy']),
                    str(r['easy_access']),
                    str(r['building_condition']),
                    str(r['garage']),
                    str(r['room_count_cb']),
                    str(r['energy_efficiency_rating_cb']),
                    str(r['note_about_price']),
                    str(r['id_of_order']),
                    str(r['last_update']),
                    str(r['material']),
                    str(r['age_of_building']),
                    str(r['ownership_type']),
                    str(r['floor']),
                    str(r['usable_area']),
                    str(r['floor_area']),
                    str(r['energy_efficiency_rating']),
                    str(r['no_barriers']),
                    timestamp
                    ])

            query = f"""
                    INSERT INTO estate_detail (
                    code, description, meta_description,
                    category_main_cb, category_type_cb, category_sub_cb,
                    broker_id, broker_company, furnished,
                    latitude, longitude, locality, city, district, region,
                    object_type, parking_lots, locality_street_id, locality_district_id,
                    locality_ward_id, locality_region_id, locality_quarter_id,
                    locality_municipality_id, locality_country_id, terrace, balcony,
                    loggia, basin, cellar, building_type, object_kind,
                    ownership, low_energy, easy_access, building_condition, garage,
                    room_count_cb, energy_efficiency_rating_cb, note_about_price,
                    id_of_order, last_update, material, age_of_building,
                    ownership_type, floor, usable_area, floor_area,
                    energy_efficiency_rating, no_barriers, created_at)
                    
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """             
            cursor.executemany(query, data_to_upload)
            conn.commit()

        except sqlite3.Error as e:
            print("Error inserting offer:", e)
            conn.rollback()
            
        conn.close()
        
    #TODO: removing this as obsolete, no updates of estate_detail in terms of price
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
                                       r["price"],
                                       timestamp
                                       ])
                
            query = f"""
                    INSERT INTO scraped_prices (
                    estate_id, price, created_at)
                    VALUES (?, ?, ?)
                    """
            cursor.executemany(query, data_to_upload)
            conn.commit()

        except sqlite3.Error as e:
            print("Error inserting offer:", e)
            conn.rollback()
            
        conn.close()
   
    

    

