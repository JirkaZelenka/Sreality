import pandas as pd
from tqdm import tqdm
import sqlite3

from config import Config 

from utils.logger import logger

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
            logger.error(f'Error connecting to database: {e}') 

            return None

    def get_count_estates(self, table_name="estate_detail"):
        
        conn = self._get_connection()        
        try:
            f"SELECT * FROM {table_name}"
            
            query = f"""SELECT 
                        COUNT(*) as 'row count', 
                        COUNT(DISTINCT estate_id) as 'unique estate count',
                        MIN(DISTINCT DATE(crawled_at)) as 'first day',
                        MAX(DISTINCT DATE(crawled_at)) as 'last day',
                        MAX(DISTINCT crawled_at) as 'last scraping',
                        COUNT(DISTINCT region) as 'regions count',
                        COUNT(DISTINCT district) as 'district count',
                        COUNT(DISTINCT city) as 'city count'
                    FROM {table_name};"""            
            
            df = pd.read_sql_query(query, conn)
            conn.close()
            return df
        
        except sqlite3.Error as e:
            logger.error(f'Error loading rows from table {table_name}: {e}') 
            conn.rollback()
        
        conn.close()
        
    def get_count_prices(self, table_name="price_history"):
        
        conn = self._get_connection()        
        try:
            f"SELECT * FROM {table_name}"
            
            query = f"""SELECT 
                        COUNT(*) as 'all_rows',
                        COUNT(DISTINCT estate_id) as 'počet unikátních objektů',
                        COUNT(DISTINCT DATE(crawled_at)) as 'počet sledovaných dnů',
                        MIN(DISTINCT DATE(crawled_at)) as 'první sledovaný den',
                        MAX(DISTINCT DATE(crawled_at)) as 'poslední sledovaný den'
                    FROM {table_name};"""            
            
            df = pd.read_sql_query(query, conn)
            conn.close()
            return df
        
        except sqlite3.Error as e:
            logger.error(f'Error loading rows from table {table_name}: {e}') 
            conn.rollback()
        
        conn.close()
    
    def get_all_rows(self, table_name):
        
        conn = self._get_connection()        
        try:
            query = f"SELECT * FROM {table_name}"
            df = pd.read_sql_query(query, conn)
            conn.close()
            return df
        
        except sqlite3.Error as e:
            logger.error(f'Error loading rows from table {table_name}: {e}') 
            conn.rollback()
        
        conn.close()
        
    def get_all_rows_from_date(self, table_name, timestamp):
        
        conn = self._get_connection()        
        try:
            query = f"SELECT * FROM {table_name} WHERE crawled_at = '{timestamp}'"
            df = pd.read_sql_query(query, conn)
            conn.close()
            return df
        
        except sqlite3.Error as e:
            logger.error(f'Error loading rows from table {table_name} for timestamp {timestamp}: {e}') 
            conn.rollback()
        
        conn.close()
        
    def clear_table(self, table_name):
        
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(f"DELETE FROM {table_name}")
            conn.commit()
            cursor.close()
        except sqlite3.Error as e:
            logger.error(f'Error clearing table {table_name}: {e}') 
            conn.rollback()
        
        conn.close()
            
    def drop_table(self, table_name):
        
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(f"DROP table {table_name}")
            conn.commit()
            cursor.close()
        except sqlite3.Error as e:
            logger.error(f'Error dropping table {table_name}: {e}') 
            conn.rollback()
        
        conn.close()
               
    def create_table(self, table_name):
        
        conn = self._get_connection()
        
        table = self.cf.table_definitions[table_name]
        try:
            cursor = conn.cursor()
            cursor.execute(table)
            conn.commit()
            cursor.close()
        except sqlite3.Error as e:
            logger.error(f'Error creating table {table_name}: {e}') 
            conn.rollback()
        
        conn.close()
                 
    def insert_new_estates(self, df):
        
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            
            data_to_upload = []
            for i in range(len(df)):
                r = df.iloc[i]
                data_to_upload.append([
                    str(r['estate_id']),
                    str(r['description']),
                    str(r['meta_description']),
                    str(r['category_main_cb']),
                    str(r['category_type_cb']),
                    str(r['category_sub_cb']),
                    str(r['locality_url']),
                    str(r['estate_url']),
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
                    str(r['start_of_offer']),
                    str(r['timestamp']),
                    ])

            query = f"""
                    INSERT INTO estate_detail (
                    estate_id, description, meta_description,
                    category_main_cb, category_type_cb, category_sub_cb, locality_url, estate_url,
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
                    energy_efficiency_rating, no_barriers, start_of_offer, crawled_at)
                    
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
                            ?, ?, ?)
                    """             
            cursor.executemany(query, data_to_upload)
            conn.commit()

        except sqlite3.Error as e:
            logger.error(f'Error inserting offer: {e}')        
            conn.rollback()
            
        conn.close()
        
    # TODO: future use?
    def _update_estate(self, df):
        """
        Updates all estates from the DF. Currently only Timestamp, but could be any detail.
        """
        
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            
            data_to_upload = []
            for i in tqdm(range(len(df))):
                r = df.iloc[i]

                data_to_upload.append([
                    r["timestamp"]
                    ])

            query = f"""
                    UPDATE estate_detail SET 
                        last_update = ?
                    WHERE code = ?
                    """          
            cursor.executemany(query, data_to_upload)
            conn.commit()

        except sqlite3.Error as e:
            logger.error(f'Error updating offer: {e}')        
            conn.rollback()
            
        conn.close()
    
    def insert_new_price(self, df):
        
        conn = self._get_connection()
        try:
            #? only here foreing keys constraint can be applied and need to be actively enforced
            #conn.execute("PRAGMA foreign_keys = ON")
            cursor = conn.cursor()
            
            data_to_upload = []
            for i in range(len(df)):
                r = df.iloc[i]
                data_to_upload.append([str(r['estate_id']),
                                       str(r["price"]),
                                       str(r["crawled_at"])
                                       ])
                
            query = f"""
                    INSERT INTO price_history (
                    estate_id, price, crawled_at)
                    VALUES (?, ?, ?)
                    """
            cursor.executemany(query, data_to_upload)
            conn.commit()

        except sqlite3.Error as e:
            logger.error(f'Error inserting offer: {e}')        
            conn.rollback()
            
        conn.close()
   
    

    

