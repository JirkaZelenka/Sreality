import pandas as pd
from tqdm import tqdm
import sqlite3

from config import Config 

from utils.logger import logger_scraping

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
            logger_scraping.error(f'Error connecting to database: {e}') 

            return None

    def get_count_estates(self, table_name="estate_detail"):
        
        conn = self._get_connection()        
        try:
            f"SELECT * FROM {table_name}"
            
            #? these names go to the table in the FastAPI
            query = f"""SELECT 
                        COUNT(*) as 'estate count', 
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
            logger_scraping.error(f'Error loading rows from table {table_name}: {e}') 
            conn.rollback()
        
        conn.close()
        
    def get_count_prices(self, table_name="price_history"):
        
        conn = self._get_connection()        
        try:
            f"SELECT * FROM {table_name}"
            
            query = f"""SELECT 
                        COUNT(*) as 'all_rows'
                    FROM {table_name};"""            
            
            df = pd.read_sql_query(query, conn)
            conn.close()
            return df
        
        except sqlite3.Error as e:
            logger_scraping.error(f'Error loading rows from table {table_name}: {e}') 
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
            logger_scraping.error(f'Error loading rows from table {table_name}: {e}') 
            conn.rollback()
        
        conn.close()
    
    #TODO: delete later? when new structure of prices holds
    def get_price_rows_for_test(self, number):
        
        conn = self._get_connection()        
        try:
            query = f"SELECT distinct estate_id, crawled_at FROM price_history limit {number}"
            df = pd.read_sql_query(query, conn)
            conn.close()
            return df
        
        except sqlite3.Error as e:
            logger_scraping.error(f'Error loading rows from table price_history for test: {e}') 
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
            logger_scraping.error(f'Error loading rows from table {table_name} for timestamp {timestamp}: {e}') 
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
            logger_scraping.error(f'Error clearing table {table_name}: {e}') 
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
            logger_scraping.error(f'Error dropping table {table_name}: {e}') 
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
            logger_scraping.error(f'Error creating table {table_name}: {e}') 
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
                    str(r['elevator']),
                    str(r['estate_area']),
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
                    loggia, basin, elevator, estate_area, cellar, building_type, object_kind,
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
                            ?, ?, ?, ?, ?)
                    """             
            cursor.executemany(query, data_to_upload)
            conn.commit()

        except sqlite3.Error as e:
            logger_scraping.error(f'Error inserting offer: {e}')        
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
            logger_scraping.error(f'Error updating offer: {e}')        
            conn.rollback()
            
        conn.close()
    
    def insert_new_price_v1(self, df):
        
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
            logger_scraping.error(f'Error inserting offer: {e}')        
            conn.rollback()
            
        conn.close()
   
    def insert_new_price_v2(self, df):
        
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            
            #? this is new data we want to store
            for i in tqdm(range(len(df))):
                r = df.iloc[i]
                estate_id = str(r['estate_id'])                
                price = str(r["price"])
                crawled_at = str(r["crawled_at"])
                #print(f"processing this record: {estate_id}, {price}, {crawled_at}")
                
                #? Check the latest price entry for this estate_id
                #TODO: nebrat start_date, protože ho stejně nepoužívám..?
                cursor.execute("""
                    SELECT estate_id, price, start_date, end_date 
                    FROM price_history_new2 
                    WHERE estate_id = ? 
                    ORDER BY start_date DESC 
                    LIMIT 1
                    """, (estate_id,))
                
                latest_entry = cursor.fetchone()
                #print(f"latest_entry: {latest_entry}")
                
                if latest_entry:
                    _estate_id, latest_price, _latest_start_date, latest_end_date = latest_entry
                    
                    #? If price changed, Update the end_date of the previous price record
                    if str(latest_price) != str(price):
                        cursor.execute("""
                            UPDATE price_history_new2
                            SET end_date = ?
                            WHERE estate_id = ? AND price = ? AND end_date = ?
                            """, (crawled_at, estate_id, latest_price, latest_end_date))
                        
                        #? and also insert the new price record, START = END
                        cursor.execute("""
                            INSERT INTO price_history_new2 (
                            estate_id, price, start_date, end_date)
                            VALUES (?, ?, ?, ?)
                            """, (estate_id, price, crawled_at, crawled_at))
                        
                        #print(f"This price {price} was New, so we updated last price end date as:{estate_id, latest_price, crawled_at} ")
                        #print(f"and we inserted a new record {(estate_id, price, crawled_at, crawled_at)}")
                    
                    #? If price is same, just update the end_date of the existing price
                    else:
                        cursor.execute("""
                            UPDATE price_history_new2
                            SET end_date = ?
                            WHERE estate_id = ? AND price = ? AND end_date = ?
                            """, (crawled_at, estate_id, price, latest_end_date))
                        #print(f"This price {price} already existed, so we just prolong the date: {crawled_at}")
                
                #? If No previous record exists, insert the new price record
                else:
                    cursor.execute("""
                        INSERT INTO price_history_new2 (
                        estate_id, price, start_date, end_date)
                        VALUES (?, ?, ?, ?)
                        """, (estate_id, price, crawled_at, crawled_at))
                    #print(f"This estate_id {estate_id} is new, so we make first row of it: {estate_id, price, crawled_at, crawled_at} !")  
                      
            conn.commit()

        except sqlite3.Error as e:
            logger_scraping.error(f'Error inserting offer: {e}')        
            conn.rollback()
            
        conn.close()

    def transformation_create_dates(self, df):
        """
        Group DF by estate_id, order chronologically, and group the consecutive prices, 
        creating start_date and end_date.
        """
        transformed_data = []
        for estate_id, group in df.groupby('estate_id'):
            group = group.sort_values(by='crawled_at').reset_index(drop=True)
            
            if group.empty:
                continue

            current_price = group.at[0, 'price']
            start_date = group.at[0, 'crawled_at']
            end_date = None
            
            for i in range(1, len(group)):
                next_start_date = group.at[i, 'crawled_at']
                
                if group.at[i, 'price'] == current_price:
                    end_date = next_start_date
                else:
                    transformed_data.append([estate_id, current_price, start_date, next_start_date])
                    current_price = group.at[i, 'price']
                    start_date = next_start_date
                    end_date = None

            transformed_data.append([estate_id, current_price, start_date, end_date])
        
        return pd.DataFrame(transformed_data, columns=['estate_id', 'price', 'start_date', 'end_date'])

    def transformation_get_old_db(self):
        """
        Grabs the current price_history table and turns it into a df.
        """
        conn = self._get_connection()        
        try:
            query = f"SELECT * FROM price_history;"
            df = pd.read_sql_query(query, conn)
            conn.close()
            print("data retrieved from table price_history")
            return df
        except sqlite3.Error as e:
            logger_scraping.error(f'Error loading rows from table price_history: {e}') 
            conn.rollback() 
            conn.close()
        
    def transformation_create_new_table(self, df, table_name):    
        conn = self._get_connection()  
            
        df.sort_values(by=['estate_id', 'crawled_at'], inplace=True)
        print("sorted by price and date")
        
        df_v2 = self.transformation_create_dates(df)
        print("data transformed to start/end_date")

        try:
            df_v2.to_sql(table_name, conn, if_exists='replace', index=False)
            print(f"data written into new table: {table_name}")
            conn.close()
            return df_v2
        except:
            print(f"data NOT written into new table: {table_name}")
            conn.close()
            return df_v2
        
