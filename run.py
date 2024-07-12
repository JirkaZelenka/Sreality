import pandas as pd
from typing import Optional
import os
import json
from tqdm import tqdm

from diagnostics.diagnostics import Diagnostics
from scraper.scraper import SrealityScraper
from db_managment.data_manager import DataManager
from utils.utils import Utilities
from utils.geodata import GeoData
from utils.mailing import EmailService
from config import Config

from utils.logger import logger_scraping

class Runner:
    
    #TODO: each object in config has its own config.py, so does runner itself. too many?
    def __init__(self) -> None: 
        self.cf = Config()   
        self.scraper = SrealityScraper()
        self.data_manager = DataManager()
        self.utils = Utilities()
        self.mailing = EmailService()
        self.geodata = GeoData()
        self.diag = Diagnostics()
        
    def scrape_prices_and_details(self, combinations: list["str"]) -> None:
        """
        1.) It runs scraping process for defined combinations of interest, such as "prodej_byty".
            Avoiding to scrape "all", as there is a limitation of 60k offers available out of 95k
        2.) After scraping prices, there is a check of missing estate details in DB and JSON files,
        and follow-up scraping of details for missing records.
        3.) For new estate_details files, there are GeoData obtained and saved
        """
        full_datetime, date_to_save = self.utils.generate_timestamp()
        
        data_dict = self.scraper.scrape_multiple_sections(combinations, full_datetime)
        logger_scraping.info(f'Data scraped for all combinations.')
        
        #TODO: missing codes. co nesesbírám hned poté, tak můůžu později, ale jen okud na něj narazím znovu na ten objekt
        for combination in combinations:
            try:
                data = data_dict[combination]
                df_data = pd.DataFrame(data)
                self.utils.safe_save_csv(df_data, 
                                         f"data_{combination}_{date_to_save}")
            except Exception as e:
                logger_scraping.error(f'There is no key {combination} in returned data: {e}')   
        
        #TODO: částečná duplikace postupu, ale..asi ok
        for combination in combinations:
            try:
                data = data_dict[combination]
                df_data = pd.DataFrame(data)
                # ? check existing codes in DB. 
                existing_codes = set(self.data_manager.get_all_rows("estate_detail")["estate_id"])
                df_codes = set(df_data["estate_id"].unique())
                
                #df_missing = [x for x in df_codes if x not in list(existing_codes)]
                df_missing = df_codes - existing_codes
                
                # ? check existing codes in JSON - might be not processed previously
                df_missing = self.utils.compare_codes_to_existing_jsons(df_missing)
                logger_scraping.info(f'There are {len(df_missing)} new codes to scrape for {combination}.')

                # ? this handles the case when empty JSON would be created and not read by GeoData
                if len(df_missing) > 0:
                    #TODO: z fce vyndat saving, abych ji mohl použít samostaně na dodatkový scrpaing + update jsonu
                    self.scraper._scrape_specific_estates(df_missing, full_datetime)
                    #logger_scraping.info(f'Finished scraping missing estate details for {combination}.')        
                else:
                    logger_scraping.info(f'No need to scrape missing estate details for {combination}.')             
            except Exception as e:
                logger_scraping.error(f'Some error occured during scraping estate_details for {combination}: {e}')  

    #TODO: tím že je to odělené od scrapingu, je nutné to mít jako samostatný run, nebo nechat jako util?
    def update_geodata(self, list_of_jsons=None):
        #? Get Geodata for all JSON files that do not have any, usually for new estate_detail scraped.
        logger_scraping.info(f'Updating GeoData')
        self.geodata.enrich_jsons_with_geodata(list_of_jsons=list_of_jsons)
        logger_scraping.info(f'Updating GeoData Done')
        
    def input_all_estates_to_db(self):
        """
        This process takes all existing JSONs with estate details, compare unique codes to those in database,
        and preprocess and insert the new ones into estate_detail table.
        """
        
        #? First prepare all existing JSONs to df, and compare to existing DB
        logger_scraping.info(f'Starting processing JSONs to estate_detail table.')
        df_new = self.utils.prepare_estate_detail_jsons_to_df()
        
        df = self.data_manager.get_all_rows("estate_detail")
        logger_scraping.info(f'Already {len(df)} rows in estate_detail.')
        
        existing_codes = df["estate_id"].unique() if len(df) > 0 else []
        df_new = df_new[~df_new["estate_id"].isin(existing_codes)]
        logger_scraping.info(f'Prepared {len(df_new)} new rows.')
                
        #? Take offers which codes are not yet in DB and load them into DB
        if len(df_new) > 0:
            self.data_manager.insert_new_estates(df_new)
            #logger_scraping.info(f'Insert into estate_detail DONE.')
        else:
            logger_scraping.info(f'No need to insert into estate_detail. DONE.')

    def input_all_prices_to_db(self):
        """
        This process takes all existing CSVs with estate prices, 
        compares the names to those in the price_history_loaded.txt,
        and preprocess and insert the new ones into price_history table.
        """
        
        logger_scraping.info(f'Starting processing CSVs to price_history table.')
        not_processed_files = self.utils.get_list_of_not_processed_files("price_history_loaded.txt")
        for file_name in tqdm(not_processed_files):
            df_new = self.utils.process_file_to_df(file_name)
            df_new = df_new.drop_duplicates()
            #logger_scraping.info(f'Prepared {len(df_new)} new rows.')
        
            """
            df = self.data_manager.get_all_rows("price_history")
            logger_scraping.info(f'Already {len(df)} rows in price_history.')
            #? cool way how to find rows which are not part of another df
            df_new = df_new[~df_new.isin(df.to_dict(orient='list')).all(axis=1)]
            """

            if len(df_new) > 0:
                try:
                    self.data_manager.insert_new_price_v1(df_new)
                    self.utils._write_processed_prices([file_name], "price_history_loaded.txt")
                except Exception as e:
                    logger_scraping.error(f'Inserting into price_history failed for file {file_name}: {e}')
            else:
                logger_scraping.info(f'No need to insert into price_history bcs file {file_name} seems empty. DONE.')          

    def input_all_prices_to_db2(self):
        """
        This process takes all existing CSVs with estate prices, 
        compares the names to those in the price_history_loaded.txt,
        and preprocess and insert the new ones into price_history table.
        """
        
        logger_scraping.info(f'Starting processing CSVs to price_history_new2 table.')
        not_processed_files = self.utils.get_list_of_not_processed_files("price_history_loaded - kopie.txt")
        print(len(not_processed_files))
        for file_name in not_processed_files:
            df_new = self.utils.process_file_to_df(file_name)
            df_new = df_new.drop_duplicates()

            if len(df_new) > 0:
                try:
                    self.data_manager.insert_new_price_v2(df_new)
                    self.utils._write_processed_prices([file_name], "price_history_loaded - kopie.txt")
                except Exception as e:
                    logger_scraping.error(f'Inserting into price_history failed for file {file_name}: {e}')
            else:
                logger_scraping.info(f'No need to insert into price_history bcs file {file_name} seems empty. DONE.')
        
        
    def run_complete_scraping(self,
                              combinations: list["str"]
                              )->pd.DataFrame:
        """
        This is a combination of Scraping, Follow-up scraping, 
        GeoPandas, and Updating both DB tables with new rows.
        """
        logger_scraping.info(f'STARTING complete process of data gathering.')

        self.scrape_prices_and_details(combinations)
        
        self.update_geodata(list_of_jsons=None)
        
        #TODO: do this repeatedly, bcs it might fail ocassionaly
        self.input_all_estates_to_db()
        self.input_all_prices_to_db()
        #self.input_all_prices_to_db2()
        #TODO: pokud chci běžet oboje, tak musím mít buď dva listy na zpracované soubory, 
        #todo nebo obě insert1 a insert2 spojit dovnitř input_all_prices, logicky. po obou se zapíše file.
        #todo: po souhrnném testu (a obětování dat 04-05) commit. a udržování obou DB.
        
        discounts_all = self.diag.discounts_in_last_batch(filters=None)
        self.mailing.send_email(subject=f'SREALITY - DISCOUNTS {discounts_all["Last Date"]}',
                                message_text=json.dumps(discounts_all))
        
        discounts_targeted = self.diag.discounts_in_last_batch()
        self.mailing.send_email(subject=f'SREALITY - DISCOUNTS Prodej-Byty-Praha,Středočeský {discounts_targeted["Last Date"]}',
                                message_text=json.dumps(discounts_targeted))
        
        logger_scraping.info(f'FINISHING complete process of data gathering.')

    #TODO: use it:)
    def update_empty_estates(self):
        
        folder_with_estate_details = f"{self.cf.project_path}/{self.cf.data_folder}/{self.cf.estate_details_folder}"
        files = os.listdir(folder_with_estate_details)
        
        for file in files:
            with open(file, 'r') as file:
                data = json.load(file)
                # TODO: pro každý item ve file se porskenuje a spustí na LISTU IDs updating
            

    def diagnostics_after_run(self):
        raise NotImplementedError