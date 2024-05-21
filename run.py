import pandas as pd
from typing import Optional
import os
import json

from diagnostics.diagnostics import Diagnostics
from scraper.scraper import SrealityScraper
from db_managment.data_manager import DataManager
from utils.utils import Utilities
from utils.geodata import GeoData
from utils.mailing import EmailService
from config import Config

from utils.logger import logger

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
        logger.info(f'Starting scraping process.')
        
        data_dict = self.scraper.scrape_multiple_sections(combinations, full_datetime)
        logger.info(f'Data scraped for all combinations.')
        
        #TODO: missing codes. co nesesbírám hned poté, tak můůžu později, ale jen okud na něj narazím znovu na ten objekt
        for combination in combinations:
            try:
                data = data_dict[combination]
                df_data = pd.DataFrame(data)
                self.utils.safe_save_csv(df_data, 
                                         f"data_{combination}_{date_to_save}")
            except Exception as e:
                logger.error(f'There is no key {combination} in returned data: {e}')   
        
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
                logger.info(f'There are {len(df_missing)} new codes to scrape for {combination}.')

                # ? this handles the case when empty JSON would be created and not read by GeoData
                if len(df_missing) > 0:
                    self.scraper._scrape_specific_estates(df_missing, full_datetime)
                    logger.info(f'Finished scraping missing estate details for {combination}.')        
                else:
                    logger.info(f'No need to scrape missing estate details for {combination}.')             
            except Exception as e:
                logger.error(f'Some error occured during scraping estate_details for {combination}: {e}')  

    #TODO: tím že je to odělené od scrapingu, je nutné to mít jako samostatný run, nebo nechat jako util?
    def update_geodata(self, list_of_jsons=None):
        #? Get Geodata for all JSON files that do not have any, usually for new estate_detail scraped.
        logger.info(f'Updating GeoData')
        self.geodata.enrich_jsons_with_geodata(list_of_jsons=list_of_jsons)
        logger.info(f'Updating GeoData Done')
        
    def input_all_estates_to_db(self):
        """
        This process takes all existing JSONs with estate details, compare unique codes to those in database,
        and preprocess and insert the new ones into estate_detail table.
        """
        
        #? First prepare all existing JSONs to df, and compare to existing DB
        logger.info(f'Starting processing JSONs to estate_detail table.')
        df_new = self.utils.prepare_estate_detail_jsons_to_df()
        
        df = self.data_manager.get_all_rows("estate_detail")
        logger.info(f'Already {len(df)} rows in estate_detail.')
        
        existing_codes = df["estate_id"].unique() if len(df) > 0 else []
        df_new = df_new[~df_new["estate_id"].isin(existing_codes)]
        logger.info(f'Prepared {len(df_new)} new rows.')
                
        #? Take offers which codes are not yet in DB and load them into DB
        if len(df_new) > 0:
            self.data_manager.insert_new_estates(df_new)
            logger.info(f'Insert into estate_detail DONE.')
        else:
            logger.info(f'No need to insert into estate_detail. DONE.')

    #TODO: pracovat pouze s NOVÝMI, nebo jinak označenými JSONy. Nikoliv merge all a pak porovnej?
    def input_all_prices_to_db(self):
        """
        This process takes all existing CSVs with estate prices, compare to existing rows in database,
        and preprocess and insert the new ones into price_history table.
        """
        
        #? Prepare all CSV with prices to df, and compare to existing DB
        #TODO: based on the file with names of files that are already loaded?
        logger.info(f'Starting processing CSVs to price_history table.')
        df_new, not_processed_files = self.utils.prepare_price_history_csv_to_df()
        df_new = df_new.drop_duplicates()
        
        """
        df = self.data_manager.get_all_rows("price_history")
        logger.info(f'Already {len(df)} rows in price_history.')
        
        #? cool way how to find rows which are not part of another df
        df_new = df_new[~df_new.isin(df.to_dict(orient='list')).all(axis=1)]
        """
        logger.info(f'Prepared {len(df_new)} new rows.')

        #? Take prices which are not yet in DB and load them into DB ??
        if len(df_new) > 0:
            try:
                self.data_manager.insert_new_price(df_new)
                logger.info(f'Inserting into price_history DONE.')
                self.utils.write_processed_prices(not_processed_files)
            except Exception as e:
                logger.error(f'Inserting into price_history failed: {e}')
        else:
            logger.info(f'No need to insert into price_history. DONE.')
    
    #? Combination of scraping, GeoPandas, and Updating DB
    def run_complete_scraping(self,
                              combinations: list["str"]
                              )->pd.DataFrame:
        """
        This is a combination of Scraping, Follow-up scraping, GeoPandas, and Updating both DB tables with new rows.
        """
        logger.info(f'STARTING complete process of data gathering.')

        self.scrape_prices_and_details(combinations)
        
        self.update_geodata(list_of_jsons=None)
        
        self.input_all_estates_to_db()
        self.input_all_prices_to_db()
        
        result = self.diag.summary_new_estates()
        self.mailing.send_email(subject=f'SCRAPING SREALITY SUMMARY for {result["Last Date"]}',
                                message_text=json.dumps(result))
        
        
        logger.info(f'FINISHING complete process of data gathering.')

    def diagnostics_after_run(self):
        raise NotImplementedError