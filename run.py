from datetime import datetime
import pandas as pd
from tqdm import tqdm  
import requests 
from typing import Optional
import os
import json

os.chdir("c:\\Users\\jirka\\Documents\\MyProjects\\Sreality")
from scraper.scraper import SrealityScraper
from db_managment.data_manager import DataManager
from utils.utils import Utilities, GeoData
from utils.mailing import EmailService
from config import Config

from utils.logger import logger

class Runner:
    
    def __init__(self) -> None: 
        self.cf = Config()   
        self.scraper = SrealityScraper()
        self.data_manager = DataManager()
        self.utils = Utilities()
        self.mailing = EmailService()
        self.geodata = GeoData()
        
    def scrape_prices_and_details(self,
                    scrape_prodej_byty: Optional[bool] = True,
                    scrape_all: Optional[bool] = True
                    ) -> pd.DataFrame:
        """
        1.) It runs scraping process for 'prodej_byty' and/or 'all' estates.
        2.) After scraping prices, there is a check of missing estate details in DB and JSON files,
        and follow-up scraping of details for missing records.
        3.) For new estate_details files there is GeoData obtained and saved
        """
        full_datetime, date_to_save = self.utils.generate_timestamp()
        logger.info(f'Starting scraping process.')
        
        # scrape all with filter
        if scrape_prodej_byty:
            logger.info(f'Running scraper for Prodeje - Byty.')
            data_prodej_byty = self.scraper.scrape_all_with_filter(timestamp=full_datetime, category_main_cb=1,category_type_cb=1)
            df_data_prodej_byty = pd.DataFrame(data_prodej_byty)
            self.utils.safe_save_csv(df_data_prodej_byty, f"data_{date_to_save}")
        
        # scrape basically all
        if scrape_all:
            logger.info(f'Running scraper for All')
            data_all = self.scraper.scrape_all_with_filter(timestamp=full_datetime)
            df_data_all = pd.DataFrame(data_all)
            self.utils.safe_save_csv(df_data_all, f"data_all_{date_to_save}")
        
        #TODO: for scrape_prodej_byty, resp for scrape_all ..
        # ? check existing codes in DB. 
        if scrape_prodej_byty:
            existing_codes = set(self.data_manager.get_all_rows("estate_detail")["code"])
            df_codes = set(df_data_prodej_byty["code"].unique())
            
            #df_missing = [x for x in df_codes if x not in list(existing_codes)]
            df_missing = df_codes - existing_codes
            
            # ? check existing codes in JSON - might be not processed previously
            df_missing = self.utils.compare_codes_to_existing_jsons(df_missing)
            logger.info(f'There are {len(df_missing)} new codes to scrape')

            # ? this handles the case when empty JSON would be created and not read by GeoData
            if len(df_missing) > 0:
                self.scraper.scrape_specific_estates(df_missing, full_datetime)
                logger.info(f'Finished scraping missing estate details')        
            else:
                logger.info(f'No need to scrape missing estate details')             
            
        # TODO: deduplicate this section, the only difference is data_all vs df_data_prodej_byty
        # TODO: and saving file name maybe?
        # TODO: or simply just run one function twice, based on argument there will be file_name.
        if scrape_all:
            existing_codes = set(self.data_manager.get_all_rows("estate_detail")["code"])
            df_codes = set(df_data_all["code"].unique())
            
            df_missing = df_codes - existing_codes
            
            df_missing = self.utils.compare_codes_to_existing_jsons(df_missing)
            logger.info(f'There are {len(df_missing)} new codes to scrape.')

            if len(df_missing) > 0:
                self.scraper.scrape_specific_estates(df_missing, full_datetime)
                logger.info(f'Finished scraping missing estate details for ALL')        
            else:
                logger.info(f'No need to scrape missing estate details for ALL')        

    #TODO: tím že je to oděělené od scrapingu, je nutné to mít jako samostatný run, nebo nechat jako util?
    def update_geodata(self, list_of_jsons=None):
        #? Get Geodata for all JSON files that do not have any, usually for new estate_detail scraped.
        logger.info(f'Updating GeoData')
        self.geodata.enrich_jsons_with_geodata(list_of_jsons=list_of_jsons)
        logger.info(f'Updating GeoData Done')
        
    #TODO: pracovat pouze s NOVÝMI, nebo jinak označenými JSONy. Nikoliv merge all a pak porovnej?
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
        
        existing_codes = df["code"].unique() if len(df) > 0 else []
        df_new = df_new[~df_new["code"].isin(existing_codes)]
        logger.info(f'Prepared {len(df_new)} new rows.')
                
        #? Take offers which codes are not yet in DB and load them into DB
        if len(df_new) > 0:
            self.data_manager.insert_new_estates(df_new)
            logger.info(f'Insert into estate_detail DONE.')
        else:
            logger.info(f'No need to insert into estate_detail. DONE.')

    def input_all_prices_to_db(self):
        """
        This process takes all existing CSVs with estate prices, compare to existing rows in database,
        and preprocess and insert the new ones into price_history table.
        """
        
        #? Prepare all CSV with prices to df, and compare to existing DB
        logger.info(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}: Starting processing CSVs to price_history table.')
        df_new = self.utils.prepare_price_history_csv_to_df()
        df_new = df_new.drop_duplicates()
        
        df = self.data_manager.get_all_rows("price_history")
        logger.info(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}: Already {len(df)} rows in price_history.')
        
        #? cool way how to find rows which are not part of another df
        df_new = df_new[~df_new.isin(df.to_dict(orient='list')).all(axis=1)]
        logger.info(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}: Prepared {len(df_new)} new rows.')

        #? Take prices which are not yet in DB and load them into DB ??
        if len(df_new) > 0:
            self.data_manager.insert_new_price(df_new)
            logger.info(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}: Inserting into price_history DONE.')
        else:
            logger.info(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}: No need to insert into price_history. DONE.')
    
    #? Combination of scraping, GeoPandas, and Updating DB
    def run_complete_scraping(self,
                            scrape_prodej_byty: Optional[bool] = True,
                            scrape_all: Optional[bool] = True)->pd.DataFrame:
        """
        This is a combination of Scraping, Follo-up scraping, GeoPandas, and Updating both DB tables with new rows.
        """
        logger.info(f'STARTING complete process of data gathering.')

        self.scrape_prices_and_details(scrape_prodej_byty=scrape_prodej_byty,
                                       scrape_all=scrape_all)
        
        self.update_geodata(list_of_jsons=None)
        
        self.input_all_estates_to_db()
        self.input_all_prices_to_db()
        
        logger.info(f'FINISHING complete process of data gathering.')
