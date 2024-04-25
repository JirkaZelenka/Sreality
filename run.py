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
from utils.utils import Utilities
from config import Config

import logging
logger = logging.getLogger(__name__)
logging.basicConfig(filename='scraping.log', encoding='utf-8', level=logging.INFO)
#TODO logging: 1) variovat jméno loggeru? 2) odebrat timestampy extra 3) pořešit proč tam mám debug logy
#TODO 4) zkontrolovat propis vnitřních logů z funkcí (až je přidám) 5) odebrat některé printy  6) víc úrovní logů než INFO

class Runner:
    
    def __init__(self) -> None: 
        self.cf = Config()   
        self.scraper = SrealityScraper()
        self.data_manager = DataManager()
        self.utils = Utilities()
        
    def regular_run(self,
                    scrape_prodej_byty: Optional[bool] = True,
                    scrape_all: Optional[bool] = True
                    ) -> pd.DataFrame:
        
        # get timestamp
        full_datetime, date_to_save = self.utils.generate_timestamp()
        print(f"timestamp: {full_datetime}")
        logger.info(f"{full_datetime}: Starting a regular run")
        
        # scrape all with filter
        if scrape_prodej_byty:
            logger.info(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}: Running scraper for Prodeje - Byty')
            data_prodej_byty = self.scraper.scrape_all_with_filter(timestamp=full_datetime, category_main_cb=1,category_type_cb=1)
            df_data_prodej_byty = pd.DataFrame(data_prodej_byty)
            self.utils.safe_save_csv(df_data_prodej_byty, f"data_{date_to_save}")
        
        if scrape_all:
            logger.info(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}: Running scraper for All')
            data_all = self.scraper.scrape_all_with_filter(timestamp=full_datetime)
            df_data_all = pd.DataFrame(data_all)
            self.utils.safe_save_csv(df_data_all, f"data_all_{date_to_save}")
        
        #TODO: for scrape_prodej_byty, resp for scrape_all ..
        # check existing code in DB. 
        if scrape_prodej_byty:
            logger.info(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}: Looking for new estates in Prodeje - Byty')
            existing_codes = self.data_manager.get_all_rows("estate_detail")["code"]
            df_codes = df_data_prodej_byty["code"].unique()
            df_missing = [x for x in df_codes if x not in list(existing_codes)]
            logger.info(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}: There are {len(df_missing)} missing codes in DB')
            print(f"Missing codes: {len(df_missing)}")
            
            # If not in DB, check prepared JSON File
            df_missing = self.utils.compare_codes_to_existing_jsons(df_missing)
            logger.info(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}: There are still {len(df_missing)} missing codes after JSON check')
            print(f"Still missing codes: {len(df_missing)}")

            # scrape details of missing esates
            logger.info(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}: Going to scrape missing estate details')
            new_estate_details = self.scraper.scrape_specific_estates(df_missing, full_datetime)
            
            logger.info(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}: Finished scraping missing estate details')        
            
            # Add locality, city, region, district.
            #TODO: instead of searching for some file with some timstamp in  name, use manually
            #TODO but in future i need to load all old jsons and do the same - these are only new buildings
            """df = self.utils.prep_df_new_estates()"""
            
            df = pd.DataFrame(new_estate_details)
            for c in ["note_about_price", "id_of_order", "last_update", "material",
                  "age_of_building", "ownership_type", "floor", "usable_area",
                  "floor_area", "energy_efficiency_rating", "no_barriers", "start_of_offer",
                  ]:
                if c not in df.columns:
                    df[c] = None
            
            logger.info(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}: Searching for Locality, City, Region, District')
            #TODO: apply is not tqdm.( cant watch it
            df = self.utils.assign_location_to_df(df)
            
            return df
            # Add new estates to DB
            
            # Update existing estates in DB
            
            # save all new prices to DB
            
            # create LOG file
        
        
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
            
        """
        ##
        
        
        
        
        