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
        
        # scrape all with filter
        if scrape_prodej_byty:
            data_prodej_byty = self.scraper.scrape_all_with_filter(timestamp=full_datetime, category_main_cb=1,category_type_cb=1)
            df_data_prodej_byty = pd.DataFrame(data_prodej_byty)
            self.utils.safe_save_csv(df_data_prodej_byty, f"data_{date_to_save}")
        
        if scrape_all:
            data_all = self.scraper.scrape_all_with_filter(timestamp=full_datetime)
            df_data_all = pd.DataFrame(data_all)
            self.utils.safe_save_csv(df_data_all, f"data_all_{date_to_save}")
        
        
        
        #TODO: for scrape_prodej_byty, resp for scrape_all ..
        # check existing code in DB. 
        if scrape_prodej_byty:
            df = df_data_prodej_byty.copy()
            existing_codes = self.data_manager.get_all_rows("estate_detail")["code"]
            df_codes = df["code"].unique()
            df_missing = [x for x in df_codes if x not in list(existing_codes)]
            
            print(f"Missing codes: {len(df_missing)}")
            
            # If not in DB, check prepared JSON File
            df_missing = self.utils.compare_codes_to_existing_jsons(df_missing)
            print(f"Still missing codes: {len(df_missing)}")

            # scrape details of missing esates
            new_estate_details = self.scraper.scrape_specific_estates(df_missing, full_datetime)
            
            return new_estate_details
        
            
            # translate codes to descriptions, add locations, ...
            
            # Add new estates to DB
            
            # Update eixsting estates in DB
            
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