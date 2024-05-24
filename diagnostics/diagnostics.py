import pandas as pd
from typing import Optional
import os
from datetime import datetime
import pandas as pd
from tqdm import tqdm

from config import Config
from db_managment.data_manager import DataManager
from utils.logger import logger

class Diagnostics:
    
    def __init__(self) -> None: 
        self.cf = Config()   
        self.data_manager = DataManager()
        
    def describe_database(self) -> pd.DataFrame:
                
        estate_stats = self.data_manager.get_count_estates()
        unique_estates = estate_stats["unique estate count"].values[0]
        
        price_stats = self.data_manager.get_count_prices()
        price_history_row_count = price_stats["all_rows"].values[0]
        
        estate_stats["price history rows"] = price_history_row_count
        
        logger.info(f'Number of unique rows in estate_detail table: {unique_estates}')
        logger.info(f'Number of rows in price_history table: {price_history_row_count}')
    
        return estate_stats
    
    def discounts_in_last_batch(self,
                                   filters={
                                   "category_type_cb": ["1"], # prodej
                                   "category_main_cb": ["1", "2"], # byt, dům
                                   "region": ["Hlavní město Praha", "Praha", "Střední Čechy"]
                                   }) -> dict:
        
        logger.info(f'Preparing summary of the last batch.')

        result = {}
        df = self.data_manager.get_all_rows("estate_detail")
        
        #TODO: není to principiálně špatně? koukám na nejnovější dva datumy u ÚPLNĚ NOVÝCH budov.
        #todo já chci dvoje nejnovější pozorování cen. to že se to skoro vždy potká není úplně safe enough.
        timestamps = df["crawled_at"].unique()
        timestamp1 = max(timestamps) # the newest timestamp
        timestamp2 = max([x for x in timestamps if x != timestamp1])
        result["Last Date"], result["Previous Date"] = timestamp1, timestamp2
        
        result["New estates observed"] = len(df[df['crawled_at'] == timestamp1])
        
        if filters:
            logger.info(f'Applying filters of interest on Discount summary - estates')
            df = self.filter_of_interest(df, filters)
            if len(df) == 0: return result
                    
        #? df1 je poslední timestamp, df2 je předposlední timestamp
        df1 = self.data_manager.get_all_rows_from_date("price_history", timestamp1)
        df2 = self.data_manager.get_all_rows_from_date("price_history", timestamp2)
        
        count_new_estates = df1[~df1['estate_id'].isin(df2['estate_id'])]['estate_id'].count()
        count_lost_estates = df2[~df2['estate_id'].isin(df1['estate_id'])]['estate_id'].count()

        #TODO: check jestli tyto jsou doplněk k New a lost nějak.
        merged_df = df1.merge(df2, on='estate_id', suffixes=('_df1', '_df2'), how='inner')
        
        # Adding URL from estate_detail. 
        merged_df = merged_df.merge(df, on="estate_id", how="left")
        
        merged_df = merged_df[["estate_id", "price_df1", "price_df2", "estate_url"]]
        merged_df["price_growth_perc"] = round(((merged_df["price_df1"] - merged_df["price_df2"]) / merged_df["price_df2"]) * 100, 2)
        
        estates_same_price = merged_df[merged_df['price_df1'] == merged_df['price_df2']]
        estates_higher_price = merged_df[merged_df['price_df1'] > merged_df['price_df2']]
        #TODO: price = 1 je special case kdy skryli cenu - taktéž chci někam reportovat
        estates_lower_price = merged_df[(merged_df['price_df1'] < merged_df['price_df2']) & (merged_df['price_df1'] > 1)]

        result["New estates compared previous timestamp"] = int(count_new_estates)
        result["Lost estates compared previous timestamp"] = int(count_lost_estates)
        result["Estates with same price"] = int(estates_same_price["estate_id"].count())
        result["Estates that are more expensive"] = int(estates_higher_price['estate_id'].count())
        result["Estates that are cheaper"] = int(estates_lower_price['estate_id'].count())
        
        #TODO: sort by price_growth_perc from lowest (negative growth = discount), add URLs
        
        estate = estates_lower_price[["price_growth_perc", "estate_url"]].sort_values(by="price_growth_perc", ascending=True)
        estate["estate_url_combined"] = estate["price_growth_perc"].astype(str) + "% = " + estate["estate_url"].astype(str)
        result["url"] = estate["estate_url_combined"].to_list()
        
        return result         
    
    def filter_of_interest(self, df, filters) -> pd.DataFrame:
        for key, value in filters.items():
            try:
                df = df[df[key].isin(value)]
            except Exception as e:
                logger.error(f'Filtering failed on {key}:{value} as {e}')
                
        return df               
    
    def compare_new_prices(self) -> pd.DataFrame:
        raise NotImplementedError
    
    def generate_report(self) -> pd.DataFrame:
        raise NotImplementedError