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
    
    def summary_new_estates(self) -> dict:
        
        logger.info(f'Preparing summary of the last batch.')

        result = {}
        df = self.data_manager.get_all_rows("estate_detail")
        
        timestamps = df["crawled_at"].unique()
        timestamp1 = max(timestamps) # the newest timestamp
        timestamp2 = max([x for x in timestamps if x != timestamp1])
        result["Last Date"] = timestamp1
        result["Previous Date"] = timestamp2
        
        result["New estates observed"] = len(df[df['crawled_at'] == timestamp1])
        
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
        
        #TODO: sort by price_growth_perc from lowest (-35 % discount), add URLs
        
        estate = estates_lower_price[["price_growth_perc", "estate_url"]].sort_values(by="price_growth_perc", ascending=True)
        estate["estate_url_combined"] = estate["price_growth_perc"].astype(str) + "% = " + estate["estate_url"].astype(str)
        result["url"] = estate["estate_url_combined"].to_list()
        
        return result
                
    def compare_new_prices(self) -> pd.DataFrame:
        raise NotImplementedError
    
    def generate_report(self) -> pd.DataFrame:
        raise NotImplementedError