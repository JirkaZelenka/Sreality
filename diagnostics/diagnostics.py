import pandas as pd
from typing import Optional
import os
from datetime import datetime
import pandas as pd
from tqdm import tqdm
import sqlite3

os.chdir("c:\\Users\\jirka\\Documents\\MyProjects\\Sreality")
from utils.utils import Utilities
from db_managment.data_manager import DataManager
from utils.mailing import EmailService
from config import Config

from utils.logger import logger

class Diagnostics:
    
    def __init__(self) -> None: 
        self.cf = Config()   
        self.utils = Utilities()
        self.data_manager = DataManager()
        self.mailing = EmailService()
        
    def generate_report(self) -> pd.DataFrame:
        return
    
    def count_new_estates(self) -> pd.DataFrame:
        
        result = {}
        df = self.data_manager.get_all_rows("estate_detail")
        
        timestamps = df["crawled_at"].unique()
        timestamp1 = max(timestamps) # the newst timestamp
        timestamp2 = max([x for x in timestamps if x != timestamp1])
        print(timestamp1, timestamp2)
        
        # This should be the new estates coming from the last timestamp
        #TODO: this should be comparable with count_new_estates, but ...
        df_estates_new = df[df['crawled_at'] == timestamp1].copy()
        result["new_estates_added"] = len(df_estates_new)
        
        df1 = self.data_manager.get_all_rows_from_date("price_history", timestamp1)
        df2 = self.data_manager.get_all_rows_from_date("price_history", timestamp2)
        
        # through negative definition:
        count_new_estates = df1[~df1['estate_id'].isin(df2['estate_id'])]['estate_id'].count()
        count_lost_estates = df2[~df2['estate_id'].isin(df1['estate_id'])]['estate_id'].count()

        merged_df = df1.merge(df2, on='estate_id', suffixes=('_df1', '_df2'), how='inner')
        count_estates_same_price = merged_df[merged_df['price_df1'] == merged_df['price_df2']]['estate_id'].count()
        estates_higher_price = merged_df[merged_df['price_df1'] > merged_df['price_df2']]
        estates_lower_price = merged_df[merged_df['price_df1'] < merged_df['price_df2']]

        result["new_estates_vs_previous_timestamp"] = count_new_estates
        result["lost_estates_vs_previous_timestamp"] = count_lost_estates
        result["estates_same_price_vs_previous_timestamp"] = count_estates_same_price
        result["estates_higher_price_vs_previous_timestamp"] = estates_higher_price['estate_id'].count()
        result["estates_lower_price_vs_previous_timestamp"] = estates_lower_price['estate_id'].count()
        result["prices_down"] = estates_lower_price["estate_id"].to_list()
        
        return result
                
    def compare_new_prices(self) -> pd.DataFrame:
        pass