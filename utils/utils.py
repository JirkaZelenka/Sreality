from datetime import datetime
from typing import Optional
import os

from config import Config

class Utilities:
    
    def __init__(self) -> None: 
        self.cf = Config()  
        
    #TODO: this could go to some general class of utils, so could safe_save
    @staticmethod
    def generate_timestamp() -> str:
        
        current_datetime = datetime.now()
        full_datetime = current_datetime.strftime("%Y-%m-%d %H:%M:%S")
        date_to_save = current_datetime.strftime("%Y%m%d_%H%M")
        return full_datetime, date_to_save
    
    def safe_save_csv(self, df, filename):

        file_path = f"{self.cf.project_path}/{self.cf.data_folder}/{filename}.csv" 
        
        if os.path.exists(file_path):
            print(f"Name {filename} in {file_path} already exists.")
            filename += "_SAFE"
            file_path = f"{self.cf.project_path}/{self.cf.data_folder}/{filename}.csv" 
            df.to_csv(file_path, sep=";", encoding="utf-8", index=False)
        
        else:
            print(f"Saving {filename} to {file_path}")
            df.to_csv(file_path, sep=";", encoding="utf-8", index=False)
            