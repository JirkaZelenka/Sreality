from datetime import datetime
from typing import Optional
import os
import pandas as pd

##### GeoPy ############        
from geopy.geocoders import Nominatim   # Geolocator   # pip install geopy  
from geopy.exc import GeocoderTimedOut  # for Error handling

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
    
    def safe_save_csv(self, df: pd.DataFrame, filename: str):

        file_path = f"{self.cf.project_path}/{self.cf.data_folder}/{filename}.csv" 
        
        if os.path.exists(file_path):
            print(f"Name {filename} in {file_path} already exists.")
            filename += "_SAFE"
            file_path = f"{self.cf.project_path}/{self.cf.data_folder}/{filename}.csv" 
            df.to_csv(file_path, sep=";", encoding="utf-8", index=False)
        
        else:
            print(f"Saving {filename} to {file_path}")
            df.to_csv(file_path, sep=";", encoding="utf-8", index=False)
    
    def get_location(self, lat: float, lon: float)-> str:

        geolocator = Nominatim(timeout = 20, user_agent = "JZ_Sreality")   # Pomohlo změnit jméno, proti "Error 403" !!        
        geo_input = ", ".join([str(lat), str(lon)])
        result = str(geolocator.reverse(geo_input))
                
        return  result
    
    def parse_location(self, location: str) -> dict:
        
        items = location.split(",")
        
        if len(items) >= 7 and (items[-5].strip().startswith("okres") or
                                items[-5].strip().startswith("obvod")):
            oblast = items[-7].strip() if len(items) >= 7 else "-"
            return {
                "oblast": oblast,
                "město": items[-6].strip(),
                "okres": items[-5].strip(),
                "kraj": items[-4].strip()
            }
        elif len(items) >= 6 and (items[-4].strip().startswith("okres") or
                                items[-4].strip().startswith("obvod")):
            return {
                "oblast": "-" if len(items) < 7 else items[-6].strip(),
                "město": items[-5].strip(),
                "okres": items[-4].strip(),
                "kraj": items[-3].strip()
            }
        else:
            return {
                "oblast": "-",
                "město": "-",
                "okres": "-",
                "kraj": "-"
            }
