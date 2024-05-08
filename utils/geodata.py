import os
import json
import pandas as pd
from tqdm import tqdm

from config import Config

from utils.logger import logger

from geopy.geocoders import Nominatim   # Geolocator   # pip install geopy  
from geopy.exc import GeocoderTimedOut  # for Error handling

    
class GeoData:
    def __init__(self) -> None: 
        self.cf = Config()  
    
    def get_location(self, lat: float, lon: float)-> str:
        try:
            geolocator = Nominatim(timeout = 20, user_agent = "JZ_Sreality")   # Pomohlo změnit jméno, proti "Error 403" !!        
            geo_input = ", ".join([str(lat), str(lon)])
            return str(geolocator.reverse(geo_input))           
        except:
            return "-, -, -, -, -, - ,-"
    
    def parse_location(self, location: str) -> dict: 
        try:
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
        except:
            return {
                "oblast": "-",
                "město": "-",
                "okres": "-",
                "kraj": "-"
            }
     
    def assign_location_to_df(self, df: pd.DataFrame) -> pd.DataFrame:
        
        #TODO: toto mi failuje, protože to je apply, tak nevidím kde přesně. bylo to na souboru kde byl jediný estate a ten neměl GPS. pak vůbec neexistuje sloupec, a není to ani jako None, je to horší)
        #? nice way how to use tqdm for apply operations ! (progress_apply instead of apply)
        tqdm.pandas(desc="Processing Locations", total=len(df))
        df["locality_to_parse"] = df.progress_apply(lambda row: self.get_location(row["locality_gps_lat"], row["locality_gps_lon"]), axis=1)
        
        df[['locality', 'city', 'district', 'region']] = df['locality_to_parse'].apply(lambda x: pd.Series(self.parse_location(x)))

        return df
    
    def enrich_jsons_with_geodata(self, list_of_jsons: None) -> pd.DataFrame:   
        """
        Works with list of JSONs names, if not provided, process all files in the folder.
        Files that are already translated in target folder are skipped.
        For each file, creates new json with geolocation data.
        """
        
        source_folder= f"{self.cf.project_path}/{self.cf.data_folder}/{self.cf.estate_details_folder}"
        files = os.listdir(source_folder) if not list_of_jsons else list_of_jsons
        target_folder = f"{self.cf.project_path}/{self.cf.data_folder}/{self.cf.geo_locations_folder}"
        
        print("ENRICHING geolocation data.")
        for file_name in tqdm(files):        
            source_path = os.path.join(source_folder, file_name)
            save_file_name = f"geodata_{file_name}"
            target_path = os.path.join(target_folder, save_file_name)
            
            if os.path.exists(target_path):
                continue

            with open(source_path, 'r') as file:
                data = json.load(file)
                df = pd.DataFrame(data)

            enriched_df = self.assign_location_to_df(df)
            
            data_to_save = enriched_df[["locality_gps_lat", "locality_gps_lon",
                                        "locality", "city", "district", "region"
                                        ]].to_dict(orient='records')
            
            with open(target_path, 'w') as json_file:
                json.dump(data_to_save, json_file, indent=4)
    
        