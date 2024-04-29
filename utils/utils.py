from datetime import datetime
from typing import Optional
import os
import json
import pandas as pd
from tqdm import tqdm

##### GeoPy ############        
from geopy.geocoders import Nominatim   # Geolocator   # pip install geopy  
from geopy.exc import GeocoderTimedOut  # for Error handling

from config import Config

class Utilities:
    
    def __init__(self) -> None: 
        self.cf = Config()  
        
    @staticmethod
    def generate_timestamp() -> str:
        current_datetime = datetime.now()
        full_datetime = current_datetime.strftime("%Y-%m-%d %H:%M:%S")
        date_to_save = current_datetime.strftime("%Y%m%d_%H%M")
        return full_datetime, date_to_save
    
    def safe_save_csv(self, df: pd.DataFrame, filename: str):

        file_path = f"{self.cf.project_path}/{self.cf.data_folder}/{self.cf.scraped_prices_folder}/{filename}.csv" 
        
        if os.path.exists(file_path):
            print(f"Name {filename} in {file_path} already exists.")
            filename += "_SAFE"
            file_path = f"{self.cf.project_path}/{self.cf.data_folder}/{self.cf.scraped_prices_folder}/{filename}.csv" 
            df.to_csv(file_path, sep=";", encoding="utf-8", index=False)
        
        else:
            print(f"Saving {filename} to {file_path}")
            df.to_csv(file_path, sep=";", encoding="utf-8", index=False)
            
    def translate_type_of_building(self, code_category_main_cb):
        return self.cf.type_of_building[str(code_category_main_cb)]
    
    def translate_type_of_deal(self, code_category_type_cb):
        return self.cf.type_of_deal[str(code_category_type_cb)]
    
    def translate_type_of_rooms(self, code_category_sub_cb):
        return self.cf.type_of_rooms[str(code_category_sub_cb)]
    
    #TODO: musí být robustní, předem očištěné JSONy o uvozovky, špatně escapované znaky, atd.
    def compare_codes_to_existing_jsons(self, codes_not_found: list[str]) -> list[str]:
        """
        Checks if given estate codes exists in any JSON file with details, and returns list of codes that are new.
        """
        
        folder_with_jsons_files= f"{self.cf.project_path}/{self.cf.data_folder}/{self.cf.estate_details_folder}"
        files = os.listdir(folder_with_jsons_files)
        
        print(f"init len of codes not found {len(codes_not_found)}")
        
        for file_name in files:
            file_path = os.path.join(folder_with_jsons_files, file_name)

            with open(file_path, 'r') as file:
                data = json.load(file)
            
            codes_from_json = [str(item['code']) for item in data]
    
            codes_not_found = [x for x in codes_not_found if x not in codes_from_json]
            #print(f"len of codes not found after checking file {file_name}: {len(codes_not_found)}")
            
        print(f"Codes Not found: {len(codes_not_found)}")         
        
        return codes_not_found
    
    def save_progress_json(self, files_with_code, date_to_save):
        file_path = f"{self.cf.project_path}/{self.cf.data_folder}/{self.cf.estate_details_folder}/{date_to_save}.json"

        # Save to JSON
        with open(file_path, 'w') as f:
            json.dump(files_with_code, f, indent=4)  
    
    def prepare_estate_detail_jsons_to_df(self) -> pd.DataFrame:   
        """
        Takes all JSON file with estate_details, and pairs them with corresponding geodata file if existing.
        Returns merged dataframe with additional columns if missing, and fillna.
        """
        
        folder_with_jsons_files= f"{self.cf.project_path}/{self.cf.data_folder}/{self.cf.estate_details_folder}"
        files = os.listdir(folder_with_jsons_files)
        
        dfs = []
        for file_name in tqdm(files):
            file_path = os.path.join(folder_with_jsons_files, file_name)

            with open(file_path, 'r') as file:
                data = json.load(file)
                
            #? add geodata file with corresponding name, if existing    
            try:
                with open(f"{self.cf.project_path}/{self.cf.data_folder}/{self.cf.geo_locations_folder}/geodata_{file_name}", 'r') as file2:
                    data2 = json.load(file2)

                zipped_data = zip(data, data2)
                merged_data = [{**d1, **d2} for d1, d2 in zipped_data]
                    
                df = pd.DataFrame(merged_data)
                dfs.append(df)
            except:
                continue

        final_df = pd.concat(dfs, ignore_index=True, sort=False)
        
        for c in ["note_about_price", "id_of_order", "last_update", "material",
                  "age_of_building", "ownership_type", "floor", "usable_area",
                  "floor_area", "energy_efficiency_rating", "no_barriers", "start_of_offer",
                  ]:
            if c not in final_df.columns:
                final_df[c] = "-"
        
        final_df.fillna("-", inplace=True)
        
        return final_df
    
    def prepare_price_history_csv_to_df(self) -> pd.DataFrame:   
        """
        Takes all CSV file with price_history.
        Returns merged dataframe.
        Compares with existing rows in database.
        """
        
        folder_with_csv_files= f"{self.cf.project_path}/{self.cf.data_folder}/{self.cf.scraped_prices_folder}"
        files = os.listdir(folder_with_csv_files)
        
        dfs = []
        for file_name in tqdm(files):
            file_path = os.path.join(folder_with_csv_files, file_name)

            try:
                with open(file_path, 'r') as file:
                    data = pd.read_csv(file, sep=";", encoding="utf-8")
                    
                #df = pd.concat([df, data], ignore_index=True)
                dfs.append(data)
            except:
                print(f"There was an error in file {file_name}")
                continue
        
        df = pd.concat(dfs, ignore_index=True)

        return df 
    
    def scrape_incomplete_estates(self, df: pd.DataFrame) -> pd.DataFrame:
        raise NotImplementedError
    
    def identify_suspicious_offers(self, df: pd.DataFrame) -> pd.DataFrame:
        raise NotImplementedError
    
    
    
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
        
        #? nice way how to use tqdm for apply operations ! (progress_apply instead of apply)
        tqdm.pandas(desc="Processing Locations", total=len(df))
        df["locality_to_parse"] = df.progress_apply(lambda row: self.get_location(row["locality_gps_lat"], row["locality_gps_lon"]), axis=1)
        
        tqdm.pandas(desc="Processing parsing", total=len(df))
        df[['locality', 'city', 'district', 'region']] = df['locality_to_parse'].progress_apply(lambda x: pd.Series(self.parse_location(x)))

        return df
    
    def enrich_jsons_with_geodata(self, list_of_jsons: None) -> pd.DataFrame:   
        """
        Works with list of JSONs names, if not provided, process all files in the folder.
        Files that are already translated are skipped.
        """
        
        source_folder= f"{self.cf.project_path}/{self.cf.data_folder}/{self.cf.estate_details_folder}"
        files = os.listdir(source_folder) if not list_of_jsons else list_of_jsons
        target_folder = f"{self.cf.project_path}/{self.cf.data_folder}/{self.cf.geo_locations_folder}"
        
        #? for each file in the folder, if it does not exist yet as a 'geodata_file' in the target folder
        for file_name in tqdm(files):        
            source_path = os.path.join(source_folder, file_name)
            save_file_name = f"geodata_{file_name}"
            target_path = os.path.join(target_folder, save_file_name)
            
            if os.path.exists(target_path):
                #print(f"Name {save_file_name} in {target_folder} already exists.")
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
    
        