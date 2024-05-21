from datetime import datetime
import os
import json
import pandas as pd
from tqdm import tqdm
import unicodedata

from config import Config

from utils.logger import logger

class Utilities:
    
    def __init__(self) -> None: 
        self.cf = Config()  
        
    @staticmethod
    def generate_timestamp() -> str:
        current_datetime = datetime.now()
        full_datetime = current_datetime.strftime("%Y-%m-%d %H:%M:%S")
        date_to_save = current_datetime.strftime("%Y%m%d_%H%M%S")
                
        return full_datetime, date_to_save
    
    def safe_save_csv(self, df: pd.DataFrame, filename: str):

        file_path = f"{self.cf.project_path}/{self.cf.data_folder}/{self.cf.scraped_prices_folder}/{filename}.csv" 
        
        if os.path.exists(file_path):
            logger.info(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}: Name {filename} in {file_path} already exists. Saving as {filename}_SAFE')
            filename += "_SAFE"
            file_path = f"{self.cf.project_path}/{self.cf.data_folder}/{self.cf.scraped_prices_folder}/{filename}.csv" 
            df.to_csv(file_path, sep=";", encoding="utf-8", index=False)
        
        else:
            df.to_csv(file_path, sep=";", encoding="utf-8", index=False)
            
    def translate_type_of_building(self, code_category_main_cb):
        if code_category_main_cb is not None and not pd.isna(code_category_main_cb):
            return self.cf.type_of_building[str(int(code_category_main_cb))]
        else:
            return None
    
    def translate_type_of_deal(self, code_category_type_cb):
        if code_category_type_cb is not None and not pd.isna(code_category_type_cb):
            return self.cf.type_of_deal[str(int(code_category_type_cb))]
        else:
            return None
    
    def translate_type_of_rooms(self, code_category_sub_cb):
        if code_category_sub_cb is not None and not pd.isna(code_category_sub_cb):
            return self.cf.type_of_rooms[str(int(code_category_sub_cb))]
        else:
            return None
    
    def compare_codes_to_existing_jsons(self, codes_not_found: list[str]) -> list[str]:
        """
        Checks if given estate codes exists in any JSON file with details, and returns list of codes that are new.
        """
        folder_with_jsons_files= f"{self.cf.project_path}/{self.cf.data_folder}/{self.cf.estate_details_folder}"
        files = os.listdir(folder_with_jsons_files)
                
        for file_name in files:
            file_path = os.path.join(folder_with_jsons_files, file_name)

            with open(file_path, 'r') as file:
                data = json.load(file)
                
            #TODO: this might backfire, this used to be code before for all
            codes_from_json = [str(item['estate_id']) for item in data]
    
            codes_not_found = [x for x in codes_not_found if x not in codes_from_json]
                    
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
        
        #? first prepare list of DFs, concat after - it's much faster
        dfs = []
        print("PREPARING Estate details JSONs to DF")
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
                  "locality_url" #? unfortunately unavailable for old scraped data, but necessary
                  ]:
            if c not in final_df.columns:
                final_df[c] = "-"
        
        final_df.rename(columns={"code": "estate_id"})
        
        #? detail/pronajem/byt/2+1/bilina-teplicke-predmesti-sidliste-shd/3980883276  
        final_df['category_main_cb_translated'] = final_df['category_main_cb'].apply(self.translate_type_of_building).apply(self.translate_unicode)
        final_df['category_type_cb_translated'] = final_df['category_type_cb'].apply(self.translate_type_of_deal).apply(self.translate_unicode)
        final_df['category_sub_cb_translated'] = final_df['category_sub_cb'].apply(self.translate_type_of_rooms).apply(self.translate_unicode)
        final_df["locality_url"] = final_df["locality_url"].astype(str).apply(self.remove_trailing_dash)
    
        #TODO: fix the issue with locality name with spaces - this replace doesnt work
        final_df["estate_url"] = "https://www.sreality.cz/detail/" + \
                                final_df["category_type_cb_translated"].astype(str) + "/" + \
                                final_df["category_main_cb_translated"].astype(str) + "/" + \
                                final_df["category_sub_cb_translated"].astype(str) + "/" + \
                                final_df["locality_url"].str.replace(' ', '-') + "/" + \
                                final_df["estate_id"].astype(str)
        
        final_df.fillna("-", inplace=True)
        
        return final_df
    
    def prepare_price_history_csv_to_df(self) -> pd.DataFrame:   
        """
        Takes all CSV file with price_history.
        Prepare list of dataframes, and in the end concats them.
        """
        
        folder_with_csv_files= f"{self.cf.project_path}/{self.cf.data_folder}/{self.cf.scraped_prices_folder}"
        files = os.listdir(folder_with_csv_files)
        
        #TODO: check list of existing CSV files previously upload, and do not load them at all)
        """
        with open(f"{self.cf.project_path}/{self.cf.data_folder}/{self.cf.scraped_prices_folder}/price_hisory_loaded.txt", 'r') as f:
            file_names = f.read().splitlines()
        
        
        with open(file_path, 'w') as f:
            for file_name in file_names:
                f.write(file_name + '\n')
        """
        
        dfs = []
        print("PREPARING Price history CSVs to DF")
        for file_name in tqdm(files):
            file_path = os.path.join(folder_with_csv_files, file_name)

            try:
                with open(file_path, 'r') as file:
                    data = pd.read_csv(file, sep=";", encoding="utf-8")
                    
                dfs.append(data)
            except:
                logger.error(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}: There was an error in file {file_name}')
                continue
        
        df = pd.concat(dfs, ignore_index=True)
        df.rename(columns={"code": "estate_id",'timestamp': 'crawled_at'}, inplace=True)

        return df 
    
    #TODO: má to být self? nebo static?
    def translate_unicode(self,text: str) -> str:
        if text is None: return " "
        else: return unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('utf-8')

    def remove_trailing_dash(self, text: str) -> str:
        return text.rstrip('-')
    
    
    
    def scrape_incomplete_estates(self, df: pd.DataFrame) -> pd.DataFrame:
        raise NotImplementedError
    
    def identify_suspicious_offers(self, df: pd.DataFrame) -> pd.DataFrame:
        raise NotImplementedError