import pandas as pd
from tqdm import tqdm  
import requests   
from typing import Optional

from config import Config
from utils.utils import Utilities
from utils.logger import logger_scraping

class SrealityScraper:
    
    def __init__(self) -> None: 
        self.utils = Utilities()
        self.config = Config()
        
    def get_counts_of_categories(self) -> dict:
        #TODO: nepřesunout toto do diagnostics? mám tam i describe DB
        result = {}
        url_base_count = "https://www.sreality.cz/api/cs/v2/estates/count"  
        url_prodej, url_pronajem = "1", "2"
        urls_to_count = {
        "all": f"{url_base_count}",
        "prodej_byty": {
            "total": f"{url_base_count}?category_type_cb={url_prodej}&category_main_cb=1",
            "praha": f"{url_base_count}?category_type_cb={url_prodej}&category_main_cb=1&locality_region_id=10",
            "stredocesky": f"{url_base_count}?category_type_cb={url_prodej}&category_main_cb=1&locality_region_id=11"
        },
        "prodej_domy": {
            "total": f"{url_base_count}?category_type_cb={url_prodej}&category_main_cb=2",
            "praha": f"{url_base_count}?category_type_cb={url_prodej}&category_main_cb=2&locality_region_id=10",
            "stredocesky": f"{url_base_count}?category_type_cb={url_prodej}&category_main_cb=2&locality_region_id=11"
        },
        "prodej_pozemky": f"{url_base_count}?category_type_cb={url_prodej}&category_main_cb=3",
        "prodej_komercni": f"{url_base_count}?category_type_cb={url_prodej}&category_main_cb=4",
        "prodej_ostatni": f"{url_base_count}?category_type_cb={url_prodej}&category_main_cb=5",
        "pronajem_byty": f"{url_base_count}?category_type_cb={url_pronajem}&category_main_cb=1",
        "pronajem_domy": f"{url_base_count}?category_type_cb={url_pronajem}&category_main_cb=2"
        }
        for key, value in urls_to_count.items():
            if isinstance(value, dict):
                result[key] = {}
                for sub_key, sub_value in value.items():
                    response = requests.get(sub_value)
                    result[key][sub_key] = response.json()["result_size"]
            else:
                response = requests.get(value)
                result[key] = response.json()["result_size"]

        return result
    
    def scrape_multiple_sections(self, 
                                combinations: list[str],
                                timestamp: str,
                                per_page: Optional[int] = 999 # 999 is max to show on single page
                                ) -> dict:
        """
        This function processes multiple combinations of the filter
        And serves number of pages necessary to scrape those urls.
        """
        counts = self.get_counts_of_categories()
        urls = self.config.urls_to_scrape
        
        result = {}
        for combo in combinations:
            logger_scraping.info(f'Running scraper for {combo}')
            
            #? this kinda handles dicts of count in dicts
            cnt = counts[f"{combo}"]
            try:
                cnt = cnt["total"]
                num_pages = (cnt//per_page) + 1
            except:
                num_pages = (cnt//per_page) + 1
            
            data = self._scrape_url(urls[combo],
                                    num_pages, 
                                    per_page,
                                    timestamp
                                    )
            result[combo] = data
        
        #TODO: is this ok, to provide dict of dicts? or list is better?
        return result
        
    def _scrape_url(self, url: str,
                    num_pages: int, 
                    per_page: int,
                    timestamp
                    ) -> pd.DataFrame:  

        # ! critical part to avoid data randomizing from Sreality
        headers = {"User-Agent": "Mozilla/5.0"}

        urls = []
        for page in range(1, max(num_pages, 1) +1):
            urls.append(f"{url}&per_page={per_page}&page={page}")

        #TODO: není lepší si dočasně "uložit" těchto 29 stránek celých, a až pak je naloadovat, rozsekat, uložit (E-T-L) a pak zas dropnout? 
        #TODO ...než dělat to: for i in result["_embedded"]["estates"]
        data = []
        print(f"SCRAPING Prices for {url}")
        for url in tqdm(urls):
            
            with requests.Session() as session:
                result = session.request(method="GET", url=url, headers=headers).json()
                    
            try:
                for i in result["_embedded"]["estates"]:                    
                    d = {}
                    d["estate_id"] = str(i["hash_id"])                    
                    d["price"] = int(i["price"])
                    d["timestamp"] = timestamp
                    data.append(d)
            except:
                continue
                
        return data       
         
    def _scrape_specific_estates(self,
                    estate_ids: list[str],
                    timestamp: str,
                    ):  
   
        url_base = f"https://www.sreality.cz/api/cs/v2/estates"            
        
        # ! critical part to avoid data randomizing from Sreality
        headers = {"User-Agent": "Mozilla/5.0"}
    
        data = []
        save_counter = 0
        for estate_id in tqdm(estate_ids):
            
            if save_counter == 500:
                _, date_to_save = self.utils.generate_timestamp()
                self.utils.save_progress_json(data, date_to_save)
                save_counter = 0
            
            url = f"{url_base}/{estate_id}"
            
            with requests.Session() as session:
                r = session.request(method="GET", url=url, headers=headers).json()
                
            d = {}
            try:
                d["estate_id"] = str(r.get("recommendations_data").get("hash_id", estate_id))
                d["description"] = str(r.get("text").get("value"))   
                d["meta_description"] = str(r.get("meta_description"))     
                #############               
                d["category_main_cb"] = int(r.get("seo").get("category_main_cb"))
                d["category_sub_cb"] = int(r.get("seo").get("category_sub_cb"))
                d["category_type_cb"] = int(r.get("seo").get("category_type_cb"))
                d["locality_url"] = str(r.get("seo").get("locality"))
                #############
                d["locality_gps_lat"] = float(r.get("recommendations_data").get("locality_gps_lat"))
                d["locality_gps_lon"] = float(r.get("recommendations_data").get("locality_gps_lon"))
                d["object_type"] = int(r.get("recommendations_data").get("object_type"))
                d["parking_lots"] = int(r.get("recommendations_data").get("parking_lots"))
                d["locality_street_id"] = int(r.get("recommendations_data").get("locality_street_id")) 
                d["locality_district_id"] = int(r.get("recommendations_data").get("locality_district_id"))  
                d["locality_ward_id"] = int(r.get("recommendations_data").get("locality_ward_id")) 
                d["locality_region_id"] = int(r.get("recommendations_data").get("locality_region_id"))  
                d["locality_quarter_id"] = int(r.get("recommendations_data").get("locality_quarter_id"))  
                d["locality_municipality_id"] = int(r.get("recommendations_data").get("locality_municipality_id"))  
                d["locality_country_id"] = int(r.get("recommendations_data").get("locality_country_id"))  
                d["terrace"] = int(r.get("recommendations_data").get("terrace")) 
                d["balcony"] = int(r.get("recommendations_data").get("balcony"))  
                d["loggia"] = int(r.get("recommendations_data").get("loggia"))  
                d["basin"] = int(r.get("recommendations_data").get("basin"))  
                d["elevator"] = int(r.get("recommendations_data").get("elevator"))
                d["estate_area"] = int(r.get("recommendations_data").get("estate_area"))    
                d["cellar"] = int(r.get("recommendations_data").get("cellar"))   
                d["building_type"] = int(r.get("recommendations_data").get("building_type"))  
                d["object_kind"] = int(r.get("recommendations_data").get("object_kind"))   
                d["ownership"] = int(r.get("recommendations_data").get("ownership"))  
                d["low_energy"] = int(r.get("recommendations_data").get("low_energy"))  
                d["easy_access"] = int(r.get("recommendations_data").get("easy_access"))  
                d["building_condition"] = int(r.get("recommendations_data").get("building_condition"))  
                d["garage"] = int(r.get("recommendations_data").get("garage"))   
                d["room_count_cb"] = int(r.get("recommendations_data").get("room_count_cb"))   
                d["energy_efficiency_rating_cb"] = int(r.get("recommendations_data").get("energy_efficiency_rating_cb"))  
                d["furnished"] = int(r.get("recommendations_data").get("furnished"))
                #############
                d["broker_id"] = str(r.get("_embedded").get("seller").get("user_id"))
                d["broker_company"] = str(r.get("_embedded").get("seller").get("_embedded").get("premise").get("name"))
                #############
                d["note_about_price"] = str(r.get("items").get("Poznámka k ceně"))
                d["id_of_order"] = str(r.get("items").get("ID zakázky"))
                d["last_update"] = str(r.get("items").get("Aktualizace"))
                d["material"] = str(r.get("items").get("Stavba"))
                d["age_of_building"] = str(r.get("items").get("Stav objektu"))
                d["ownership_type"] = str(r.get("items").get("Vlastnictví"))
                d["floor"] = str(r.get("items").get("Podlaží"))
                d["usable_area"] = str(r.get("items").get("Užitná plocha"))
                d["floor_area"] = str(r.get("items").get("Plocha podlahová"))
                d["start_of_offer"] = str(r.get("items").get("Datum zahájení prodeje"))
                ############# 
                d["timestamp"] = timestamp
                save_counter += 1
                data.append(d)
                
            except:
                d["estate_id"] = estate_id
                d["timestamp"] = timestamp
                save_counter += 1
                data.append(d)
                continue
                
        _, date_to_save = self.utils.generate_timestamp()    
        self.utils.save_progress_json(data, date_to_save)
                
        return data       
         
    