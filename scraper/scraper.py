from datetime import datetime
import pandas as pd
from tqdm import tqdm  
import requests   
import re
from typing import Optional
import os

from config import Config

class SrealityScraper:
    
    def __init__(self) -> None: 
        self.cf = Config()   
    
    def scrape_all_with_filter(self,
                    timestamp: str,
                    category_main_cb: Optional[int] = None, # 1 = Byt
                    category_type_cb: Optional[int] = None, # 1 = Prodej
                    category_sub_cb: Optional[int] = None, # 2 = 1+kk
                    per_page: Optional[int] = 999, # 999 is max to display on one page
                    ) -> pd.DataFrame:  
   

        url_base = "https://www.sreality.cz/api/cs/v2/estates?"
        url_count = f"https://www.sreality.cz/api/cs/v2/estates/count" 
        url_count_prodej_byty = f"https://www.sreality.cz/api/cs/v2/estates/count?category_main_cb=1&category_type_cb=1"
        #TODO: nefunguje jim dobře počet na kombinovaném filtru, např byt + Prodej
        
        # ! critical part to avoid data randomizing from Sreality
        headers = {"User-Agent": "Mozilla/5.0"}

        with requests.Session() as session:
            result =  session.request(method="GET", url=url_count, headers=headers).json()
        print(f"number of ALL estates (Byt, Dům, Pronájem, Prodej): {result['result_size']}")
        
        #TODO: nyní radši beru celkový počet stran, abych vzal určitě všechno. 
        #TODO Příště rozpadnout podle inputů na správnou url/count a správné pages
        pages = (result["result_size"]//per_page) + 1
        print(f"we will scrape max {pages} pages with {per_page} results each")

        with requests.Session() as session:
            result =  session.request(method="GET", url=url_count_prodej_byty, headers=headers).json()
        print(f"number of Prodej - Byty: {result['result_size']}")

        urls = []
        for page in range(1, pages+1):
            url = f"{url_base}"
            if category_main_cb:
                url += f"category_main_cb={category_main_cb}&"
            if category_type_cb:
                url += f"category_type_cb={category_type_cb}&"
            if category_sub_cb:
                url += f"category_sub_cb={category_sub_cb}&"
            
            urls.append(f"{url}per_page={per_page}&page={page}")
        print(urls)

        #TODO: není lepší si dočasně "uložit" těchto 29 stránek celých, a až pak je naloadovat, rozsekat, uložit (E-T-L) a pak zas dropnout? 
        #TODO ...než dělat to: for i in result["_embedded"]["estates"]
        data = []
        for url in tqdm(urls):
            
            with requests.Session() as session:
                result = session.request(method="GET", url=url, headers=headers).json()
                    
            try:
                for i in result["_embedded"]["estates"]:                    
                    d = {}
                    d["code"] = str(i["hash_id"])                    
                    d["price"] = int(i["price"])
                    d["timestamp"] = timestamp
                    data.append(d)
            except:
                continue
                
        return data       
         
    def scrape_specific_estates(self,
                    codes: list[str],
                    timestamp: str,
                    ):  
   
        url_base = f"https://www.sreality.cz/api/cs/v2/estates"
        
        urls = []
        for code in codes:
            urls.append(f"{url_base}/{code}")
        
        # ! critical part to avoid data randomizing from Sreality
        headers = {"User-Agent": "Mozilla/5.0"}
    
        data = []
        for url in tqdm(urls):
            
            with requests.Session() as session:
                r = session.request(method="GET", url=url, headers=headers).json()
                    
            try:
                d = {}
                d["description"] = str(r["text"]["value"])   
                d["meta_description"] = str(r["meta_description"])     
                #############               
                d["category_main_cb"] = int(r["seo"]["category_main_cb"])
                d["category_sub_cb"] = int(r["seo"]["category_sub_cb"])
                d["category_type_cb"] = int(r["seo"]["category_type_cb"])
                #############
                d["broker_id"] = str(r["_embedded"]["seller"]["user_id"])
                d["broker_company"] = str(r["_embedded"]["seller"]["_embedded"]["premise"]["name"])
                #############
                d["furnished"] = int(r["recommendations_data"]["furnished"])
                d["locality_gps_lat"] = float(r["recommendations_data"]["locality_gps_lat"])
                d["locality_gps_lon"] = float(r["recommendations_data"]["locality_gps_lon"])
                d["object_type"] = int(r["recommendations_data"]["object_type"])
                d["parking_lots"] = int(r["recommendations_data"]["parking_lots"])
                d["locality_street_id"] = int(r["recommendations_data"]["locality_street_id"])
                d["locality_district_id"] = int(r["recommendations_data"]["locality_district_id"])
                d["locality_ward_id"] = int(r["recommendations_data"]["locality_ward_id"])
                d["locality_region_id"] = int(r["recommendations_data"]["locality_region_id"])
                d["locality_quarter_id"] = int(r["recommendations_data"]["locality_quarter_id"])
                d["locality_municipality_id"] = int(r["recommendations_data"]["locality_municipality_id"])
                d["locality_country_id"] = int(r["recommendations_data"]["locality_country_id"])
                d["terrace"] = int(r["recommendations_data"]["terrace"])
                d["balcony"] = int(r["recommendations_data"]["balcony"])
                d["loggia"] = int(r["recommendations_data"]["loggia"])
                d["basin"] = int(r["recommendations_data"]["basin"])
                d["cellar"] = int(r["recommendations_data"]["cellar"])
                d["building_type"] = int(r["recommendations_data"]["building_type"])
                d["object_kind"] = int(r["recommendations_data"]["object_kind"])
                d["usable_area"] = int(r["recommendations_data"]["usable_area"])
                d["ownership"] = int(r["recommendations_data"]["ownership"])
                d["low_energy"] = int(r["recommendations_data"]["low_energy"])
                d["easy_access"] = int(r["recommendations_data"]["easy_access"])
                d["building_condition"] = int(r["recommendations_data"]["building_condition"])
                d["garage"] = int(r["recommendations_data"]["garage"])
                d["room_count_cb"] = int(r["recommendations_data"]["room_count_cb"])
                d["energy_efficiency_rating_cb"] = int(r["recommendations_data"]["energy_efficiency_rating_cb"])
                ############# 
                d["timestamp"] = timestamp
                data.append(d)
            except:
                continue
                
        return data       
         
    