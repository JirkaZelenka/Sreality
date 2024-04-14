from datetime import datetime
import pandas as pd
from tqdm import tqdm  
import requests   
import re
from typing import Optional

class SrealityScraper:
    
    def __init__(self) -> None: 
        pass     

    def run_scraping(self,
                    category_main_cb: Optional[int] = None, # 1 = Byt
                    category_type_cb: Optional[int] = None, # 1 = Prodej
                    category_sub_cb: Optional[int] = None, # 2 = 1+kk
                    per_page: Optional[int] = 999, # 999 is max to display on one page
                    ) -> pd.DataFrame:  
   
   
        current_datetime = datetime.now()
        formatted_datetime = current_datetime.strftime("%Y-%m-%d %H:%M:%S")
        print(formatted_datetime)

        url_base = "https://www.sreality.cz/api/cs/v2/estates?"
        url_count = f"https://www.sreality.cz/api/cs/v2/estates/count" 
        #TODO: nefunguje jim dobře počet na kombinovaném filtru, např byt + Prodej

        with requests.Session() as session:
            result =  session.request(method="GET", url=url_count).json()
        print(f"number of ALL estates (Byt, Dům, Pronájem, Prodej): {result['result_size']}")

        pages = (result["result_size"]//per_page) + 1
        print(f"we will scrape max {pages} pages with {per_page} results each")

        urls = []
        for page in tqdm(range(1, pages+1)):
            #TODO: proč nestáhnout rovnou vše. Je to jen 6x tolik. 95k místo 17k. Zároveň rozdělující parametry jsou vždy v seo sekci
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
        url_lengths = {}
        for url in tqdm(urls):
            
            with requests.Session() as session:
                result =  session.request(method="GET", url=url).json()
        
            estates_per_page = 0
            
            try:
                for i in result["_embedded"]["estates"]:
                    estates_per_page +=1
                    
                    d = {}
                    d["code"] = str(i["hash_id"])
                    d["name"] = i["name"]
                    d["category_main_cb"] = i["seo"]["category_main_cb"]
                    d["category_type_cb"] = i["seo"]["category_type_cb"]
                    d["category_sub_cb"] = i["seo"]["category_sub_cb"]
                                    
                    pattern = r'Prodej bytu\s+(\S+)\s+(\d+)\s+'
                    match = re.search(pattern, i["name"])

                    if match:
                        d["rooms"] = match.group(1)  # Extracts '2+kk'
                        d["size_meters"] = float(match.group(2))    # Extracts '39'
                    else:
                        d["rooms"] = "-"
                        d["size_meters"] = "-"
                        
                    d["locality"] = i["seo"]["locality"]
                    d["latitude"] = i["gps"]["lat"]
                    d["longitude"] = i["gps"]["lon"]
                    
                    d["price"] = int(i["price"])
                    d["timestamp"] = formatted_datetime
                                
                    data.append(d)
            except:
                url_lengths[url] = 0
                continue
                
            url_lengths[url] = estates_per_page
                
        print(len(data))
        return data       
         
 
    