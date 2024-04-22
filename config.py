from dotenv import load_dotenv
import os

class Config:
    
    def __init__(self):
        load_dotenv()
        
        self.project_path = os.getenv("project_path") 
        self.data_folder = os.getenv("data_folder")  
        self.db_name = os.getenv("db_name") 
        
        # category_type_cb
        self.type_of_deal={"1": "prodej",
                        "2": "pronájem",
                        "3": "dražba",
                        "4": "prodej podílu",
                        }
        
        # category_main_cb
        self.type_of_building={"1": "byt",
                            "2": "dům",
                            "3": "pozemek",
                            "4": "komerční nemovitost a nebytový prostor",
                            "5": "ostatní",
                            }

        # category_sub_cb
        self.type_of_rooms={"1" : "N/A",
                        "2": "1+kk",
                        "3": "1+1",
                        "4": "2+kk",
                        "5": "2+1",
                        "6": "3+kk",
                        "7": "3+1",
                        "8": "4+kk",
                        "9": "4+1",
                        "10": "5+kk",
                        "11": "5+1",
                        "12": "6 pokoju a vic",
                        "16": "atypické",
                        "19": "stavební parcela",
                        "23": "zahrada" ,
                        "33": "chata" ,
                        "37": "rodinný" ,
                        "39": "vila",
                        "47": "pronájem pokoje"
                        }
                
        self.table_definitions={
            "estate_detail": 
                    """ 
                    CREATE TABLE IF NOT EXISTS estate_detail (
                    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                    code VARCHAR(255) NOT NULL,
                    description VARCHAR(255) NOT NULL,
                    meta_description VARCHAR(255) NOT NULL,
                    category_main_cb INTEGER NOT NULL,
                    category_type_cb INTEGER NOT NULL,
                    category_sub_cb INTEGER NOT NULL,
                    broker_id INTEGER NOT NULL,
                    broker_company VARCHAR(255) NOT NULL,
                    furnished INTEGER NOT NULL,
                    latitude FLOAT NOT NULL,
                    longitude FLOAT NOT NULL,
                    locality VARCHAR(255) NOT NULL,
                    city VARCHAR(255) NOT NULL,
                    district VARCHAR(255) NOT NULL,
                    region VARCHAR(255) NOT NULL,
                    object_type INTEGER NOT NULL,
                    parking_lots INTEGER NOT NULL,
                    locality_street_id INTEGER NOT NULL,
                    locality_district_id INTEGER NOT NULL,
                    locality_ward_id INTEGER NOT NULL,
                    locality_region_id INTEGER NOT NULL,
                    locality_quarter_id INTEGER NOT NULL,
                    locality_municipality_id INTEGER NOT NULL,
                    locality_country_id INTEGER NOT NULL,
                    terrace INTEGER NOT NULL,
                    balcony INTEGER NOT NULL,
                    loggia INTEGER NOT NULL,
                    basin INTEGER NOT NULL,
                    cellar INTEGER NOT NULL,
                    building_type INTEGER NOT NULL,
                    object_kind INTEGER NOT NULL,
                    ownership VARCHAR(255) NOT NULL,
                    low_energy INTEGER NOT NULL,
                    easy_access INTEGER NOT NULL,
                    building_condition INTEGER NOT NULL,
                    garage INTEGER NOT NULL,
                    room_count_cb INTEGER NOT NULL,
                    energy_efficiency_rating_cb INTEGER NOT NULL,
                    note_about_price VARCHAR(255) NOT NULL,
                    id_of_order INTEGER NOT NULL,
                    last_update VARCHAR(255) NOT NULL,
                    material VARCHAR(255) NOT NULL,
                    age_of_building VARCHAR(255) NOT NULL,
                    ownership_type VARCHAR(255)  NOT NULL,
                    floor VARCHAR(255) NOT NULL,
                    usable_area INTEGER NOT NULL,
                    floor_area INTEGER NOT NULL,
                    energy_efficiency_rating VARCHAR(255) NOT NULL,
                    no_barriers VARCHAR(255) NOT NULL,
                    start_of_offer VARCHAR(255) NOT NULL,
                    created_at datetime NOT NULL
                    ); 
                    """,
            "scraped_prices": 
                    """ 
                    CREATE TABLE IF NOT EXISTS scraped_prices (
                    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                    estate_id INTEGER NOT NULL,
                    price INTEGER NOT NULL,
                    crawled_at datetime NOT NULL,
                    FOREIGN KEY (estate_id) REFERENCES estate_detail (id),
                    ); 
                    """                
        }