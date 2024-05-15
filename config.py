from dotenv import load_dotenv
import os

class Config:
    
    def __init__(self):
        load_dotenv()
        
        self.project_path = os.getenv("project_path") 
        self.data_folder = os.getenv("data_folder")  
        self.estate_details_folder = os.getenv("estate_details_folder")
        self.geo_locations_folder = os.getenv("geo_locations_folder")
        self.scraped_prices_folder = os.getenv("scraped_prices_folder")
        self.db_name = os.getenv("db_name") 
        self.mailing_credentials = {"server": os.getenv("server"),
                                    "port": os.getenv("port"),
                                    "sender_email": os.getenv("sender_email"),
                                    "password": os.getenv("password"),
                                    "receiver_email": os.getenv("receiver_email")
                                    }
        
        self.urls_to_scrape = {"all"           : "https://www.sreality.cz/api/cs/v2/estates" 
                            ,"prodej_byty"     : "https://www.sreality.cz/api/cs/v2/estates?category_type_cb=1&category_main_cb=1"
                            ,"prodej_domy"     : "https://www.sreality.cz/api/cs/v2/estates?category_type_cb=1&category_main_cb=2"
                            ,"prodej_pozemky"  : "https://www.sreality.cz/api/cs/v2/estates?category_type_cb=1&category_main_cb=3"
                            ,"prodej_komercni" : "https://www.sreality.cz/api/cs/v2/estates?category_type_cb=1&category_main_cb=4"
                            ,"prodej_ostatni"  : "https://www.sreality.cz/api/cs/v2/estates?category_type_cb=1&category_main_cb=5"
                            ,"pronajem_byty"     : "https://www.sreality.cz/api/cs/v2/estates?category_type_cb=2&category_main_cb=1"
                            ,"pronajem_domy"     : "https://www.sreality.cz/api/cs/v2/estates?category_type_cb=2&category_main_cb=2"
                            ,"pronajem_pozemky"  : "https://www.sreality.cz/api/cs/v2/estates?category_type_cb=2&category_main_cb=3"
                            ,"pronajem_komercni" : "https://www.sreality.cz/api/cs/v2/estates?category_type_cb=2&category_main_cb=4"
                            ,"pronajem_ostatni"  : "https://www.sreality.cz/api/cs/v2/estates?category_type_cb=2&category_main_cb=5"
                            }
        
        # category_type_cb
        self.type_of_deal={"1": "prodej",
                           "2": "pronájem",
                           "3": "dražba",
                           "4": "podíly", # oficially "prodej podílu", but "podily" fots the URL
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
                            "18": "komerční", #
                            "19": "stavební parcela",
                            "20": "pole", #
                            "21": "les", #
                            "22": "louka", #
                            "23": "zahrada",
                            "24": "ostatní pozemky", #
                            "25": "kancelář", #
                            "26": "sklad", #
                            "27": "výrobní hala", #
                            "28": "obchodní prostory",  #
                            "29": "ubytovací zařízení", #
                            "30": "restaurace", #
                            "31": "zemědělský objekt", #
                            "32": "komerční nemovitost", #
                            "33": "chata",
                            "34": "garáž", #
                            "35": "památka", #
                            "36": "specifický typ nemovitosti", #
                            "37": "rodinný",
                            "38": "činžovní dům", #
                            "39": "vila",
                            "40": "projekt na klíč", #
                            "43": "chalupa",
                            "44": "zemědělská usedlost", #
                            "46": "rybník",
                            "47": "pronájem pokoje",
                            "48": "pozemek - eng", #
                            "49": "virtuální kancelář", #
                            "50": "vinný sklep", #
                            "51": "půdní prostor", #
                            "52": "garážové stání", #
                            "53": "mobilheim", #
                            "54": "vícegenerační dům", #
                            "56": "ordinace", #
                            "57": "apartmány" #
                            }
                
        self.table_definitions={
            "estate_detail": 
                    """ 
                    CREATE TABLE IF NOT EXISTS estate_detail (
                    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                    estate_id VARCHAR(255) NOT NULL,
                    description VARCHAR(255) NOT NULL,
                    meta_description VARCHAR(255) NOT NULL,
                    category_main_cb INTEGER NOT NULL,
                    category_type_cb INTEGER NOT NULL,
                    category_sub_cb INTEGER NOT NULL,
                    locality_url VARCHAR(255) NOT NULL,
                    estate_url VARCHAR(255) NOT NULL,
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
                    crawled_at datetime NOT NULL
                    ); 
                    """,
            "price_history": 
                    """ 
                    CREATE TABLE IF NOT EXISTS price_history (
                    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                    estate_id VARCHAR(255) NOT NULL,
                    price INTEGER NOT NULL,
                    crawled_at datetime NOT NULL,
                    FOREIGN KEY (estate_id) REFERENCES estate_detail (id)
                    ); 
                    """                
        }
        