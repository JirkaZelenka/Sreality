from dotenv import load_dotenv
import os

class Config:
    
    def __init__(self):
        load_dotenv()
        
        self.project_path = os.getenv("project_path") 
        self.data_folder = os.getenv("data_folder")  
        self.db_name = os.getenv("db_name") 
        
        
        # category_main_cb
        self.type_of_building={"1": "byt",
                            "2": "dům",
                            "3": "pozemek",
                            "4": "komerční nemovitost a nebytový prostor",
                            "5": "ostatní"
                            }

        # category_type_cb
        self.type_of_deal={"1": "prodej",
                        "2": "pronájem",
                        "3": "dražba"
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
                        "16": "atypický",
                        "19": "stavební parcela",
                        "23": "zahrada" ,
                        "33": "chata" ,
                        "37": "rodinný" ,
                        "39": "vila" }
                
        self.table_definitions={
            "batch_detail": 
                    """ 
                    CREATE TABLE IF NOT EXISTS batch_detail (
                    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                    timestamp datetime NOT NULL,
                    created_at datetime NOT NULL
                    ); 
                    """,
                    
            "estate_detail": 
                    """ 
                    CREATE TABLE IF NOT EXISTS estate_detail (
                    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                    code VARCHAR(255) NOT NULL,
                    name VARCHAR(255) NOT NULL,
                    category_main_cb INTEGER NOT NULL,
                    category_type_cb INTEGER NOT NULL,
                    category_sub_cb INTEGER NOT NULL,
                    type_of_building VARCHAR(255) NOT NULL,
                    type_of_deal VARCHAR(255) NOT NULL,
                    type_of_rooms VARCHAR(255) NOT NULL,
                    size_meters FLOAT NOT NULL,
                    locality VARCHAR(255) NOT NULL,
                    region VARCHAR(255) NOT NULL,
                    district VARCHAR(255) NOT NULL,
                    latitude FLOAT NOT NULL,
                    longitude FLOAT NOT NULL,
                    first_update datetime NOT NULL,
                    last_update datetime NOT NULL,
                    first_price INTEGER NOT NULL,
                    last_price INTEGER NOT NULL,
                    max_price INTEGER NOT NULL,
                    min_price INTEGER NOT NULL,
                    created_at datetime NOT NULL
                    ); 
                    """,
            "scraped_prices": 
                    """ 
                    CREATE TABLE IF NOT EXISTS scraped_prices (
                    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                    estate_id INTEGER NOT NULL,
                    batch_id INTEGER NOT NULL,
                    price INTEGER NOT NULL,
                    created_at datetime NOT NULL,
                    FOREIGN KEY (estate_id) REFERENCES estate_detail (id),
                    FOREIGN KEY (batch_id) REFERENCES batch_detail (id)
                    ); 
                    """                
        }