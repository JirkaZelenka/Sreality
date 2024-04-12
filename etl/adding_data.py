from datetime import datetime
import pandas as pd
from tqdm import tqdm  
import requests   
import re
from typing import Optional

import json
import sqlite3
import os
from dotenv import load_dotenv


class DataAdder:
    
    def __init__(self, df) -> None: 
        self.df = df     
        
        load_dotenv()
        self.db_path = os.getenv("db_path") 

    def load_current_db(self):
        pass
    
    def find_new_estates(self):
        pass
    
    def write_new_estates(self):
        pass
    
    def update_current_estates(self):
        pass
    
    def get_regions_and_districs(self):
        pass
    