import json
from fastapi import FastAPI, BackgroundTasks, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

from run import Runner
from utils.logger import logger_app

#This is not class. how it handles created objects?
app = FastAPI()
runner = Runner()

templates = Jinja2Templates(directory="templates")

scraping_progress = {"progress": 0}

class ScrapeItems(BaseModel):
    items: list[str]

@app.on_event("startup")
def startup_event():
    print("Starting your app.") #to the console
    logger_app.info(f'Starting your app.') #to the dedicated file
        
#? musí tam být ta response_class, a vracet TemplateResponse?
@app.get("/", response_class=HTMLResponse)
async def show_main(request: Request):
    logger_app.info(f'Opening main page.')
    
    log_content = get_logs()
    
    #? removed for loading speed of main page
    #stats = runner.diag.describe_database()
    #stats_dict = stats.to_dict(orient="records")[0]
    
    #? removed to work offline
    #market_stats = runner.scraper.get_counts_of_categories()
    
    #? logs go automatically to logger.. print goes to console. return can go to the app window.
    return templates.TemplateResponse("main.html", 
                                      {"request": request,
                                       "log_content": log_content,
                                       "db_stats": {}, #stats_dict,
                                       "market_stats": {} #market_stats
                                    })
    
#? Needs GET (and no POST) to be able to go to the URL. otherwise i can use /docs/ and Try to execute
@app.get("/send_email_discounts/")  
def send_mail_with_discounts(): 
    logger_app.info(f'Sending email with discounts. Probably targeted.')
    
    discounts_targeted = runner.diag.discounts_in_last_batch()
    runner.mailing.send_email(subject=f'SREALITY - DISCOUNTS Prodej-Byty-Praha,Středočeský  {discounts_targeted["Last Date"]}',
                                message_text=json.dumps(discounts_targeted))

@app.get("/get_market_stats/")
def get_market_stats():
    logger_app.info(f'Obtaining market stats.')
    try:
        stats = runner.scraper.get_counts_of_categories()
    except Exception as e:
        logger_app.error(f'Error getting market stats: {e}')
    
    return stats

@app.get("/get_db_stats/")
def get_db_stats():
    logger_app.info(f'Obtaining DB stats.')
    
    stats = runner.diag.describe_database()
    stats_dict = stats.to_dict(orient="records")[0]
    return stats_dict

@app.get("/get_logs/")
def get_logs():
    logger_app.info(f'Loading Scraping log file.')
    
    with open("scraping.log", "r", encoding="utf8") as file:
        log_lines = file.readlines()
    log_content = ''.join(log_lines[-50:])
    return log_content


@app.post("/start_scraping/")
async def start_scraping(scrape_items: ScrapeItems, background_tasks: BackgroundTasks):
    background_tasks.add_task(run_scraping, scrape_items.items)
    return JSONResponse({"message": f"Scraping started for {scrape_items}"})

@app.get("/scraping_status/")
async def scraping_status():
    return scraping_progress

async def run_scraping(items):
    total_items = len(items)
    full_datetime, date_to_save = runner.utils.generate_timestamp()
    print(items)
    """
    for idx, item in enumerate(items, start=1):
        runner.scraper.scrape(item)
        time.sleep(1)  # Simulating scraping time
        scraping_progress["progress"] = int((idx / total_items) * 100)
        # Log the progress
        with open("scraping.log", "a", encoding="utf8") as log_file:
            log_file.write(f"Scraped item {item}. Progress: {scraping_progress['progress']}%\n")
    
    # Reset progress after completion
    scraping_progress["progress"] = 0
    """


@app.on_event("shutdown")
def shutdown_event():
    print("Application being shut down successfully.")
    logger_app.info(f'Application being shut down successfully.')