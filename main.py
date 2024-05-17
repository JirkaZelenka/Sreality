from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from scraper.scraper import SrealityScraper
from utils.mailing import EmailService
from utils.logger import logger

#This is not class. how it handles created objects?
app = FastAPI()

templates = Jinja2Templates(directory="templates")

@app.on_event("startup")
def startup_event():
    print("Starting your app.")
    logger.info(f'Starting your app.')
        
#? musí tam být ta response_class, a vracet TemplateResponse?
@app.get("/", response_class=HTMLResponse)
async def show_main(request: Request):
    with open("scraping.log", "r", encoding="utf8") as file:
        log_content = file.read()
    return templates.TemplateResponse("main.html", 
                                      {"request": request,
                                       "log_content": log_content
                                    })
    
#? Needs GET (and no POST) to be able to go to the URL. otherwise i can use /docs/ and Try to execute
@app.get("/email/")  
def send_mail():   
    emailer = EmailService()
    emailer.send_email(subject="Test", message_text="Testova zprava")


@app.get("/get_stats/")
def get_stats():
    scraper = SrealityScraper()
    #TODO this goes automatically to logger.. print goes to console. return can go to the app window.
    stats = scraper.get_counts_of_categories()
    return stats

@app.on_event("shutdown")
def shutdown_event():
    print("Application being shut down successfully.")
    logger.info(f'Application being shut down successfully.')