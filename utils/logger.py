import logging

logging.basicConfig(filename='scraping.log', 
                    encoding='utf-8', 
                    level=logging.INFO, 
                    format='%(levelname)s:%(asctime)s:%(name)s: %(message)s %(pathname)s:%(lineno)d', datefmt='%Y-%m-%d %H:%M:%S'
                    )

#logger = logging.getLogger(__name__)
logger_scraping = logging.getLogger("logger_scraping")

#? Second Logger:
logger_app = logging.getLogger('logger_app')
file_handler = logging.FileHandler('app.log', encoding='utf-8')
file_handler.setLevel(logging.INFO)

formatter = logging.Formatter('%(levelname)s:%(asctime)s:%(name)s: %(message)s %(pathname)s:%(lineno)d', datefmt='%Y-%m-%d %H:%M:%S')
file_handler.setFormatter(formatter)

logger_app.addHandler(file_handler)
logger_app.propagate = False

__all__ = ['logger_scraping', 'logger_app']