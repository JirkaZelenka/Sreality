import logging

logging.basicConfig(filename='scraping.log', 
                    encoding='utf-8', 
                    level=logging.INFO, 
                    format='%(levelname)s:%(asctime)s:%(name)s: %(message)s %(pathname)s:%(lineno)d', datefmt='%Y-%m-%d %H:%M:%S'
                    )

logger = logging.getLogger(__name__)