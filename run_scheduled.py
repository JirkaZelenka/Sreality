
import os
os.chdir("c:\\Users\\jirka\\Documents\\MyProjects\\Sreality")

from run import Runner
runner = Runner()

if __name__ == "__main__":
    runner.run_complete_scraping(scrape_prodej_byty=True, 
                                scrape_all=True
                                )