# Sreality
#### Web-scraping Project - costs of houses in the Czech Republic on the biggest real-estate web - "Sreality.cz"
## OLD INFO:
* Approximately 12.000 items scraped every week and added to total - "data_prodej_byty_souhrn.xlsx"
* PowerBI file with some interactive visualizations for 29.3.20-1.3.201 - "Vizualizace.pbix"
* Four notebooks: Scraper, Cleaning & Dropping, Visualizaton, and All in one.
* To run scraping, one needs to have an up-to-date chromedriver.exe in the same folder as Jupyter notebook

## NEW INFO:
### Structure of this project:
- MakeFile
- Requirements: 
- scraper: responsible for obtaining the data, there are few options: ...
- utils: ..., Geodata 
- db_managment: ..
#### xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
* 12.3.- 22.4.2020 = First outputs, preparation of the automatized process.
* 24.-26.4. Visualizations in PowerBI
* 27.-28.4. Creating representative .ipynb files with comments
* TO be Done: Many things, mentioned in the files (full automatization, reporting, historical prices via Insidero?, checks, better time estimates, GPS smoothing, better way to handle extreme values, ...)
xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
* spring 2024: started rework of the scraper
* 11.4. extreme speed-up of getting offer details using different API call with filters
* 14-17.4. complete table managment
* 18.4. config with db structures and code translations, Inserting and Updating Estate details
* 19.4. Dealing with randomized data bcs of missing header
* 20.4. Solved translation of GPS into Kraj-Okres-Město-Oblast
* 21.4. creating estate_detail check, individual scraper for missing estate details, change of estate_detail table structure
* 23.4. downloaded details to all current offers, updating this db after each new scraping
* 24.-25.4. applied logging, requirements.in and Makefile
* 26.4. new class for Geodata handling
* 29.4. Preparing and inserting estate_details into DB
* 30.4. Preparing and inserting Price history into DB
* 1.5. Complete run with scraping missing details and updating DB
* 3.5. Logging significantly improved
* 4.5. basic mailing and scheduler of the scraper