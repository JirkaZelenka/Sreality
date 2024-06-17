# Sreality
#### Web-scraping Project - costs of houses in the Czech Republic on the biggest real-estate web - "Sreality.cz"

### Structure of this project:
- MakeFile
- Requirements: 
- scraper: responsible for obtaining the data, there are few options: ...
- utils: ..., Geodata 
- db_managment: ..

### Good to know about Sreality:
1) There is no official documentation for the API. Instead I used two main sources:
    - https://dspace.cvut.cz/bitstream/handle/10467/103384/F8-BP-2021-Malach-Ondrej-thesis.pdf
    - https://dspace.cvut.cz/bitstream/handle/10467/111141/F8-DP-2023-Drska-Vojtech-thesis.pdf

2) Header is necessary for requests, e.g. {"User-Agent": "Mozilla/5.0"} If you do not use any, data is stil provided by request, BUT it is RANDOMIZED (prices, size of area, GPS) eventhough it might still looks valid, it is probably made up!
3) There is a limitation to number of displayed estates per page = 999 (using "&per_page=999") - enough to get unique id + price, but not all the details, so the second API is needed to obtain details.
4) There is a limitation to display estates in general - 60k, meaning one can get data from url ending with "&per_page=999&page=60", but not "&per_page=999&page=61", neither "&per_page=500&page=121" etc. Therefore it is a good practice to focus on categories one by one, instead of the general request, as there are currently around 95k offers to scrape.
5) There is a way how to redirect to the full url using 'estate_id' a.k.a. 'hash_id'. Full address looks like: https://www.sreality.cz/detail/prodej/komercni/ubytovani/karlstejn-karlstejn-/937702732 but you can be redirected to this one using simplified url: https://www.sreality.cz/detail/x/x/x/x/937702732 where 'x' can be replaced by any value, ID and the structure is what matters.

|       | A    | B    |
|-------------|---  -|------|
| 123         | ✅   | ✅  |
| 456         | ✅   | ❌  |
| 124         | ❌   | ✅  |
| 324         | ❌   | ✅  |
| 225         | ❌   | ✅  |

 xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
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
* 5.5. Diagnostics: new batch summary, price changes as mail
* 6.5. scraping info to provide URL of the estate
* 7.5. rework of estate_id, diagnostics provide URL and percentage change of price
* 10.5. separating scrapers per category to avoid limit of 60k offers
* 15-17.5. started using FastAPI, main.py + main.html
* 19.5. main page formatting statistics + refreshing options 
* 22.5. hacked redirecting or URL - estate_id is all we need
