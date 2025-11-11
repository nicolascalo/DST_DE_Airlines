# Nicolas CALO
# 
# Script to data mine the https://edi-gla.co.uk website that catalogs flight routes
# 
# 



import pandas as pd
from bs4 import BeautifulSoup as bs
from io import StringIO
from getpass import getpass
import os
import mechanicalsoup
import time

### Making new directories

if not os.path.isdir("edi_gla_flight_routes"):
    os.mkdir("edi_gla_flight_routes")

if not os.path.isdir("edi_gla_flight_routes/pages"):
    os.mkdir("edi_gla_flight_routes/pages")



### Initializing the virtual browser


browser = mechanicalsoup.StatefulBrowser(
    soup_config={'features':'lxml'},
    raise_on_404=True,
    user_agent='MyBot/0.1: mysite.example.com/bot_info'
)


### Login to the web site

browser.open("https://edi-gla.co.uk/user/login")
browser.select_form()
browser["LoginForm[username]"] = "nicolas.calo@yahoo.fr" # Your user id
browser["LoginForm[password]"] = "Wat^B%5p8eg9Z^Q@" # Your login password
resp = browser.submit_selected()


# verify we are now logged in

#browser.open("https://edi-gla.co.uk/flightplan/search")
#browser["Flightplan[search_date_from]"] = "2025-10-01"
#browser["Flightplan[search_date_to]"] = "2025-10-05"
#resp = browser.submit_selected()


page_nb = 1
response_ok = True
while response_ok:
    
    while os.path.isfile("edi_gla_flight_routes/pages/edi_gla_flight_routes_20250401to20251013_"+str(page_nb) + ".csv"):
        page_nb = page_nb + 1


    url_base = "https://edi-gla.co.uk/flightplan/search?Flightplan[callsign]=&Flightplan[aircraft_icao]=&Flightplan[dep]=&Flightplan[dest]=&Flightplan[search_flight_time]=&Flightplan[search_contributor_username]=&Flightplan[remarks]=&Flightplan[search_date_from]=2025-04-01&Flightplan[search_date_to]=2025-10-14&Flightplan[airac_cycle_validated]=&Flightplan[search_sort_field]=fpl_id&Flightplan[search_sort_order]=3&per-page=40&page="+str(page_nb)

    response = browser.open(url_base)


    response_ok = response.__bool__()
    df = pd.read_html(StringIO(str(browser.page)))[0]
    print(f"{page_nb = }")
    df.to_csv("edi_gla_flight_routes/pages/edi_gla_flight_routes_20250401to20251013_"+str(page_nb) + ".csv", index=0)   
    time.sleep(0.5)
    

df_all = pd.DataFrame()

for file in os.listdir("edi_gla_flight_routes/"):
    print("Processing file "+ file)
    df=pd.read_csv("edi_gla_flight_routes/"+file)
    df_all = pd.concat([df_all, df],ignore_index=True)
    

df_all.drop_duplicates().to_csv("edi_gla_flight_routes/edi_gla_flight_routes_20250401to20251013.csv", index=0)  