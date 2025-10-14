import pandas as pd
from bs4 import BeautifulSoup as bs
import requests
from io import StringIO

url = "https://www.wikiwand.com/en/articles/List_of_international_airports_by_country"

page =requests.get(url)

soup = bs(page.content, "lxml")

bs_continent =  soup.find_all('h2')

df_all = pd.DataFrame(columns=['continent','subcontinent','country','Location','Aiport','IATA Code','ICAO Code','State/Union Territory'])


for item in bs_continent:

    continent = item.text
    
    bs_continent_children = soup.find('h2', id = continent)

    
    if bs_continent_children is not None:
        bs_continent_children =  soup.find('h2', id = continent).find_next('section').find_all("h3")
             
        for item in bs_continent_children: 
            
            subcontinent =    item.text            
            
            bs_subcontinent_children = soup.find('h2', id = continent).find_next('section').find('h3', string = subcontinent)
            
            if bs_subcontinent_children is not None:
                bs_subcontinent_children =  soup.find('h2', id = continent).find_next('section').find('h3', string = subcontinent).find_previous('section').find_all("h4")
        
           
                for item in bs_subcontinent_children: 
                        country = item.text
                        print(continent + " - " + subcontinent + " - " + country)
                        
                        bs_country_children =  soup.find('h2', id = continent).find_next('section').find('h3', string = subcontinent).find_previous('section').find("h4", string = country)
                        #print(bs_country_children)
                                               
                        if bs_country_children is not None:
                            bs_country_children =  soup.find('h2', id = continent).find_next('section').find('h3', string = subcontinent).find_previous('section').find("h4", string = country).find_next('table').find_previous().contents
                                                     
                            df_country = pd.read_html(StringIO(str(bs_country_children)))[0]
                            df_country['continent'] = continent
                            df_country['subcontinent'] = subcontinent
                            df_country['country'] = country
                            
                            df_all = pd.concat([df_all, df_country],ignore_index=True)
                            
                            

df_all = df_all.reset_index()
df_all.to_csv("airport_list.csv", index=0)                        
                              
