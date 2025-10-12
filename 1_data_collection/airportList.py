import pandas as pd

url = "https://www.wikiwand.com/en/articles/List_of_international_airports_by_country"
urlread = pd.read_html(url)

for i,card in enumerate(urlread) :
    print(urlread[i])
