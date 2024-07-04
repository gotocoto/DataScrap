from datetime import datetime, timedelta
import sqlite3
import requests
from bs4 import BeautifulSoup
import json
def print_days_in_year(year):
    start_date = datetime(year, 1, 1)
    end_date = datetime(year, 1, 31)
    total = 0
    current_date = start_date

    while current_date <= end_date:
        day = current_date.strftime("%Y%m%d")
        nextday = (current_date+timedelta(days=1)).strftime("%Y%m%d")
        current_date += timedelta(days=1)
        getRequest = f'https://api.foxnews.com/search/web?q=climate -num:20 -filetype:amp -filetype:xml more:pagemap:metatags-prism.section more:pagemap:metatags-pagetype:article more:pagemap:metatags-dc.type:Text.Article&siteSearch=foxnews.com&siteSearchFilter=i&sort=date:r:{day}:{nextday}'
        response = requests.get(
                getRequest,
        ).json()
        total+=int(response["searchInformation"]["totalResults"])
        print(f"Total Results: %s" % response["searchInformation"]["totalResults"])
    print(total)
#print_days_in_year(2022)
month = 1
sum = 0
search = "climate"
websites = []
while month <= 4:
    print(f'Scanning month: {month}')
    index = 1
    while(True):
        getRequest = f'https://api.foxnews.com/search/web?q={search} -num:20 -filetype:amp -filetype:xml more:pagemap:metatags-prism.section more:pagemap:metatags-pagetype:article more:pagemap:metatags-dc.type:Text.Article&siteSearch=foxnews.com/politics&siteSearchFilter=i&sort=date:r:2023{month:02d}01:2023{month:02d}80&start={index}'
        response = requests.get(
                getRequest,
        ).json()
        #print(response)
        #print(response)
        if "items" in response:
            sum+=len(response["items"])
            websites.extend(response["items"])
            print(sum)
        
        if "nextPage" in response["queries"]:
            index = response["queries"]["nextPage"][0]["startIndex"]
        else:
            break
    month += 1
urls = []

for i in range(len(websites)):
    url = websites[i]['link']
    urls.append(url)
print(urls)
