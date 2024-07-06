#page 93 and above have comments
import requests
import xml.etree.ElementTree as ET
import logging
import json
import mysql.connector



logging.basicConfig(filename='sitemap_parser.log', level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')

def get_sitemaps(url):
    #Get all the sub sitemaps from a main sitemap
    try:
        response = requests.get(url)
        response.raise_for_status()
        root = ET.fromstring(response.content)
        raw_urls = root.findall('.//{http://www.sitemaps.org/schemas/sitemap/0.9}loc')
        #get text
        urls = map(lambda x:x.text.strip(),raw_urls)
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching sitemap from {url}: {e}")
        return []
    except ET.ParseError as e:
        logging.error(f"Error parsing XML content from {url}: {e}")
    return urls

def parse_sitemap(url):
    logging.info(f"Checking {url}")
    response = requests.get(url, timeout=5)
    response.raise_for_status()
    subroot = ET.fromstring(response.content)
    iterator = subroot.iter()
    iterator.__next__()
    news_article_url = ""
    urls = []
    while True:
        try:
            # Retrieve the next item
            iterator.__next__()
            news_article_url = iterator.__next__().text
            last_mod = iterator.__next__().text
            changefreq = iterator.__next__().text
            priority = iterator.__next__().text
            newsType,sep,name = news_article_url[24:].partition('/')
            urls.append((news_article_url,newsType,last_mod))
        except StopIteration:
            break
        except IndexError:
            logging.error(f"Error parsing url to get news type for {news_article_url}")
    '''
    else:
        # The loop's code block goes here...
        print(item)
    for loc_elem in subroot.iterfind('url'):
        news_article_u+
        rl = loc_elem.text
        urls.append(news_article_url)'''
    return urls

if __name__ == "__main__":
    # Load database configuration from file
    with open('db_config.json', 'r') as config_file:
        config = json.load(config_file)

    # Insert Statement
    insert_st = """
	INSERT INTO Articles (url, category, last_mod, title, author,scrapped) 
	VALUES (%s, %s, %s, NULL, NULL,NULL     ) 
	ON DUPLICATE KEY UPDATE 
	Category=VALUES(Category), LastMod=VALUES(LastMod)
	"""
     # Connect to MySQL database
    try:
        connection = mysql.connector.connect(
            host=config['host'],
            user=config['user'],
            password=config['password'],
            database=config['database']
        )

        if connection.is_connected():
            print("Connected to the database")
            # creating cursor object
            cursor = connection.cursor()  
            # Scrape URLS
            sitemap_url = 'https://foxnews.com/sitemap.xml'
            for url in get_sitemaps(sitemap_url):
                if "type=articles" in url:
                    print(url)
                    urls = parse_sitemap(url)
                    cursor.executemany(insert_st, urls)
                    connection.commit()


    except mysql.connector.Error as err:
        print(f"Error: {err}")
    finally:
        if connection.is_connected():
            connection.close()
            print("Connection closed")
    sitemap_url = 'https://foxnews.com/sitemap.xml'
    
