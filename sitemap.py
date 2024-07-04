#page 93 and above have comments
import requests
import xml.etree.ElementTree as ET
import logging

logging.basicConfig(filename='sitemap_parser.log', level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')

def parse_sitemap(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        root = ET.fromstring(response.content)
        
        urls = []
        for loc_elem in root.findall('.//{http://www.sitemaps.org/schemas/sitemap/0.9}loc'):
            sitemap_url = loc_elem.text.strip()

            # Check if the URL points to a nested sitemap
            if 'type=articles' in sitemap_url:
                logging.info(f"Checking {sitemap_url}")
                response = requests.get(sitemap_url, timeout=5)
                response.raise_for_status()
                print(sitemap_url)
                subroot = ET.fromstring(response.content)
                iterator = subroot.iter()
                iterator.__next__()
                news_article_url = ""
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
            print(urls)
            urls = []
        return urls

    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching sitemap from {url}: {e}")
        return []
    except ET.ParseError as e:
        logging.error(f"Error parsing XML content from {url}: {e}")
        return []

# Example usage
sitemap_url = 'https://foxnews.com/sitemap.xml'
urls = parse_sitemap(sitemap_url)

if urls:
    logging.info(f"Found {len(urls)} URLs in sitemap {sitemap_url}:")
    for url in urls:
        print(url)
        logging.info(url)
else:
    logging.warning(f"No URLs found in sitemap {sitemap_url}")
