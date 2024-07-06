import requests
from bs4 import BeautifulSoup
import time
import json
import asyncio
import aiohttp
import json
import time
import logging
import mysql.connector
#logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
'''
logging.basicConfig(
    filename='scrapurl.log',  # Specify the filename for the log file
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.DEBUG  # Set the logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
)'''
logger = logging.getLogger('my_logger')
logger.setLevel(logging.DEBUG)

# Create a formatter
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

# Create a StreamHandler for console output (stdout)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)
# Create a FileHandler for file output
file_handler = logging.FileHandler('scrapurl.log')
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)

# Add the handlers to the logger
logger.addHandler(console_handler)
logger.addHandler(file_handler)
success_time = []
error_time = []

with open('db_config.json', 'r') as config_file:
    config = json.load(config_file)

def to_comment(chat,entry):
        try:
            return [
                chat['post_id'],
                entry['root_comment'],
                entry['parent_id'],
                entry['depth'],
                entry['id'],
                entry['user_id'],
                entry['time'],
                entry['replies_count'],
                entry['rank'].get('ranks_up', 0),
                entry['rank'].get('ranks_down', 0),
                entry['rank_score'],
                entry['content'][-1]['text'],
                entry['user_reputation'],
                entry['best_score']
            ]
        except KeyError as e:
            logger.debug(f"KeyError in to_comment: {e}")
            return None
        except IndexError as e:
            logger.debug(f"IndexError in to_comment: {e}")
            return None
def get_replies(chat,ids,comment):
        replies = []
        try:
            if comment['id'] not in ids:
                ids.add(comment['id'])
                comment_data = to_comment(chat,comment)
                if comment_data:
                    replies.append(comment_data)

            for reply in comment['replies']:
                reply_data = get_replies(chat,ids,reply)
                if reply_data:
                    replies += reply_data
        except KeyError as e:
            logger.debug(f"KeyError in get_replies: {e}")
        except Exception as e:
            logger.debug(f"Unhandled exception in get_replies: {e}")

        return replies
async def make_request(url, headers, json_data, request_count):
    logger.debug(request_count)
    #start_time = time.time()

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=json_data, timeout=0.8) as response:
                #end_time = time.time()
                #elapsed_time = end_time - start_time

                if response.status == 200:
                    #success_time.append(elapsed_time)
                    return await response.json()
                elif response.status == 104:
                    logger.debug("Connection reset by peer. Waiting for a second before retrying...")
                    await asyncio.sleep(1)
                    return await make_request(url, headers, json_data, request_count)  # Retry
                else:
                    logger.debug(f"Error in request. Status code: {response.status}")
                    return None

    except Exception as e:
        logger.debug(f"Error making request: {e}")
        #end_time = time.time()
        #elapsed_time = end_time - start_time
        #error_time.append(elapsed_time)
        # Potential infinite loop fix later (e.g., with retries limit)
        await asyncio.sleep(.5)
        return await make_request(url, headers, json_data, request_count)
async def async_get(url, timeout=5,sleep =1):
    async with aiohttp.ClientSession() as session:
        for _ in range(5):
            try:
                async with session.get(url, timeout=timeout) as response:
                    return await response.text()
            except Exception as e:
                pass
            await asyncio.sleep(sleep)
    #POTENTIAL TO BE AN INFINITE LOOP
    return async_get(url,sleep=sleep*2)


async def scrape_url(url,semaphore,search = ""):
    async with semaphore:
        chat = {}
        logger.info(f"Scrapping url: %s" % url)
        try:
            html = await async_get(url)
            if html:
                soup = BeautifulSoup(html, 'html.parser')
                address_key = soup.head.find(attrs={"name": "dc.identifier"})['content']
                title = soup.head.find(attrs={"name": "dc.title"})['content']
                author = soup.head.find(attrs={"name": "dc.creator"})['content']
                date = soup.head.find(attrs={"name": "dc.date"})['content']
            else:
                logger.debug("NO HTML!!")
        except Exception as e:
            logger.debug(f"Error occurred while scraping {url}: {e}")
        headers = {
        'User-Agent': '',
        'x-spot-id': 'sp_ANQXRpqH',
        'x-post-id': address_key
        }

        json_data = {
            'count': 10000,
            'child_count': 1000,
            'offset': 0,
            'depth': 1000,
            'sort_by': 'oldest',
        }

        url = 'https://api-2-0.spot.im/v1.0.0/conversation/read'

        try:
            # Make the initial request
            request_count = 1
            response_json = await make_request(url, headers, json_data, request_count)

            if response_json:
                chat = response_json['conversation']
                logger.info(f"'messages_count': {chat['messages_count']}\n'replies_count': {chat['replies_count']}\n'comments_count': {chat['comments_count']}")

                has_next = chat['has_next']
                offset = chat['offset']
                i = 0

                # Continue making requests until there are no more next pages
                while has_next:
                    i += 1
                    json_data['offset'] = offset
                    request_count += 1
                    response_json = await make_request(url, headers, json_data, request_count)

                    if response_json:
                        new_chat = response_json['conversation']
                        has_next = new_chat['has_next']
                        offset = new_chat['offset']

                        chat['comments'].extend(new_chat['comments'])
                        chat['users'].update(new_chat['users'])
                    else:
                        logger.debug("Error in inner request.")
                        break

        except KeyboardInterrupt:
            logger.debug("Process interrupted.")
        except Exception as e:
            logger.debug("Unhandled exception in scrape url: "+str(e))
            logger.debug(e)
        #print(response_json)
        #print("{" + "\n".join("{!r}: {!r},".format(k, v) for k, v in data.items()) + "}")
        #the 'demopage.asp' prints all HTTP Headers
        #ADD COMMENTS TO DATABASE
        try:
            connection = mysql.connector.connect(**config)
            #print(connection.total_changes)
            cur = connection.cursor()
            connection.start_transaction()
            article = (chat['post_id'],url,title,author,date,address_key,search)
            cur.execute("INSERT OR REPLACE INTO article (id, url,title,author,date,key,search) VALUES (?,?,?,?,?,?,?);",article)
            #connection.commit()

            #print(connection.total_changes)
            cur = connection.cursor()
            chat_list = list(map(lambda x: (x['id'],x['user_name'],x['reputation'].get('received_ranked_up',0),x['reputation'].get('total',0)),chat['users'].values()))
            cur.executemany("INSERT OR REPLACE INTO user (id, user_name, received_ranked_up, total) VALUES (?,?,?,?);",chat_list)
            #ADD comments
            #from collections import Counter
            comments = []
            ids = set()

            for comment in chat['comments']:
                comment_data = get_replies(chat,ids,comment)
                if comment_data:
                    comments += comment_data

            #ids = list(map(lambda x:x[4],comments))
            #print(Counter(ids))
            #print(connection.total_changes)
            #connection.execute("PRAGMA busy_timeout = 30000") 
            cur.executemany("INSERT OR REPLACE INTO comment (article, root_comment, parent_id, depth, id, user_id, time, replies_count, ranks_up, ranks_down, rank_score, content, user_reputation, best_score) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?);",comments)
            cur.execute("UPDATE Articles \
                        SET Scrapped=CURDATE(),Title=?,Author=? \
                        WHERE URL=?;",(title,author,url))
            connection.commit()
            logger.info(f'Comments added: %d' % len(comments)) 
        except mysql.connector.Error as err:
            print(f"Error: {err}")
            connection.rollback()
        finally:
            if connection.is_connected():
                connection.close()
                print("Connection closed")

async def scrape_urls(urls):
    success_times = []
    error_times = []
    batch_size = 300
    tasks = []
    semaphore = asyncio.Semaphore(64)  # Limit to 20 concurrent tasks
    tasks = [scrape_url(url, semaphore) for url in urls]
    await asyncio.gather(*tasks)


# Example usage:



def main():

    urls = ['https://www.foxnews.com/politics/global-elites-took-150-private-jets-fight-climate-change-davos', 'https://www.foxnews.com/politics/biden-says-climate-change-is-bigger-threat-humanity-nuclear-war', 'https://www.foxnews.com/politics/al-gore-history-climate-predictions-statements-proven-false']
    asyncio.run(scrape_urls(urls))

# Print the profiling results
'''
import cProfile
import pstats
from io import StringIO
def speedTest(func):
    profiler = cProfile.Profile()
    profiler.enable()
    func()
    profiler.disable()
    print(len(urls))
    stats = StringIO()
    stats_print = pstats.Stats(profiler, stream=stats).sort_stats('cumulative')
    stats_print.print_stats()
    logger.info(stats.getvalue())'''
if __name__ == "__main__":
    main()
    #speedTest(main())


'''
import statistics
def array_summary_statistics(data):
    """
    Compute summary statistics for an array of numbers using the statistics module.

    Parameters:
    data (list or tuple): Input array of numbers.

    Returns:
    dict: Dictionary containing summary statistics.
    """
    # Calculate summary statistics
    summary_stats = {
        "Mean": statistics.mean(data),
        "Variance": statistics.variance(data),
        "Standard Deviation": statistics.stdev(data),
        "Minimum Value": min(data),
        "Maximum Value": max(data),
        "Median": statistics.median(data),
        "25th Percentile": statistics.quantiles(data, n=4)[0],
        "75th Percentile": statistics.quantiles(data, n=4)[-1]
    }

    return summary_stats'''