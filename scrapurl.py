import requests
from bs4 import BeautifulSoup
import time
import orjson
import asyncio
import aiohttp
import json
import time
import logging
import mysql.connector
import traceback
from datetime import datetime
import os
import sys
import aiomysql
import warnings
import cProfile
import pstats
from io import StringIO
from concurrent.futures import ProcessPoolExecutor
log_file_path = 'scrapurl.log'
'''

if os.path.exists(log_file_path):
    # Delete the log file
    os.remove(log_file_path)
    print(f"Deleted existing log file: {log_file_path}")'''
#logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
'''
logging.basicConfig(
    filename='scrapurl.log',  # Specify the filename for the log file
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.DEBUG  # Set the logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
)'''
warnings.filterwarnings("ignore", category=Warning, message="Duplicate entry")
successful_requests = 0
start_time = time.time()
clear_log = False
if '--clear-log' in sys.argv:
    clear_log = True


# Clear log file if clear_log flag is set
if clear_log:
    with open(log_file_path, 'w'):
        pass  # This clears the log file
    print("Log File cleared")

logger = logging.getLogger('my_logger')
logger.setLevel(logging.DEBUG)
TESTING =False
if '--testing' in sys.argv:
    TESTING = True
PROFILING = False
if '--profiling' in sys.argv:
    PROFILING = True
# Create a formatter
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

# Create a StreamHandler for console output (stdout)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)
# Create a FileHandler for file output
file_handler = logging.FileHandler(log_file_path)
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)

# Add the handlers to the logger
logger.addHandler(console_handler)
logger.addHandler(file_handler)

if(TESTING):
    # Create a new logger instance
    table_logger = logging.getLogger('table_logger')
    table_logger.setLevel(logging.INFO)

    # Create a file handler for the logger
    file_handler = logging.FileHandler('request_log.log')
    formatter = logging.Formatter('%(asctime)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    file_handler.setFormatter(formatter)
    table_logger.addHandler(file_handler)
with open('db_config.json', 'r') as config_file:
    config = orjson.load(config_file)

def to_comment(chat,entry):
        try:
            return [
                chat['post_id'][21:],
                entry['root_comment'].rpartition('_')[-1],
                entry['parent_id'].rpartition('_')[-1] if entry['parent_id'] else None,
                entry['depth'],
                entry['id'].rpartition('_')[-1],
                entry['user_id'],
                datetime.fromtimestamp(entry['time']),
                entry['replies_count'],
                entry['rank'].get('ranks_up', 0),
                entry['rank'].get('ranks_down', 0),
                entry['rank_score'],
                entry['content'][0]['text'],
                entry['user_reputation'],
                entry['best_score']
            ]
        except KeyError as e:
            #logger.debug(f"KeyError in to_comment: {e} \n{entry['content']}")
            for data in entry['content']:
                if 'text' in data:
                    return [
                        chat['post_id'][21:],
                        entry['root_comment'].rpartition('_')[-1],
                        entry['parent_id'].rpartition('_')[-1] if entry['parent_id'] else None,
                        entry['depth'],
                        entry['id'].rpartition('_')[-1],
                        entry['user_id'],
                        datetime.fromtimestamp(entry['time']),
                        entry['replies_count'],
                        entry['rank'].get('ranks_up', 0),
                        entry['rank'].get('ranks_down', 0),
                        entry['rank_score'],
                        data['text'],
                        entry['user_reputation'],
                        entry['best_score']
                    ]
            for data in entry['content']:
                if 'url' in data:
                    return [
                        chat['post_id'][21:],
                        entry['root_comment'].rpartition('_')[-1],
                        entry['parent_id'].rpartition('_')[-1] if entry['parent_id'] else None,
                        entry['depth'],
                        entry['id'].rpartition('_')[-1],
                        entry['user_id'],
                        datetime.fromtimestamp(entry['time']),
                        entry['replies_count'],
                        entry['rank'].get('ranks_up', 0),
                        entry['rank'].get('ranks_down', 0),
                        entry['rank_score'],
                        data['url'],
                        entry['user_reputation'],
                        entry['best_score']
                    ]
            logger.debug(f"KeyError in to_comment: {e} \n{entry['content']}")
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
async def make_request(url, headers, json_data,attempt = 1):
    #start_time = time.time()
    #global successful_requests
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=json_data, timeout=1) as response:
                if response.status == 200:
                    #success_time.append(elapsed_time)
                    #successful_requests += 1
                    response_json = await response.read()
                    return orjson.loads(response_json)
                    #return await response.json()
                elif response.status == 403:
                    logger.debug(f"Too many request made. Trying again: Attempt {attempt} ")
                    await asyncio.sleep(attempt)
                    
                elif response.status == 104:
                    logger.debug("Connection reset by peer. Waiting for a second before retrying...")
                    await asyncio.sleep(attempt)
                else:
                    logger.debug(f"Error in request. Status code: {response.status}")
                if(attempt>10):
                    logger.debug(f"Unable to process request, try 10 times and failed")
                    return None
                attempt+=1
                return await make_request(url, headers, json_data,attempt=attempt)  # Retry

    except TimeoutError as e:
        logger.debug(f"Request took to long to respond. Trying again.. {json_data} {headers}")
        await asyncio.sleep(attempt)
        return await make_request(url, headers, json_data,attempt=attempt+1)
async def async_get(url, timeout=3, sleep=1):
    async with aiohttp.ClientSession() as session:
        for _ in range(5):
            try:
                async with session.get(url, timeout=timeout) as response:
                    if response.status == 200:  # Check if response status is OK
                        return await response.text()
                    elif response.status == 403:  # Retry on 403 Forbidden
                        logger.debug(f'Received 403 Forbidden for {url}, retrying...')
                    elif response.status >= 500:  # Retry on server errors
                        logger.debug(f'Server error: {response.reason}')
            except aiohttp.ClientResponseError as cre:
                logger.debug(f'Retry {url} due to ClientResponseError: {cre}')
            except aiohttp.ClientError as ce:
                logger.debug(f'Retry {url} due to ClientError: {ce}')
            except asyncio.TimeoutError:
                logger.debug(f'Retry {url} due to TimeoutError')
            await asyncio.sleep(sleep)
            sleep *=2  # Exponential backoff

    logger.debug(RuntimeError(f'Failed to fetch {url} after multiple attempts'))
    return None
async def scrape_url(url,get_semaphore,save_connection,save_semaphore,search = ""):
    async with get_semaphore:
        chat = {}
        logger.info(f"Scrapping url: %s" % url)
        try:
            html = await async_get(url)
            if html:
                soup = BeautifulSoup(html, 'html.parser')
                address_key = soup.head.find(attrs={"name": "dc.identifier"})['content']
                title = soup.head.find(attrs={"name": "dc.title"})['content']
                author = soup.head.find(attrs={"name": "dc.creator"})['content']
                last_mod = soup.head.find(attrs={"name": "dcterms.modified"})['content']
            else:
                logger.debug("NO HTML!!")
                return None
        except Exception as e:
            logger.debug(f"Error occurred while scraping {url}: {e}")
            return None
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

        api_url = 'https://api-2-0.spot.im/v1.0.0/conversation/read'

        try:
            request_count = 1
            logger.debug(f"Request: {request_count} for {url[24:]}")
            response_json = await make_request(api_url, headers, json_data)

            if not response_json:
                logger.error(f'Error in first comment request. Failed for url {url}')
                return None
            
            chat = response_json['conversation']
            logger.debug(f"'messages_count': {chat['messages_count']}\t'replies_count': {chat['replies_count']}\t'comments_count': {chat['comments_count']}")

            has_next = chat['has_next']
            offset = chat['offset']

            while has_next:
                json_data['offset'] = offset
                request_count += 1
                logger.debug(f"Request: {request_count} for {url[24:]}")
                response_json = await make_request(api_url, headers, json_data)

                if not response_json:
                    logger.error(f"Error in inner request. Failed for url {url}")
                    break

                new_chat = response_json['conversation']
                has_next = new_chat['has_next']
                offset = new_chat['offset']

                chat['comments'].extend(new_chat['comments'])
                chat['users'] = {**chat['users'], **new_chat['users']}

        except KeyboardInterrupt:
            logger.debug("Process interrupted.")
        except Exception as e:
            logger.error(f"Unhandled exception in scrape url: {e}")
            logger.debug(e)
            return None  # Return None to handle the error upstream
        #print(response_json)
        #print("{" + "\n".join("{!r}: {!r},".format(k, v) for k, v in data.items()) + "}")
        #the 'demopage.asp' prints all HTTP Headers
        #ADD COMMENTS TO DATABASE
    update_article = """
                        UPDATE article
                        SET title = %s,
                            author = %s,
                            post_id = %s,
                            last_mod = %s
                        WHERE url = %s;
                    """
    insert_users_ignore = """
                        INSERT IGNORE INTO user (id, user_name, received_ranked_up, total)
                        VALUES (%s, %s, %s, %s);
                    """
    insert_comments_ignore = """
                        INSERT IGNORE INTO comment 
                        (post_id, root_comment, parent_id, depth, id, user_id, time, replies_count, ranks_up, ranks_down, rank_score, content, user_reputation, best_score) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                    """
    comments = []
    ids = set()
    article = (title, author, address_key[15:], last_mod, url)
    users = list(map(lambda x: (x['id'], x['user_name'], x['reputation'].get('received_ranked_up', 0), x['reputation'].get('total', 0)), chat['users'].values()))
    for comment in chat['comments']:
        comment_data = get_replies(chat, ids, comment)
        if comment_data:
            comments.extend(comment_data)
    
    try:
        #async with save_semaphore:
        async with save_connection.acquire() as connection:
            connection = await aiomysql.connect(**config)
            async with connection.cursor() as cur:
                await cur.execute("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")
                await connection.begin()
                await cur.execute(update_article, article)
                await cur.executemany(insert_users_ignore, users)
                logger.debug(f'Users added: {len(users)}')
                await cur.executemany(insert_comments_ignore, comments)
                await connection.commit()
                logger.info(f'Comments added: {len(comments)} \t {url[24:]}')
                global successful_requests
                successful_requests+=len(comments)
    except aiomysql.Error as err:
        logger.error(f"Error: {err} on line {traceback.print_exc()}")
        await connection.rollback()
    except Exception as e:
        logger.error(f"Error: {e}")
        traceback.print_exc()
    finally:
        if 'connection' in locals():
            connection.close()
            logger.debug(f'Connection closed for {url[24:]}')
async def scrape_urls(urls,concurrent_requests):
    #Wrapper that split the urls into many processes to run async
    success_times = []
    error_times = []
    batch_size = 300
    tasks = []
    get_semaphore = asyncio.Semaphore(concurrent_requests)  # Limit to 20 concurrent tasks
    save_semaphore = asyncio.Semaphore(20)
    save_connection = await aiomysql.create_pool(**config)
    tasks = [scrape_url(url, get_semaphore,save_connection,save_semaphore) for url in urls]
    
    if TESTING:
        monitor_task = asyncio.create_task(monitor_requests())
    await asyncio.gather(*tasks)
    save_connection.close()
    await save_connection.wait_closed()
    if TESTING:
        monitor_task.cancel()
def main():
    global successful_requests
    #urls = ['https://www.foxnews.com/politics/global-elites-took-150-private-jets-fight-climate-change-davos']
    #urls = ['https://www.foxnews.com/politics/global-elites-took-150-private-jets-fight-climate-change-davos', 'https://www.foxnews.com/politics/biden-says-climate-change-is-bigger-threat-humanity-nuclear-war', 'https://www.foxnews.com/politics/al-gore-history-climate-predictions-statements-proven-false']
    #urls = ['https://www.foxnews.com/politics/massachusetts-gov-healey-unveils-climate-blueprint-coastal-communities']
    
    # Establish connection to MySQL
    count = 6
    default_async = 9
    old_urls_set = set()
    query = """
            SELECT url
            FROM article
            WHERE (YEAR(last_mod) IN (2019, 2020, 2021, 2022, 2023))
            AND category = 'politics'
            AND scraped IS NULL
            ORDER BY RAND()
            LIMIT 500;
        """
    while True:
        
        logger.info(f'{count} iteration of scraping urls')
        count += 1
        
        connection = mysql.connector.connect(**config)
        cur = connection.cursor()
        
        
        cur.execute(query)
        urls = [row[0] for row in cur.fetchall()]
        
        if len(urls) == 0:
            break
        logger.info(f'Got new {len(urls)} urls')
        """
        if len(urls)<400:
            query = 
                SELECT url
                FROM article
                WHERE (YEAR(last_mod) IN (2019, 2020, 2021, 2022, 2023))
                AND scraped IS NULL
                ORDER BY RAND()
                LIMIT 1;
            """
        cur.close()
        connection.close()
        logger.info(f'Checked for overlap, starting async scrapping now')
        start_time = time.time()
        if TESTING:
            asyncio.run(scrape_urls(urls,count))
        else:
            asyncio.run(scrape_urls(urls,default_async))
        if TESTING:
            elapsed_time = time.time() - start_time
            log_message = f'| Successfully processed | {successful_requests} | in | {elapsed_time:.2f} | seconds with | {count} | concurrent requests |'
            table_logger.info(log_message)
            successful_requests = 0  # Reset the counter
            time.sleep(5)
async def monitor_requests():
    global successful_requests
    while True:
        await asyncio.sleep(60)  # Wait for 1 minute
        elapsed_time = time.time() - start_time
        logger.info(f"Successful requests: {successful_requests}")

def speedTest(func):
    profiler = cProfile.Profile()
    profiler.enable()
    try:
        func()
    except:
        pass
    finally:
        profiler.disable()
        
        stats = StringIO()
        stats_print = pstats.Stats(profiler, stream=stats).sort_stats('cumulative')
        stats_print.print_stats()
        logger.info(stats.getvalue())
if __name__ == "__main__":

    if PROFILING:
        speedTest(main)
    else:
        main()
    


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