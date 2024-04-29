import sqlite3
import requests
from bs4 import BeautifulSoup
import time
import json
import asyncio
import aiohttp
import json
import time
success_time = []
error_time = []
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
            print(f"KeyError in to_comment: {e}")
            return None
        except IndexError as e:
            print(f"IndexError in to_comment: {e}")
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
            print(f"KeyError in get_replies: {e}")
        except Exception as e:
            print(f"Unhandled exception in get_replies: {e}")

        return replies
async def make_request(url, headers, json_data, request_count):
    print(request_count)
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
                    print("Connection reset by peer. Waiting for a second before retrying...")
                    await asyncio.sleep(1)
                    return await make_request(url, headers, json_data, request_count)  # Retry
                else:
                    print(f"Error in request. Status code: {response.status}")
                    return None

    except Exception as e:
        print(f"Error making request: {e}")
        #end_time = time.time()
        #elapsed_time = end_time - start_time
        #error_time.append(elapsed_time)
        # Potential infinite loop fix later (e.g., with retries limit)
        return await make_request(url, headers, json_data, request_count)
async def async_get(url, timeout=5):
    for _ in range(5):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=timeout) as response:
                    return await response.text()
        except asyncio.TimeoutError:
            pass


async def scrape_url(url,search = ""):
    chat = {}
    print(f"Scrapping url: %s" % url)
    try:
        html = await async_get(url)
        if html:
            soup = BeautifulSoup(html, 'html.parser')
            address_key = soup.head.find(attrs={"name": "dc.identifier"})['content']
            title = soup.head.find(attrs={"name": "dc.title"})['content']
            author = soup.head.find(attrs={"name": "dc.creator"})['content']
            date = soup.head.find(attrs={"name": "dc.date"})['content']
            
    except Exception as e:
        print(f"Error occurred while scraping {url}: {e}")
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
            print(f"'messages_count': {chat['messages_count']}\n'replies_count': {chat['replies_count']}\n'comments_count': {chat['comments_count']}")

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
                    print("Error in inner request.")
                    break

    except KeyboardInterrupt:
        print("Process interrupted.")
    except Exception as e:
        print(f"Unhandled exception in scrape url: {e}")
        print(e)
    #print(response_json)
    #print("{" + "\n".join("{!r}: {!r},".format(k, v) for k, v in data.items()) + "}")
    #the 'demopage.asp' prints all HTTP Headers
    #ADD TO DATABASE

    #ADD ARTICLE
    connection = sqlite3.connect('news.db')
    #print(connection.total_changes)
    cur = connection.cursor()
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

    connection.commit()
    print(f'Comments added: %d' % len(comments)) 
    connection.close()

async def scrape_urls(urls):
    success_times = []
    error_times = []
    tasks= []
    for idx, url in enumerate(urls):
        task = scrape_url(url)
        tasks.append(task)
    results = await asyncio.gather(*tasks)

# Example usage:
import cProfile
import pstats
from io import StringIO
urls = ['https://www.foxnews.com/politics/global-elites-took-150-private-jets-fight-climate-change-davos', 'https://www.foxnews.com/politics/biden-says-climate-change-is-bigger-threat-humanity-nuclear-war', 'https://www.foxnews.com/politics/al-gore-history-climate-predictions-statements-proven-false', 'https://www.foxnews.com/politics/biden-admin-gives-top-energy-post-climate-activist-failed-senate-confirmation-ethics-concerns', 'https://www.foxnews.com/politics/mysterious-eco-group-funding-local-climate-journalism-advocacy-dressed-up-news-reporting', 'https://www.foxnews.com/politics/al-gore-inflation-reduction-act-climate-change-bill', 'https://www.foxnews.com/politics/john-kerry-climate-threat-wartime-urgency-turn-factories-solar-panel-producers', 'https://www.foxnews.com/politics/gallagher-accuses-biden-admin-being-divided-whether-china-climate-change-top-threat-us', 'https://www.foxnews.com/politics/science-prof-blasts-green-activists-clearly-dont-believe-climate-doom-robbing-young-people-hope', 'https://www.foxnews.com/politics/al-gore-takes-swing-trump-appointed-world-bank-president-climate-denier', 'https://www.foxnews.com/politics/eco-group-slams-davos-summit-global-elite-arrive-private-jets-talk-climate-policy', 'https://www.foxnews.com/politics/al-gore-explains-global-ai-program-spying-thousands-facilities-monitor-emissions', 'https://www.foxnews.com/politics/democrats-eco-groups-set-sights-other-home-appliances-gas-stove-debate', 'https://www.foxnews.com/politics/republican-senator-unveils-bills-targeting-bidens-climate-agenda-energy-backbone-economy', 'https://www.foxnews.com/politics/climate-equity-guided-bidens-decision-award-billion-grants-mega-transportation-projects', 'https://www.foxnews.com/politics/ca-introduces-climate-bill-make-companies-disclose-greenhouse-gas-emissions', 'https://www.foxnews.com/politics/podesta-linked-energy-executive-positioned-benefit-bidens-latest-climate-agenda', 'https://www.foxnews.com/politics/nm-bill-aimed-protecting-communities-climate-crisis-advances-legislature', 'https://www.foxnews.com/politics/bidens-new-border-plan-includes-fighting-xenophobia-adding-ev-chargers-climate-change', 'https://www.foxnews.com/politics/house-democratic-leaders-non-binary-child-arrested-anti-cop-protest-climate-change-nightmares', 'https://www.foxnews.com/politics/john-kerry-applauds-fellow-davos-attendees-extra-terrestrial-wanting-save-planet', 'https://www.foxnews.com/politics/energy-workers-havent-forgotten-wont-forgive-biden-killing-keystone-xl-jobs-un-american', 'https://www.foxnews.com/politics/democrat-led-cities-already-moving-forward-gas-stove-bans-affect-millions', 'https://www.foxnews.com/politics/republicans-spr-bill-leaves-democrats-squirming-oil-leasing-process-balance', 'https://www.foxnews.com/politics/biden-admin-issues-20-year-mining-ban-turns-foreign-supply-chain-amid-green-energy-push', 'https://www.foxnews.com/politics/biden-visit-ca-areas-devastated-extreme-weather-thursday', 'https://www.foxnews.com/politics/virginia-residents-reject-massive-solar-farm-plan-third-time-over-environmental-concerns', 'https://www.foxnews.com/politics/elites-davos-strategize-how-fight-right-wing-groups-hit-back', 'https://www.foxnews.com/politics/democrats-push-amend-constitution-16-year-olds-vote', 'https://www.foxnews.com/politics/problems-persist-faa-despite-23-billion-budget', 'https://www.foxnews.com/politics/trade-unions-representing-laid-off-keystone-xl-workers-silent-report-shows-thousands-job-losses', 'https://www.foxnews.com/politics/border-patrol-union-rips-biden-admin-new-pursuit-policy-smugglers-encouraged-drive-recklessly']

profiler = cProfile.Profile()
profiler.enable()

asyncio.run(scrape_urls(urls))

profiler.disable()

# Print the profiling results
stats = StringIO()
stats_print = pstats.Stats(profiler, stream=stats).sort_stats('cumulative')
stats_print.print_stats()
print(stats.getvalue())

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

    return summary_stats
'''
# Example usage:
input_data = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
result = array_summary_statistics(input_data)

# Print the summary statistics
print("Summary Statistics:")
print("Success_time:")
for stat, value in array_summary_statistics(success_time).items():
    print(f"{stat}: {value}")
print("Error_time:")
for stat, value in array_summary_statistics(error_time).items():
    print(f"{stat}: {value}")
    '''