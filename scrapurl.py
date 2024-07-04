import sqlite3
import requests
from bs4 import BeautifulSoup
import time
import json
import asyncio
import aiohttp
import json
import time
import logging
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
    for _ in range(5):
        try:
            async with aiohttp.ClientSession() as session:
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
        logger.info(f'Comments added: %d' % len(comments)) 
        connection.close()

async def scrape_urls(urls):
    success_times = []
    error_times = []
    batch_size = 300
    tasks = []
    semaphore = asyncio.Semaphore(64)  # Limit to 20 concurrent tasks
    tasks = [scrape_url(url, semaphore) for url in urls]
    await asyncio.gather(*tasks)

# Example usage:
import cProfile
import pstats
from io import StringIO
urls = ['https://www.foxnews.com/politics/global-elites-took-150-private-jets-fight-climate-change-davos', 'https://www.foxnews.com/politics/biden-says-climate-change-is-bigger-threat-humanity-nuclear-war', 'https://www.foxnews.com/politics/al-gore-history-climate-predictions-statements-proven-false', 'https://www.foxnews.com/politics/biden-admin-gives-top-energy-post-climate-activist-failed-senate-confirmation-ethics-concerns', 'https://www.foxnews.com/politics/mysterious-eco-group-funding-local-climate-journalism-advocacy-dressed-up-news-reporting', 'https://www.foxnews.com/politics/al-gore-inflation-reduction-act-climate-change-bill', 'https://www.foxnews.com/politics/john-kerry-climate-threat-wartime-urgency-turn-factories-solar-panel-producers', 'https://www.foxnews.com/politics/gallagher-accuses-biden-admin-being-divided-whether-china-climate-change-top-threat-us', 'https://www.foxnews.com/politics/science-prof-blasts-green-activists-clearly-dont-believe-climate-doom-robbing-young-people-hope', 'https://www.foxnews.com/politics/al-gore-takes-swing-trump-appointed-world-bank-president-climate-denier', 'https://www.foxnews.com/politics/eco-group-slams-davos-summit-global-elite-arrive-private-jets-talk-climate-policy', 'https://www.foxnews.com/politics/al-gore-explains-global-ai-program-spying-thousands-facilities-monitor-emissions', 'https://www.foxnews.com/politics/democrats-eco-groups-set-sights-other-home-appliances-gas-stove-debate', 'https://www.foxnews.com/politics/republican-senator-unveils-bills-targeting-bidens-climate-agenda-energy-backbone-economy', 'https://www.foxnews.com/politics/climate-equity-guided-bidens-decision-award-billion-grants-mega-transportation-projects', 'https://www.foxnews.com/politics/ca-introduces-climate-bill-make-companies-disclose-greenhouse-gas-emissions', 'https://www.foxnews.com/politics/podesta-linked-energy-executive-positioned-benefit-bidens-latest-climate-agenda', 'https://www.foxnews.com/politics/nm-bill-aimed-protecting-communities-climate-crisis-advances-legislature', 'https://www.foxnews.com/politics/bidens-new-border-plan-includes-fighting-xenophobia-adding-ev-chargers-climate-change', 'https://www.foxnews.com/politics/house-democratic-leaders-non-binary-child-arrested-anti-cop-protest-climate-change-nightmares', 'https://www.foxnews.com/politics/john-kerry-applauds-fellow-davos-attendees-extra-terrestrial-wanting-save-planet', 'https://www.foxnews.com/politics/energy-workers-havent-forgotten-wont-forgive-biden-killing-keystone-xl-jobs-un-american', 'https://www.foxnews.com/politics/democrat-led-cities-already-moving-forward-gas-stove-bans-affect-millions', 'https://www.foxnews.com/politics/republicans-spr-bill-leaves-democrats-squirming-oil-leasing-process-balance', 'https://www.foxnews.com/politics/biden-admin-issues-20-year-mining-ban-turns-foreign-supply-chain-amid-green-energy-push', 'https://www.foxnews.com/politics/biden-visit-ca-areas-devastated-extreme-weather-thursday', 'https://www.foxnews.com/politics/virginia-residents-reject-massive-solar-farm-plan-third-time-over-environmental-concerns', 'https://www.foxnews.com/politics/elites-davos-strategize-how-fight-right-wing-groups-hit-back', 'https://www.foxnews.com/politics/democrats-push-amend-constitution-16-year-olds-vote', 'https://www.foxnews.com/politics/problems-persist-faa-despite-23-billion-budget', 'https://www.foxnews.com/politics/problems-persist-faa-despite-23-billion-budget', 'https://www.foxnews.com/politics/ron-desantis-flames-davos-jetsetters-technocratic-elites-fiery-inauguration-speech', 'https://www.foxnews.com/politics/biden-admin-cracks-down-washers-fridges-latest-climate-action-overregulation-steroids', 'https://www.foxnews.com/politics/white-house-climate-czar-met-privately-eco-group-pushing-gas-stove-bans', 'https://www.foxnews.com/politics/john-kerry-family-private-jet-sold-shortly-after-accusations-climate-hypocrisy', 'https://www.foxnews.com/politics/wwii-style-rationing-food-fuel-combat-climate-change-academics-claim-new-paper', 'https://www.foxnews.com/politics/climate-activist-dems-dropped-1-4-million-private-jets-midterms', 'https://www.foxnews.com/politics/george-soros-calls-weather-control-stop-global-warming-ice-sheet-melting', 'https://www.foxnews.com/politics/ccp-backed-tech-companies-poised-cash-in-bidens-climate-bill-national-security-experts-warn', 'https://www.foxnews.com/politics/far-left-environmental-groups-conducted-internal-biden-admin-trainings-documents-show', 'https://www.foxnews.com/politics/china-unleashes-massive-coal-power-expansion-despite-john-kerrys-climate-pleas', 'https://www.foxnews.com/politics/climate-crisis-avoided-colonists-adopted-indigenous-lifestyle-top-biden-official', 'https://www.foxnews.com/politics/senate-dems-focus-climate-change-alarmism-first-budget-hearing-but-sign-budget-plan', 'https://www.foxnews.com/politics/gop-senators-manchin-challenge-bidens-esg-climate-investment-rule-politicizing-americans-401ks', 'https://www.foxnews.com/politics/rep-barbara-lee-launches-senate-campaign-promising-climate-crisis-stop-maga-extremists', 'https://www.foxnews.com/politics/biden-torched-climate-change-tweet-vowing-cleaner-air-safer-water-ohio-train-derailment-fallout', 'https://www.foxnews.com/politics/socialists-cheer-dem-states-climate-bill-mandating-fossil-fuel-shutdown-will-transform-new-york', 'https://www.foxnews.com/politics/house-votes-kill-bidens-woke-esg-investment-rule-props-up-phony-climate-movement', 'https://www.foxnews.com/politics/chatgpt-alters-response-benefits-fossil-fuels-now-refuses-answer-climate-concerns', 'https://www.foxnews.com/politics/gop-sen-josh-hawley-interrupted-climate-protester-during-speech-china-not-our-enemy', 'https://www.foxnews.com/politics/john-kerrys-secret-ccp-negotiations-probed-gop-oversight-chairman', 'https://www.foxnews.com/politics/biden-climate-envoy-kerry-meets-socialist-brazilian-leadership', 'https://www.foxnews.com/politics/major-medical-group-wipes-study-advocating-doctors-give-less-anesthesia-reduce-carbon-footprint', 'https://www.foxnews.com/politics/curbing-u-s-oil-gas-production-hurt-environment-report-finds', 'https://www.foxnews.com/politics/john-kerry-frustrated-oil-companies-interested-costly-renewable-business', 'https://www.foxnews.com/politics/bill-gates-defense-flying-private-echoes-john-kerry-excuse-jet-setting-around-world', 'https://www.foxnews.com/politics/house-gop-announces-aggressive-first-kind-effort-combat-esg-movement', 'https://www.foxnews.com/politics/inspector-general-investigating-pete-buttigiegs-extensive-private-jet-travel', 'https://www.foxnews.com/politics/biden-admins-billion-dollar-environmental-justice-spending-probed-gop-oversight-leaders', 'https://www.foxnews.com/politics/esg-investment-rule-emerges-top-woke-target-republicans-battling-biden', 'https://www.foxnews.com/politics/minnesota-moves-toward-2040-carbon-neutrality-goal', 'https://www.foxnews.com/politics/house-gop-votes-next-week-kill-bidens-woke-esg-investing-rule', 'https://www.foxnews.com/politics/buttigiegs-dot-spending-662m-fix-americas-ports-only-projects-advance-esg-equity-goals', 'https://www.foxnews.com/politics/chinese-balloon-flying-us-intentional-not-weather-craft-blew-off-course-us-official-says', 'https://www.foxnews.com/politics/top-republican-raises-alarm-biden-energy-secretarys-work-china-connected-group-pushing-gas-stove-ban', 'https://www.foxnews.com/politics/dark-money-group-pushing-gas-stove-crackdown-significant-financial-stake-green-energy', 'https://www.foxnews.com/politics/over-100-groups-back-manchin-gop-plan-block-bidens-woke-esg-investing-rule', 'https://www.foxnews.com/politics/white-house-pressed-why-biden-sent-top-gun-fighters-shoot-down-suspected-weather-balloons', 'https://www.foxnews.com/politics/fox-news-poll-gop-maintains-advantage-top-issues-democrats-show-some-momentum', 'https://www.foxnews.com/politics/high-ranking-dem-turns-biden-admin-gives-major-oil-project-green-light', 'https://www.foxnews.com/politics/biden-issue-first-veto-congress-prepares-vote-against-esg-investment-rule', 'https://www.foxnews.com/politics/rep-barbara-lees-history-defending-convicted-murderers-resurfaces-senate-announcement', 'https://www.foxnews.com/politics/china-insists-second-surveillance-craft-over-caribbean-also-thrown-off-course-by-weather', 'https://www.foxnews.com/politics/state-of-the-union-biden-lays-out-economic-plan-declares-climate-as-existential-threat', 'https://www.foxnews.com/politics/chinese-spy-balloon-poses-no-imminent-danger-top-foreign-affairs-democrat-says', 'https://www.foxnews.com/politics/californias-grid-faces-collapse-leaders-push-renewables-electric-vehicles-experts-say', 'https://www.foxnews.com/politics/bidens-sotu-ignores-earthquake-turkey-syria-killed-thousands', 'https://www.foxnews.com/politics/democratic-voters-cheer-gas-stove-ban-poll', 'https://www.foxnews.com/politics/gretchen-whitmer-criticized-apparent-night-out-while-michigan-hit-with-severe-winter-weather-power-loss', 'https://www.foxnews.com/politics/internal-biden-admin-memo-shows-serious-banning-gas-stoves-public-uproar', 'https://www.foxnews.com/politics/vp-kamala-harris-touts-successes-border-czar-returns-immigration-beat-overseas-investments', 'https://www.foxnews.com/politics/gop-pushback-woke-esg-investing-begins-bear-fruit', 'https://www.foxnews.com/politics/massive-green-energy-company-reports-1-billion-losses-calls-further-governmental-action', 'https://www.foxnews.com/politics/no-evidence-china-surveillance-flights-used-spread-covid-house-intel-committee-member', 'https://www.foxnews.com/politics/risk-safety-montana-congressman-rejects-pentagons-reason-not-shooting-down-chinese-spy-balloon', 'https://www.foxnews.com/politics/pete-buttigieg-blames-trump-ohio-train-derailment-were-constrained', 'https://www.foxnews.com/politics/24-states-sue-biden-epa-environmental-rule-targeting-farmers-landowners', 'https://www.foxnews.com/politics/border-patrol-calls-agents-volunteer-northern-border-amid-846-spike-one-sector', 'https://www.foxnews.com/politics/chinese-spy-balloons-over-us-during-trump-admin-discovered-after-he-left-office-senior-biden-official', 'https://www.foxnews.com/politics/dems-join-gop-vote-condemn-chinas-surveillance-flight-demand-info-biden', 'https://www.foxnews.com/politics/biden-admin-removing-most-surveillance-balloons-southern-border-due-cost-sources', 'https://www.foxnews.com/politics/biden-admin-makes-stunning-admission-climate-agenda-leaked-internal-memo', 'https://www.foxnews.com/politics/navy-secretary-cites-climate-change-top-priority-biden-proposes-shrinking-fleet', 'https://www.foxnews.com/politics/john-kerry-rushes-defense-climate-activist-leaders-use-private-jets', 'https://www.foxnews.com/politics/un-climate-report-latest-string-cataclysmic-predictions-stretching-back-decades', 'https://www.foxnews.com/politics/biden-nominee-coordinated-dark-money-climate-nuisance-lawsuits-involving-leonardo-dicaprio', 'https://www.foxnews.com/politics/biden-set-approve-massive-oil-drilling-project-climate-activists-derided-carbon-bomb', 'https://www.foxnews.com/politics/researchers-identify-mammal-latest-potential-cause-climate-change-suggest-balancing-species', 'https://www.foxnews.com/politics/california-dem-senate-candidates-all-back-far-left-climate-proposals', 'https://www.foxnews.com/politics/un-calls-mass-fossil-fuel-shutdowns-prevent-climate-time-bomb', 'https://www.foxnews.com/politics/republicans-probe-biden-admins-climate-overreach-consumer-insurance', 'https://www.foxnews.com/politics/aoc-top-democrats-issue-stinging-rebuke-biden-failed-climate-promises', 'https://www.foxnews.com/politics/biden-admin-bucks-climate-activists-holds-enormous-trump-era-oil-gas-lease-sale', 'https://www.foxnews.com/politics/oregon-eyes-mandate-climate-change-lessons-schools', 'https://www.foxnews.com/politics/biden-climate-change-cause-colorado-river-dry-up-ritzy-dnc-dinner', 'https://www.foxnews.com/politics/democrats-turn-climate-change-next-mental-health-crisis', 'https://www.foxnews.com/politics/biden-expected-approve-enormous-oil-drilling-project-blow-climate-activists-complete-betrayal', 'https://www.foxnews.com/politics/manchin-tanks-biden-nominee-over-climate-activism', 'https://www.foxnews.com/politics/climate-czar-john-kerry-says-biden-impose-more-mandates-go-farther-inflation-reduction-act', 'https://www.foxnews.com/politics/ted-cruz-leads-12-republicans-blasting-nasa-highly-politicized-climate-regulation', 'https://www.foxnews.com/politics/biden-admin-moves-limit-ev-tax-credit-eligibility-potential-blow-climate-agenda', 'https://www.foxnews.com/politics/federal-judge-delivers-blow-bidens-climate-agenda-destructive-federal-overreach', 'https://www.foxnews.com/politics/biden-cabinet-official-forced-admit-climate-agenda-strengthening-china', 'https://www.foxnews.com/politics/granholm-pressed-explain-ccp-propaganda-comments-us-can-learn-china-climate-change', 'https://www.foxnews.com/politics/bidens-climate-change-zealotry-contributed-silicon-valley-bank-collapse-republican-ags-charge', 'https://www.foxnews.com/politics/harvard-journal-accepts-left-wing-paper-accusing-fossil-fuel-industry-homicide', 'https://www.foxnews.com/politics/climate-activists-dems-turn-on-biden-likely-alaskan-oil-drilling-project-existential-threat', 'https://www.foxnews.com/politics/youth-un-adviser-says-climate-change-needs-treated-covid-19-emergency', 'https://www.foxnews.com/politics/republicans-slam-biden-energy-secretary-claiming-us-learn-china-doing-climate-change', 'https://www.foxnews.com/politics/democrats-own-witness-blasted-bidens-climate-bill-permitting-climate-apocalypse', 'https://www.foxnews.com/politics/biden-energy-secretary-defends-praising-china-on-climate-change-spending-in-fiery-hearing-exchange', 'https://www.foxnews.com/politics/blue-city-wants-ban-new-gas-auto-service-stations-climate-change', 'https://www.foxnews.com/politics/us-intel-community-warns-complex-threats-china-russia-north-korea', 'https://www.foxnews.com/politics/biden-admin-hit-lawsuit-approval-massive-oil-drilling-project-enraged', 'https://www.foxnews.com/politics/republicans-demand-bidens-energy-secretary-retract-unserious-comments-praising-china', 'https://www.foxnews.com/politics/the-plane-truth-anti-global-warmers-buttigieg-biden-and-kerry-all-questioned-about-their-jet-use', 'https://www.foxnews.com/politics/national-weather-service-dragged-transgender-day-visibility-tweet', 'https://www.foxnews.com/politics/biden-washing-machine-rule-would-make-americans-dirtier-stinkier-raise-prices-manufacturers', 'https://www.foxnews.com/politics/gop-opens-investigation-biden-admin-obstructing-us-energy-producers-radical-eco-agenda', 'https://www.foxnews.com/politics/leaked-biden-admin-energy-security-memo-could-torpedo-key-biden-nominee', 'https://www.foxnews.com/politics/biden-indefinitely-blocks-millions-acres-land-water-future-oil-drilling', 'https://www.foxnews.com/politics/nevada-governor-blasts-biden-locking-up-mineral-rich-land-historic-mistake', 'https://www.foxnews.com/politics/dark-money-eco-group-appears-astroturfing-grassroots-opposition-major-oil-project', 'https://www.foxnews.com/politics/biden-admin-cracks-down-air-conditioners-war-appliances-continues', 'https://www.foxnews.com/politics/san-francisco-latest-dem-city-crack-down-gas-appliances', 'https://www.foxnews.com/politics/21-states-threaten-banks-legal-action-woke-policies-stay-your-lane', 'https://www.foxnews.com/politics/ringleader-gas-stove-crackdown-speak-house-democrats-annual-conference', 'https://www.foxnews.com/politics/bidens-energy-secretary-pumps-brakes-rapid-green-energy-transition-we-need-both', 'https://www.foxnews.com/politics/kamala-harris-telling-childhood-story-says-she-asked-her-mom-why-conservatives-bad', 'https://www.foxnews.com/politics/biden-broke-central-campaign-promise-progressives-not-happy', 'https://www.foxnews.com/politics/democrats-republicans-aim-block-biden-admins-action-protecting-chinese-solar-companies', 'https://www.foxnews.com/politics/nm-considers-providing-financial-relief-families-young-kids-low-income-households-veterans', 'https://www.foxnews.com/politics/house-dem-pushes-legislation-against-big-oil-profiteering-after-investing-major-oil-companies', 'https://www.foxnews.com/politics/biden-considering-tearing-down-key-green-energy-source-eco-concerns', 'https://www.foxnews.com/politics/manchin-aligns-gop-biden-oil-drilling-project-defends-record-energy-production-democrats-fret', 'https://www.foxnews.com/politics/alaska-governor-says-biden-treats-venezuela-better-his-state', 'https://www.foxnews.com/politics/oil-giants-offer-264m-gulf-mexico-drilling-rights', 'https://www.foxnews.com/politics/biden-admins-war-household-appliances-cause-higher-prices-dirtier-clothes-dishes-experts-warn', 'https://www.foxnews.com/politics/biden-clean-energy-czar-podesta-claims-chinese-companies-big-players-future-us-energy-production', 'https://www.foxnews.com/politics/joe-manchin-blasts-biden-admin-ceding-control-ccp-green-energy-pathetic', 'https://www.foxnews.com/politics/top-new-york-republican-scorches-state-dems-effort-ban-gas-stoves-attack-working-people', 'https://www.foxnews.com/politics/utah-state-treasurer-leader-movement-corporate-wokeness-says-esg-part-satans-plan', 'https://www.foxnews.com/politics/stacey-abrams-getsnew-job-after-election-loss-joins-environmental-group-trying-eliminate-gas-stoves', 'https://www.foxnews.com/politics/senators-accuse-biden-skirting-rules-esg-push-weaponizing-americans-retirement-funds-ahead-vote', 'https://www.foxnews.com/politics/biden-admin-quietly-delays-major-oil-gas-leasing-decision', 'https://www.foxnews.com/politics/domestic-terror-suspects-cop-city-attack-links-left-wing-groups-protest-movements', 'https://www.foxnews.com/politics/california-gov-gavin-newsom-personal-trip-baja-california-winter-storms', 'https://www.foxnews.com/politics/marjorie-taylor-greenes-twitter-account-temporarily-suspended-violating-rules', 'https://www.foxnews.com/politics/seven-figure-ad-campaign-exposes-billions-bidens-medicare-cuts-after-accusations-gop-wants-slash', 'https://www.foxnews.com/politics/cori-bush-paid-137k-private-security-unlicensed-anti-semitic-spiritual-guru-claims-tornado-power', 'https://www.foxnews.com/politics/indiana-medical-students-dei-instruction-gender-basic-human-structure-course', 'https://www.foxnews.com/politics/climate-activists-arrested-blocking-rush-hour-dc-traffic', 'https://www.foxnews.com/politics/house-lawmakers-un-report-climate-change', 'https://www.foxnews.com/politics/biden-admin-rushing-industrialize-us-oceans-stop-climate-change-environmental-wrecking-ball', 'https://www.foxnews.com/politics/biden-nominee-wants-hijack-little-known-agency-ram-through-climate-agenda', 'https://www.foxnews.com/politics/climate-activists-shut-down-wh-clean-energy-czar-john-podestas-speech-room-full-millionaires', 'https://www.foxnews.com/politics/aoc-admits-massive-scale-green-new-deal-says-climate-change-even-worse', 'https://www.foxnews.com/politics/biden-pledges-1b-more-us-funding-un-green-climate-fund', 'https://www.foxnews.com/politics/climate-czar-john-kerry-says-extreme-storms-climate-change-rip-crops-away-destroy-homes', 'https://www.foxnews.com/politics/biden-climate-czar-john-kerrys-top-deputies-discussed-keeping-discussions-off-paper', 'https://www.foxnews.com/politics/kamala-harris-issues-dire-climate-change-warning-africa-existential-threat-entire-planet', 'https://www.foxnews.com/politics/republican-led-state-opens-sweeping-investigation-woke-investing-group', 'https://www.foxnews.com/politics/kerry-says-us-china-must-work-together-climate-world-not-doing-enough', 'https://www.foxnews.com/politics/shut-down-far-left-climate-protesters-arrested-storming-blackrocks-nyc-hq-pitchforks', 'https://www.foxnews.com/politics/new-york-climate-law-defines-ritzy-communities-million-dollar-homes-disadvantaged', 'https://www.foxnews.com/politics/cattlemen-roast-vegan-nyc-mayor-cracking-down-food-part-climate-agenda', 'https://www.foxnews.com/politics/expelled-tennessee-democrat-justin-jones-spotted-climate-change-protest-accusing-biden-ecocide', 'https://www.foxnews.com/politics/biden-admin-approves-massive-gas-pipeline-huge-blow-climate-activists', 'https://www.foxnews.com/politics/climate-activists-plan-blockade-white-house-correspondents-dinner-accuse-biden-ecocide', 'https://www.foxnews.com/politics/utah-best-economic-climate-country-new-york-worst-report', 'https://www.foxnews.com/politics/navy-secretary-defends-calling-climate-change-top-priority-equally-important-other-goals', 'https://www.foxnews.com/politics/panel-democrats-environmental-activists-blame-climate-change-whale-deaths', 'https://www.foxnews.com/politics/pennsylvania-gov-josh-shapiros-group-designed-fight-climate-change-meets-first-time-week', 'https://www.foxnews.com/politics/adams-applauds-nyc-hospitals-feeding-patients-plant-based-meals-meat-default-climate-push', 'https://www.foxnews.com/politics/biden-executive-order-require-agencies-make-environmental-justice-part-mission', 'https://www.foxnews.com/politics/judge-cancels-montana-gas-plants-permit-climate-concerns', 'https://www.foxnews.com/politics/republican-lawmakers-rally-condemn-biden-proposed-electric-vehicle-rule', 'https://www.foxnews.com/politics/swiss-billionaire-backed-environmental-groups-spent-big-targeting-millions-biden-voters', 'https://www.foxnews.com/politics/aoc-other-dems-reintroduce-trillion-dollar-green-new-deal-end-fossil-fuels', 'https://www.foxnews.com/politics/biden-energy-secretary-doubles-down-on-electrifying-us-militarys-vehicle-fleet-2030-we-can-get-there', 'https://www.foxnews.com/politics/harris-announces-over-550-million-in-recommended-funding-make-communities-resilient-to-climate-impacts', 'https://www.foxnews.com/politics/biden-admin-moving-forward-light-bulb-bans-coming-weeks', 'https://www.foxnews.com/politics/white-house-connected-energy-firm-first-beneficiaries-inflation-reduction-act', 'https://www.foxnews.com/politics/white-house-furiously-consulting-eco-groups-ahead-expected-power-plant-crackdown', 'https://www.foxnews.com/politics/new-york-moves-become-first-state-banning-natural-gas-hookups', 'https://www.foxnews.com/politics/jayapal-criticized-saying-immigrants-needed-america-to-pick-the-food-we-eat-clean-our-homes', 'https://www.foxnews.com/politics/psaki-has-dessert-hard-hitting-interview-former-boss-doesnt-ask-tough-questions', 'https://www.foxnews.com/politics/biden-admin-receives-backlash-nearly-two-dozen-groups-cracking-down-gas-stoves', 'https://www.foxnews.com/politics/biden-unveils-toughest-ever-car-emissions-rules-bid-force-electric-vehicle-purchases', 'https://www.foxnews.com/politics/federal-appeals-court-strikes-down-democratic-citys-natural-gas-ban-backed-biden-admin', 'https://www.foxnews.com/politics/biden-democratic-challenger-robert-f-kennedy-jr-files-run-president', 'https://www.foxnews.com/politics/joe-manchin-quietly-lobbied-biden-admin-grant-funding-campaign-treasurers-organization', 'https://www.foxnews.com/politics/congress-gears-up-smack-down-president-bidens-solar-handout', 'https://www.foxnews.com/politics/bidens-ambitious-ev-plans-make-us-more-dependent-chinese-supply-chains', 'https://www.foxnews.com/politics/farmers-score-victory-biden-admin-judge-pauses-controversial-eco-rules', 'https://www.foxnews.com/politics/bidens-war-hunting-faces-blowback-republicans-sportsmen-groups', 'https://www.foxnews.com/politics/chinese-tech-companies-exploiting-us-green-energy-goals-former-state-department-officials-warn', 'https://www.foxnews.com/politics/bidens-navy-secretary-blasted-ignoring-congress-fleet-strength-violating-law', 'https://www.foxnews.com/politics/bidens-green-energy-plans-pose-national-security-risk-pentagon-warns', 'https://www.foxnews.com/politics/republican-lawmakers-delaware-push-back-against-electric-vehicle-mandate', 'https://www.foxnews.com/politics/biden-admin-outlines-plan-cut-vital-water-supplies-western-states', 'https://www.foxnews.com/politics/byron-donalds-endorses-trump-president-strength-resolve', 'https://www.foxnews.com/politics/robert-f-kennedy-jr-democrat-challenging-biden-what-is-his-platform', 'https://www.foxnews.com/politics/gop-presidential-candidate-slaps-world-economic-forum-with-lawsuit-using-name-radical-worldview', 'https://www.foxnews.com/politics/conservative-activist-launches-ultra-right-beer-rival-bud-light-after-dylan-mulvaney-controversy', 'https://www.foxnews.com/politics/gop-senators-un-ambassador-report-pedophiles', 'https://www.foxnews.com/politics/illinois-state-senator-defends-chicago-teens-rioting-looting-mass-protest', 'https://www.foxnews.com/politics/democrats-ignore-criminals-blame-car-companies-skyrocketing-auto-thefts', 'https://www.foxnews.com/politics/biden-raises-eyebrows-telling-irish-leaders-lick-world', 'https://www.foxnews.com/politics/lockdowns-mandates-scandals-gavin-newsoms-covid-19-response-brought-california-knees', 'https://www.foxnews.com/politics/desantis-super-pac-never-back-down-first-ad-buy-2024', 'https://www.foxnews.com/politics/whistleblower-tells-congress-that-govt-delivering-migrant-children-human-traffickers', 'https://www.foxnews.com/politics/save-womens-sports-bill-passes-house-zero-votes-dems-transgender-bullying']
profiler = cProfile.Profile()
profiler.enable()

asyncio.run(scrape_urls(urls))

profiler.disable()

# Print the profiling results
stats = StringIO()
stats_print = pstats.Stats(profiler, stream=stats).sort_stats('cumulative')
stats_print.print_stats()
logger.info(stats.getvalue())

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
print(len(urls))
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