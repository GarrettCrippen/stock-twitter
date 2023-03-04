from Mystream import Mystream
import tweepy
import secret_info as s

from selenium import webdriver
from bs4 import BeautifulSoup
import re
import traceback

DRY_RUN = False

# client = tweepy.Client(s.bearer_token, s.api_key,
#                       s.api_secret, s.access_token, s.access_secret)
#auth = tweepy.OAuth1UserHandler(s.api_key, s.api_secret)
#auth.set_access_token(s.access_token, s.access_secret)
#api = tweepy.API(auth)

# Adds rules containing a fixed number of hash_tags


def add_rules(stock_tags, rule_size):

    subsets = list([stock_tags[x:x+rule_size]
                   for x in range(0, len(stock_tags), rule_size)])

    streaming_client = Mystream(s.bearer_token)

    rules = []
    try:
        for ruleid in streaming_client.get_rules().data:
            rules.append(ruleid.id)

        print(rules)
        streaming_client.delete_rules(ids=rules, dry_run=DRY_RUN)
        print('Deleted Rules')
    except Exception:
        traceback.print_exc()

    rules = []
    try:
        for hash_tags in subsets:
            rule = ' OR '.join(hash_tags)
            print(f'adding rule: \'{rule}\'')
            rules.append(tweepy.StreamRule(value=rule))

        streaming_client.add_rules(add=rules, dry_run=DRY_RUN)
        print('Added rules')
    except Exception:
        traceback.print_exc()


# Webrowser stuff(not sure if Selenium works on jupyter, else just run on your local pc)
browser = webdriver.Firefox()
browser.get('https://www.slickcharts.com/nasdaq100')
soup = BeautifulSoup(browser.page_source, "html.parser")
browser.close()

stock_tags = []
for element in soup.find_all('a', {'href': re.compile('^(/symbol/)')}):
    stock_tags.append(element.text)
subsets = list(map(lambda x: '$'+x, stock_tags[1::2]))


# adds 17 rules containing 6 stock_tags
add_rules(subsets, 6)
