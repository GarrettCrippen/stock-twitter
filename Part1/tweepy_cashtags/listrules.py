from Mystream import Mystream
import tweepy
import secret_info as s

from selenium import webdriver
from bs4 import BeautifulSoup
import re
import traceback


streaming_client = Mystream(s.bearer_token)
# print(streaming_client.get_rules().data[0].id)
rules = []
for ruleid in streaming_client.get_rules().data:
    rules.append(ruleid)
print(rules)
