from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import re

chromeOptions =Options()
chromeOptions.headless=True
browser = webdriver.Chrome(executable_path='../drivers/chromedriver',options=chromeOptions)
browser.get('https://www.slickcharts.com/nasdaq100')
print('Headless browser Initiated')
soup = BeautifulSoup(browser.page_source, "html.parser")
browser.close()

stock_tags=[]
nasdaq_data = []

for element in soup.find_all('a',{'href':re.compile('^(/symbol/)')}):
    stock_tags.append(element.text)

stock_tags =  list(map(lambda x:'#'+x ,stock_tags[1::2]))


rule_size = 17
rules=[]
subsets = list([stock_tags[x:x+rule_size] for x in range(0, len(stock_tags), rule_size)])





for element in soup.find_all('td',{'class':'text-nowrap'}):
    nasdaq_data.append(element.text)

nasdaq_data = nasdaq_data[:nasdaq_data.index('S&P 500')]
price = nasdaq_data[::3]
chg = nasdaq_data[1::3]
p_chg = nasdaq_data[2::3]


print(price)
