from polygon import RESTClient
from typing import cast
from urllib3 import HTTPResponse
from datetime import datetime
import time


client = RESTClient("vaC1psYvgXrGsW7GlszGrwkMaOwppG3D")

stocks = ["AAPL","MSFT","AMZN","TSLA","GOOG","GOOGL","META","NVDA","PEP","COST","AVGO","CSCO","TMUS","ADBE","TXN","CMCSA","AMGN","QCOM","NFLX","HON","INTU","INTC","SBUX","PYPL","ADP","AMD","GILD","MDLZ","REGN","ISRG","VRTX","ADI","BKNG","AMAT","FISV","CSX","MU","ATVI","KDP","CHTR","MAR","MRNA","PANW","ORLY","ABNB","MNST","LRCX","KHC","SNPS","AEP","ADSK","CDNS","MELI","CTAS","FTNT","PAYX","KLAC","BIIB","DXCM","NXPI","EXC","ASML","LULU","EA","XEL","MCHP","CRWD","MRVL","AZN","ILMN","PCAR","DLTR","CTSH","WDAY","ROST","ODFL","WBA","CEG","IDXX","TEAM","VRSK","FAST","CPRT","PDD","SGEN","SIRI","DDOG","LCID","ZS","JD","EBAY","VRSN","ZM","ANSS","BIDU","ALGN","SWKS","MTCH","SPLK","NTES","DOCU","OKTA"]

date = "2022-10-15"
date2 = "2022-11-14"

f = open('stocks.json','a')
#5 calls/ min

try:
    for stock in stocks:

        aggs =client.get_aggs(ticker = stock, from_ =date, to = date2, timespan = 'minute', multiplier=60, limit =50000, adjusted=False)
        for agg in aggs:
            dtime=datetime.fromtimestamp((agg.timestamp/1000))
            dt=dtime.strftime("%Y-%m-%d")
            stock_time = dtime.strftime("%H:%M")
            f.write(f"{{\"ticker\": \"{stock}\",\n\"date\": \"{dt}\",\n\"time\": \"{stock_time}\",\n\"open\": {agg.open},\n\"high\": {agg.high},\n\"low\": {agg.low},\n\"close\": {agg.close},\n\"volume\": {agg.volume}\n}},")

        print(len(aggs))
        time.sleep(65)
except Exception as e:
    print('error!!!!!!!!!!!!!!!')
    print(e)

f.close()
