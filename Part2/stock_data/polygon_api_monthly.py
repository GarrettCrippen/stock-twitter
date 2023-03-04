from polygon import RESTClient
from typing import cast
from urllib3 import HTTPResponse
import pandas

client = RESTClient("vaC1psYvgXrGsW7GlszGrwkMaOwppG3D")

stocks = ["AAPL","MSFT","AMZN","TSLA","GOOG","GOOGL","META","NVDA","PEP","COST","AVGO","CSCO","TMUS","ADBE","TXN","CMCSA","AMGN","QCOM","NFLX","HON","INTU","INTC","SBUX","PYPL","ADP","AMD","GILD","MDLZ","REGN","ISRG","VRTX","ADI","BKNG","AMAT","FISV","CSX","MU","ATVI","KDP","CHTR","MAR","MRNA","PANW","ORLY","ABNB","MNST","LRCX","KHC","SNPS","AEP","ADSK","CDNS","MELI","CTAS","FTNT","PAYX","KLAC","BIIB","DXCM","NXPI","EXC","ASML","LULU","EA","XEL","MCHP","CRWD","MRVL","AZN","ILMN","PCAR","DLTR","CTSH","WDAY","ROST","ODFL","WBA","CEG","IDXX","TEAM","VRSK","FAST","CPRT","PDD","SGEN","SIRI","DDOG","LCID","ZS","JD","EBAY","VRSN","ZM","ANSS","BIDU","ALGN","SWKS","MTCH","SPLK","NTES","DOCU","OKTA"]

#Only weekdays are valid
date = "2022-11-0x"

f = open('stocks.json','a')
#5 calls/ min rate
aggs =client.get_grouped_daily_aggs(date =date)

for agg in aggs:
        if agg.ticker in stocks:
            f.write(f"{{\"ticker\": \"{agg.ticker}\",\n\"date\": \"{agg.date}\",\n\"time\": \"{stock_time}\",\n\"open\": {agg.open},\n\"high\": {agg.high},\n\"low\": {agg.low},\n\"close\": {agg.close},\n\"volume\": {agg.volume}\n}},")
f.close()
