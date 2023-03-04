from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import explode, udf, size, col, concat_ws, to_date, to_timestamp, date_trunc, count, array_contains, month, dayofmonth, hour, explode_outer, split
from django.http import HttpResponse
import plotly.express as px
import plotly.graph_objects as go
import mysql.connector
import pandas
import pandasql
from django.shortcuts import render
from polls.forms import DateForm
import datetime


import numpy as np

def execute_sql(sql):
    db_connection = mysql.connector.connect(user="group6", password="grp6", host="127.0.0.1", database='cs179g')
    db_cursor = db_connection.cursor()
    # db_cursor.execute(sql)
    # df = pandas.DataFrame(db_cursor.fetchall())
    # df.columns = db_cursor.keys()
    # return df
    return pandas.read_sql_query(sql,con=db_connection)

def plotly_chart(request):

    #default params
    stock_day = 26
    stock_month = 10
    stock_ticker = 'TEAM'
    stock_year = 22
    
    date = request.GET.get('date')
    if(date):
        date = datetime.datetime.strptime(date, "%Y-%m-%d")
        stock_day = date.day
        stock_month = date.month
        stock_year = date.year
        stock_ticker = request.GET.get('ticker')
        print(stock_day,stock_month,stock_ticker)
    

    current_stock = execute_sql(f"SELECT * from stocks_hourly where day = {stock_day} and month = {stock_month} and ticker = '{stock_ticker}' order by hour")


    #some issue with our DB
    hourly_tags  = execute_sql(f"Select tags, times, hour from top_tags where tickers = '{stock_ticker}' and month = {stock_month} and day = {stock_day} group by tickers, times,tags, hour order by times DESC  ")
    
    tags_freq=[]

    text=[]
    for i in np.array(current_stock['hour']).reshape(-1,1):
        tmp = pandasql.sqldf(f"SELECT tags, times from hourly_tags where hour = {i[0]} order by times DESC limit 3",locals())
        text_hour = ""
        for j, row in tmp.iterrows():
            text=""
            for col in tmp.columns.values:
                if(col == 'tags'):
                    text += f"#{row[col]}: "
                else:
                    text += f"{row[col]}, "
            #print(i,text)
            text_hour+=text
        tags_freq.append(i)
        tags_freq.append([text_hour])

   

    tags_freq = np.array(tags_freq).reshape(-1,)

    tags_hour = pandas.Series(tags_freq[::2],name ='hour')

    tags_count = pandas.Series(tags_freq[1::2],name = 'times')

    tmp_frame = pandas.DataFrame(columns = ['hour', 'times'])
    tmp_frame['hour'] = tags_hour.astype(int)
    tmp_frame['times'] = tags_count

    current_stock=current_stock.merge(tmp_frame)

    current_stock.head(15)

    daily_close=0
    try:
        daily_close = execute_sql(f"SELECT close from stocks_monthly where day = {stock_day} and month = {stock_month} and ticker = '{stock_ticker}' ").loc[0]['close']
    except:
        print('no daily_close')
        
    customdata  = np.stack((current_stock['open'], current_stock['high'], current_stock['low'], current_stock['volume'], current_stock['times']), axis=-1)
    fig = px.line(data_frame=current_stock,x='hour', y='close',title=f'{stock_ticker}: {stock_month}/{stock_day}/{stock_year}',template='plotly')
    fig.update_layout(shapes=[
        # adds line at y=5
        dict(
          type= 'line',
          xref= 'paper', x0=0, x1=1,
          yref= 'y', y0= daily_close, y1= daily_close,
          line=dict(
            color="LightSeaGreen",
            width=2,
            dash="dashdot",
        )
        )
    ])

    fig.update_traces(customdata = customdata, hovertemplate=
                                                                                    'open: </b>%{customdata[0]: .2f} <br>' + \
                                                                                    'high: </b>%{customdata[1]: .2f} <br>' + \
                                                                                    'low: </b>%{customdata[2]: .2f} <br>' + \
                                                                                    'volume: </b>%{customdata[3]: ,} <br>'+ \
                                                                                     '<extra> %{customdata[4]} </extra>' 

                     )
    fig.update_layout(yaxis=dict(showgrid=False))

    config = {'displayModeBar': False}

    chart = fig.to_html(config=config)
    context = {'chart':chart,'form':DateForm()}
    return render(request,'index.html',context)

    # fig.write_html("../mysite2/templates/p.html",config=config)
