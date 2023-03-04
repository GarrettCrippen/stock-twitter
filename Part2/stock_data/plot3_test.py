from pyspark.sql import SparkSession, DataFrame
import plotly.express as px
import plotly.graph_objects as go
import numpy as np

spark = SparkSession.builder.appName('practice').config('spark.jars.packages', 'mysql:mysql-connector-java:8.0.30').master(
    "local").getOrCreate()

hourly_df = spark.read.format("jdbc").option("url", "jdbc:mysql://localhost:3306/cs179g") \
    .option("driver", "com.mysql.jdbc.Driver").option("dbtable", "stocks_hourly") \
    .option("user", "Garrett").option("password", "123").load()

monthly_df = spark.read.format("jdbc").option("url", "jdbc:mysql://localhost:3306/cs179g") \
    .option("driver", "com.mysql.jdbc.Driver").option("dbtable", "stocks_monthly") \
    .option("user", "Garrett").option("password", "123").load()

union_df = spark.read.format("jdbc").option("url", "jdbc:mysql://localhost:3306/cs179g") \
    .option("driver", "com.mysql.jdbc.Driver").option("dbtable", "union_df") \
    .option("user", "Garrett").option("password", "123").load()

stock_day = 17
stock_month = 10
stock_ticker = 'AAPL'

hourly_df.createOrReplaceTempView("current_stock")
monthly_df.createOrReplaceTempView("monthly")

current_stock = spark.sql(f"SELECT * from current_stock where day = {stock_day} and month = {stock_month} and ticker = '{stock_ticker}' order by hour")
current_stock = current_stock.toPandas()

daily_close = spark.sql(f"SELECT close from monthly where day = {stock_day} and month = {stock_month} and ticker = '{stock_ticker}' ").first()['close']

customdata  = np.stack((current_stock['open'], current_stock['high'], current_stock['low'], current_stock['volume'], current_stock['hour']), axis=-1)
fig = px.line(data_frame=current_stock,x='hour', y='close',title='hourly_stock',template='plotly')
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
                                                                                 '<extra> AAPL </extra>'
                 )
fig.update_layout(yaxis=dict(showgrid=False))

fig.show()