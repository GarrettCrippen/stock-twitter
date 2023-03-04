import json
from datetime import datetime, timedelta

import mysql.connector
from django.http import HttpResponse
from pyecharts import options as opts
from pyecharts.charts import Bar, Bar3D, Timeline, Pie, Tab
# Create your views here.
def response_as_json(data):
   json_str = json.dumps(data)
   response = HttpResponse(
       json_str,
       content_type="application/json",
   )
   response["Access-Control-Allow-Origin"] = "*"
   return response


def json_response(data, code=200):
   data = {
       "code": code,
       "msg": "success",
       "data": data,
   }
   return response_as_json(data)


def json_error(error_string="error", code=500, **kwargs):
   data = {
       "code": code,
       "msg": error_string,
       "data": {}
   }
   data.update(kwargs)
   return response_as_json(data)


JsonResponse = json_response
JsonError = json_error

def execute_sql(sql):
   db_connection = mysql.connector.connect(user="group6", password="grp6", host="127.0.0.1", database='cs179g')
   db_cursor = db_connection.cursor()
   db_cursor.execute(sql)
   return db_cursor.fetchall()




def chart1_base() -> Bar:
   result = execute_sql(sql="SELECT * FROM tag_freq_new order by cnt desc LIMIT 5;")
   tags = list(map(lambda x: x[0], result))
   freqs = list(map(lambda x: x[1], result))
   c = (
       Bar()
           .add_xaxis(tags)
           .add_yaxis("frequency", freqs)
           .set_global_opts(title_opts=opts.TitleOpts(title="Stock Frequency"))
           .dump_options_with_quotes()
   )
   return c


#def chart1_base() -> Bar:
   #result = execute_sql(sql="SELECT * FROM tag_freq order by cnt desc;")
  # tags = list(map(lambda x: x[0], result))
   #freqs = list(map(lambda x: x[1], result))
  # c = (
    #   Bar()
     #      .add_xaxis(tags)
   #        .add_yaxis("frequency", freqs)
    #       .set_global_opts(title_opts=opts.TitleOpts(title="Stock Frequency"),
   #                         xaxis_opts=opts.AxisOpts(type_='category', name_rotate=180, max_interval=0),
   #                         datazoom_opts=opts.DataZoomOpts(range_start=0, range_end=10, is_zoom_lock=True))
   #        .dump_options_with_quotes()
  # )
  # return c


def chart2_base() -> Bar:
   result = execute_sql(sql="SELECT * FROM top5_stock_statistics_new order by final_result desc;")
   tags = list(map(lambda x: x[0], result))
   cnts = list(map(lambda x: x[1], result))
   retweets = list(map(lambda x: x[2], result))
   final_results = list(map(lambda x: x[3], result))
   c = (
       Bar()
           .add_xaxis(tags)
           .add_yaxis("cnt", cnts)
           .add_yaxis("retweet", retweets)
           .add_yaxis("final_result", final_results)
           .set_global_opts(title_opts=opts.TitleOpts(title="Stock Frequency & retweets & results"),
                            datazoom_opts=opts.DataZoomOpts(range_start=0, range_end=5, is_zoom_lock=True)
                            )
           .dump_options_with_quotes()
   )
   return c


def chart3_base() -> Bar:
   result = execute_sql(
       sql="SELECT t_hour, CONCAT(tag, '•', ref_tag) as stock, cnt FROM popular_tags_hourly_count_new where `rank`=1 order by t_hour ")
   days = list(set(list(map(lambda x: str(x[0])[:10], result))))
   days.sort()
   days=days[:-1]

   data = [[i, j, result[24 *j + i][2]] for i in range(24) for j in range(len(days))]
   # data2 = [[i, j, result[i * j][2], result[i * j][1]] for i in range(24) for j in range(len(days))]
   z_tags = [result[24 *j + i][1] for i in range(24) for j in range(len(days))]

   hours = ["12a", "1a", "2a", "3a", "4a", "5a", "6a", "7a", "8a", "9a", "10a", "11a", "12p", "1p", "2p", "3p", "4p", "5p", "6p",
            "7p", "8p", "9p", "10p", "11p"]

   bar3d = Bar3D(init_opts=opts.InitOpts(width="1600px", height="800px"))
   bar3d.add(series_name="cnt", data=data, shading="lambert",
             xaxis3d_opts=opts.Axis3DOpts(name="hour", type_="category", data=hours, interval=0),
             yaxis3d_opts=opts.Axis3DOpts(name="day", type_="category", data=days, interval=0),
             zaxis3d_opts=opts.Axis3DOpts(name="cnt", type_="value", data=z_tags)
             )
  

   global_opts = bar3d.set_global_opts(visualmap_opts=opts.VisualMapOpts(max_=250,
                                                                         range_color=["#313695", "#4575b4", "#74add1", "#abd9e9",
                                                                                      "#e0f3f8", "#ffffbf", "#fee090", "#fdae61",
                                                                                      "#f46d43", "#d73027", "#a50026"], ),
                                       # tooltip_opts=opts.TooltipOpts(formatter=JsCode(tooltipJS))
                                       )
   c = (
       global_opts.dump_options_with_quotes()
   )
   return c


def chart4_base() -> Bar:
   result = execute_sql(sql="SELECT * FROM popular_tags_daily_count_new order by t_day;")

   days = list(set(list(map(lambda x: str(x[0])[:10], result))))
   days.sort()

  
   begin = datetime.strptime(days[0], '%Y-%m-%d').date()
   end = datetime.strptime(days[-1], '%Y-%m-%d').date()

  
   tl = Timeline()
   tl.add_schema()

   for i in range((end - begin).days):
       day = begin + timedelta(days=i)
       tags_daily = [result[5 * i + j][1] for j in range(5)]
       cnt_daily = [result[5 * i + j][2] for j in range(5)]

       bar = (Pie()
              .add("", [list(z) for z in zip(tags_daily, cnt_daily)])
              .set_colors(["blue", "green", "yellow", "red", "pink"])
              .set_global_opts(title_opts=opts.TitleOpts(title="Stock Daily Count"))
              .set_series_opts(label_opts=opts.LabelOpts(formatter="{b}: {c}"))
              )

       tl.add(bar, day)
   c = (
       tl.dump_options_with_quotes()
   )
   return c
