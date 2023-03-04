import warnings
from time import time
import re
from typing import List

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import explode, udf, size, col, concat_ws, to_date, to_timestamp, date_trunc, count
from pyspark.sql.types import StringType, ArrayType, Row


def extract_cash_tags(col_value: str) -> list[str]:
    return list(map(lambda x: str(x).upper(), re.findall(r"$(\w+)", col_value))) if col_value else []


def extract_hash_tags(col_value: str) -> list[str]:
    return list(map(lambda x: str(x).upper(), re.findall(r"#(\w+)", col_value))) if col_value else []


# change directories back -------------------------------------------------------------------------------------------------------------------
def process(spark: SparkSession):
    #print("Reading data")
    hashtag_df: DataFrame = spark.read.option("multiLine", "true").option("mode", "PERMISSIVE").json(
        "c_o/.*")

    cashtag_df: DataFrame = spark.read.option("multiLine", "true").option("mode", "PERMISSIVE").json(
        "h_o/.*")
    # print("Done")
    #hashtag_df = hashtag_df.union(cashtag_df)
    #print("Exploding tweets")
    tweets_hashtag_data_df = hashtag_df.select(
        explode("tweets")).select("col.data.*")
    tweets_cashtag_data_df = cashtag_df.select(
        explode("tweets")).select("col.data.*")
    # print("Done")

    #print("Extracting cash and hashtags")
    hashtags_df = tweets_hashtag_data_df.select("author_id", "created_at", "public_metrics.*", "text").withColumn(
        "tags", udf(extract_hash_tags, ArrayType(StringType()))("text"))

    cashtags_df = tweets_cashtag_data_df.select("author_id", "created_at", "public_metrics.*", "text").withColumn(
        "tags", udf(extract_cash_tags, ArrayType(StringType()))("text"))
    # print("Done")
    # print("Merging")
    #  merge two dataframe
    union_df = hashtags_df.union(cashtags_df).withColumn("created_at",
                                                         to_timestamp("created_at", "yyyy-MM-dd'T'HH:mm:ss.SSSX"))
    # print("Done")
    # print("Tags_df")
    tags_df = union_df.filter(size("tags") > 0).withColumn(
        "tag", explode("tags"))
    # 1 top 5 stock with count #$ only
    tags_df.drop("tags").createOrReplaceTempView("tag")
    stocks = ["AAPL", "MSFT", "AMZN", "TSLA", "GOOG", "GOOGL", "META", "NVDA", "PEP", "COST", "AVGO", "CSCO", "TMUS",
              "ADBE", "TXN", "CMCSA", "AMGN", "QCOM", "NFLX", "HON", "INTU", "INTC", "SBUX", "PYPL", "ADP", "AMD",
              "GILD", "MDLZ", "REGN", "ISRG", "VRTX", "ADI", "BKNG", "AMAT", "FISV", "CSX", "MU", "ATVI", "KDP", "CHTR",
              "MAR", "MRNA", "PANW", "ORLY", "ABNB", "MNST", "LRCX", "KHC", "SNPS", "AEP", "ADSK", "CDNS", "MELI",
              "CTAS", "FTNT", "PAYX", "KLAC", "BIIB", "DXCM", "NXPI", "EXC", "ASML", "LULU", "EA", "XEL", "MCHP",
              "CRWD", "MRVL", "AZN", "ILMN", "PCAR", "DLTR", "CTSH", "WDAY", "ROST", "ODFL", "WBA", "CEG", "IDXX",
              "TEAM", "VRSK", "FAST", "CPRT", "PDD", "SGEN", "SIRI", "DDOG", "LCID", "ZS", "JD", "EBAY", "VRSN", "ZM",
              "ANSS", "BIDU", "ALGN", "SWKS", "MTCH", "SPLK", "NTES", "DOCU", "OKTA"]
    #print("Creating tags_freq_df")
    tag_freq_df: DataFrame = spark.sql(
        "select upper(tag) as tag, count(*) as cnt from tag group by upper(tag) order by cnt desc")
    tag_freq_df = tag_freq_df.filter(col('tag').isin(stocks))
    #print("Step 2")
    # 2.  top 5 stock with count #$ , retweets feel free to choice either
    stock_tags_df = tags_df.filter(col('tag').isin(stocks)).drop_duplicates()
    # like_count|quote_count|reply_count|retweet_count|text|tag|
    stock_tags_df.createOrReplaceTempView("Tags")
    stocks_statistics = spark.sql(
        "with tmp_view as (select tag, count(*) as cnt ,sum(retweet_count) as retweets from Tags group by tag)" "select tag, cnt, retweets, cnt+retweets as final_result from tmp_view order by final_result desc")
    #print("Step 3")
    # 3.   most popular hashtags under tweets that contain the hashtag or cashtag of a particular stock by the hour.
    contain_text = tags_df.filter(col('tag').isin(stocks)).select("author_id", "created_at", "tags", "text",
                                                                  "tag").withColumn("ref_tag", explode("tags")).filter(
        ~col('ref_tag').isin(stocks)).drop_duplicates()
    # print("tags_hourly")
    tags_hourly = contain_text.withColumn(
        "t_hour", date_trunc('hour', 'created_at'))
    # write_db(tags_hourly.drop("tags"), "tags_hourly")

    tags_hourly.createOrReplaceTempView("tags_hourly")
    # tags_hourly.groupby("t_hour", "ref_tag").agg(count("ref_tag").alias("cnt")).show()
    #print("Tags hourly count")
    tags_hourly_count = spark.sql(
        "select t_hour, tag, ref_tag, count(*) as cnt from tags_hourly group by t_hour,tag, ref_tag order by t_hour desc, cnt desc")
    # tags_hourly_count.show()

    tags_hourly_count.createOrReplaceTempView("tags_hourly_count")
    #print("popular tags hourly count")
    popular_tags_hourly_count = spark.sql(
        "select * from (select *, row_number() over (PARTITION BY t_hour ORDER BY cnt desc) rank from tags_hourly_count) tmp where rank<=5 order by t_hour desc, cnt desc")
    # 3
    #print("Removing ebay")
    popular_tags_hourly_count = popular_tags_hourly_count.filter(
        ~col("tag").isin(['EBAY']))

    #print("Daily tags df")
    # 4 the volume of tweets about stocks daily, and get most popular stocks by day(top 5).
    tags_daily_df = tags_df.filter(col('tag').isin(stocks)).withColumn(
        "t_day", date_trunc('day', 'created_at'))
    # write_db(tags_daily_df, "tags_daily")

    tags_daily_df.createOrReplaceTempView("tags_daily")
    #print("tags dailt count")
    tags_daily_count = spark.sql(
        "select t_day, tag, count(*) as cnt from tags_daily group by t_day, tag order by t_day desc, cnt desc")

    tags_daily_count.createOrReplaceTempView("tags_daily_count")
    #print("tags daily top5 count")
    tags_daily_top5_count = spark.sql(
        "select * from (select *, row_number() over (PARTITION BY t_day ORDER BY cnt desc) rank from tags_daily_count) tmp where rank<=5 order by t_day desc, cnt desc")


warnings.filterwarnings('ignore')
times = []
for j in range(1, 10):
    localJ = f'local[{2}]'
    spark = SparkSession.builder.master(
        localJ).appName('workerTest').getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("FATAL")
    t0 = time()
    spark.catalog.clearCache()
    process(spark)
    print(f'{2} executors, time= {time() - t0}')
    times.append((j, time() - t0))
    spark.stop()
del spark

print(times)

# from time import time
# from pyspark import SparkContext
# for j in range(1,10):
#     sc = SparkContext(master=f'local[{j}]')
#     t0 = time()
#     for i in range(5):
#        sc.process()
#     print(f'{j} executors, time= {time() - t0}')
#     sc.stop()
