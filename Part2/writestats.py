# Identifying tweets by the stock they are talking about before we drop the tweets in the conversation.
# Ctrl+f "Wyatt" to see my comments on the code.
import warnings
from time import time
import re
from typing import List

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import explode, udf, size, col, concat_ws, to_date, to_timestamp, date_trunc, count, array_contains, month, dayofmonth, hour, explode_outer
from pyspark.sql.types import StringType, ArrayType, Row

stocks = ["AAPL", "MSFT", "AMZN", "TSLA", "GOOG", "GOOGL", "META", "NVDA", "PEP", "COST", "AVGO", "CSCO", "TMUS",
          "ADBE", "TXN", "CMCSA", "AMGN", "QCOM", "NFLX", "HON", "INTU", "INTC", "SBUX", "PYPL", "ADP", "AMD",
          "GILD", "MDLZ", "REGN", "ISRG", "VRTX", "ADI", "BKNG", "AMAT", "FISV", "CSX", "MU", "ATVI", "KDP", "CHTR",
          "MAR", "MRNA", "PANW", "ORLY", "ABNB", "MNST", "LRCX", "KHC", "SNPS", "AEP", "ADSK", "CDNS", "MELI",
          "CTAS", "FTNT", "PAYX", "KLAC", "BIIB", "DXCM", "NXPI", "EXC", "ASML", "LULU", "EA", "XEL", "MCHP",
          "CRWD", "MRVL", "AZN", "ILMN", "PCAR", "DLTR", "CTSH", "WDAY", "ROST", "ODFL", "WBA", "CEG", "IDXX",
          "TEAM", "VRSK", "FAST", "CPRT", "PDD", "SGEN", "SIRI", "DDOG", "LCID", "ZS", "JD", "EBAY", "VRSN", "ZM",
          "ANSS", "BIDU", "ALGN", "SWKS", "MTCH", "SPLK", "NTES", "DOCU", "OKTA"]

cashtag_extract = "\$(?!((AAPL|MSFT|AMZN|TSLA|GOOG|GOOGL|META|NVDA|PEP|COST|AVGO|CSCO|TMUS|ADBE|TXN|CMCSA|AMGN|QCOM|NFLX|HON|INTU|INTC|SBUX|PYPL|ADP|AMD|GILD|MDLZ|REGN|ISRG|VRTX|ADI|BKNG|AMAT|FISV|CSX|MU|ATVI|KDP|CHTR|MAR|MRNA|PANW|ORLY|ABNB|MNST|LRCX|KHC|SNPS|AEP|ADSK|CDNS|MELI|CTAS|FTNT|PAYX|KLAC|BIIB|DXCM|NXPI|EXC|ASML|LULU|EA|XEL|MCHP|CRWD|MRVL|AZN|ILMN|PCAR|DLTR|CTSH|WDAY|ROST|ODFL|WBA|CEG|IDXX|TEAM|VRSK|FAST|CPRT|PDD|SGEN|SIRI|DDOG|LCID|ZS|JD|EBAY|VRSN|ZM|ANSS|BIDU|ALGN|SWKS|MTCH|SPLK|NTES|DOCU|OKTA)(?![a-z|A-Z])))\w+"
hashtag_extract = "\#(?!((AAPL|MSFT|AMZN|TSLA|GOOG|GOOGL|META|NVDA|PEP|COST|AVGO|CSCO|TMUS|ADBE|TXN|CMCSA|AMGN|QCOM|NFLX|HON|INTU|INTC|SBUX|PYPL|ADP|AMD|GILD|MDLZ|REGN|ISRG|VRTX|ADI|BKNG|AMAT|FISV|CSX|MU|ATVI|KDP|CHTR|MAR|MRNA|PANW|ORLY|ABNB|MNST|LRCX|KHC|SNPS|AEP|ADSK|CDNS|MELI|CTAS|FTNT|PAYX|KLAC|BIIB|DXCM|NXPI|EXC|ASML|LULU|EA|XEL|MCHP|CRWD|MRVL|AZN|ILMN|PCAR|DLTR|CTSH|WDAY|ROST|ODFL|WBA|CEG|IDXX|TEAM|VRSK|FAST|CPRT|PDD|SGEN|SIRI|DDOG|LCID|ZS|JD|EBAY|VRSN|ZM|ANSS|BIDU|ALGN|SWKS|MTCH|SPLK|NTES|DOCU|OKTA)(?![a-z|A-Z])))\w+"


def make_regex():
    regex = "((\$|\#|\＃)("
    for stock in stocks:
        regex = regex + stock + "|"
    regex = regex[:-1]
    regex = regex + ")(?![a-z|A-Z]))"
    return regex


ticker_matcher = make_regex()
# print(ticker_matcher)
ticker_matcher = re.compile(ticker_matcher, re.IGNORECASE)


def write_db(save_df: DataFrame, table_name: str, mode="overwrite"):
    save_df.write.format("jdbc") \
        .mode(mode) \
        .option("url", "jdbc:mysql://127.0.0.1:3306/cs179g") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", f"{table_name}") \
        .option("user", "group6") \
        .option("batchsize", "100000") \
        .option("password", "grp6").save()


def extract_hash_tags(col_value: str) -> list[str]:
    return list(map(lambda x: str(x).upper(), re.findall(r"#(\w+)", col_value))) if col_value else []


def extract_hashtags_u(col_value: str) -> list[str]:
    return list(map(str(col_value).upper(), re.findall(r"\#(\w+)", col_value))) if col_value else []


def tweets_to_tickers(tweets, baseTweetText) -> list[str]:
    if tweets is None:
        return None
    if baseTweetText is None:
        return None
    tweets_text = baseTweetText
    # print(f"\nBase tweet's text:{tweets_text}")
    for item in tweets:
        tweets_text = tweets_text + "     " + item.text
    tweets_text = tweets_text + " "
    # print(f"\nConversation's text:{tweets_text}")

    stockList = ticker_matcher.findall(tweets_text)
    _stockList = []
    stockListEmpty = 1
    for item in stockList:
        _stockList.append(item[2].upper())
        stockListEmpty = 0
    stockList = [*set(_stockList)]
    stockList.sort()
    # if stockListEmpty:
    #     print(f"\n\nStockList empty!:\n\n{tweets_text}\n\n")
    # print(stockList)
    return stockList


extract_hashtags_UDF = udf(
    lambda x: extract_hashtags_u(x), ArrayType(StringType()))

tweets_to_tickers_UDF = udf(
    lambda x, y: tweets_to_tickers(x, y), ArrayType(StringType()))


# change directories back -------------------------------------------------------------------------------------------------------------------
def process(spark: SparkSession, test=False):
    # print("Reading data")
    hashtag_loc = ""
    cashtag_loc = ""
    if test:
        hashtag_loc = f"h_o/1/*.json"
        cashtag_loc = f"c_o/1/*.json"
    else:
        hashtag_loc = f"hashtag_output/*.json"
        cashtag_loc = f"cashtag_output/*.json"
    time_read = time()

    # hashtag_df: DataFrame = spark.read.option("multiLine", "true").option("mode", "PERMISSIVE").json(
    #     hashtag_loc)

    cashtag_df: DataFrame = spark.read.option("multiLine", "true").option("mode", "PERMISSIVE").json(
        cashtag_loc)

    time_read = time() - time_read
    # print("Done")
    # hashtag_df = hashtag_df.union(cashtag_df)
    # print("Exploding tweets")
    # Wyatt: We need to select col.includes.tweets as well to get the context of a tweet.
    time_explode = time()
    # tweets_hashtag_data_df = hashtag_df.select(
    #     explode("tweets")).select("col.data.*", "col.includes.tweets")
    tweets_cashtag_data_df = cashtag_df.select(
        explode("tweets")).select("col.data.*", "col.includes.tweets")
    time_explode = time() - time_explode
    # print("Done")
    time_identify = time()
    # tweets_hashtag_identified_df = tweets_hashtag_data_df.withColumn(
    #     "tickers", tweets_to_tickers_UDF(col("tweets"), col("text")))
    tweets_cashtag_identified_df = tweets_cashtag_data_df.withColumn(
        "tickers", tweets_to_tickers_UDF(col("tweets"), col("text")))
    time_identify = time() - time_identify

    # print("Extracting hashtags")
    time_hashextract = time()
    # hashtags_df = tweets_hashtag_identified_df.select("author_id", "created_at", "public_metrics.*", "text", "tickers").withColumn(
    #     "tags", udf(extract_hash_tags, ArrayType(StringType()))("text"))

    cashtags_df = tweets_cashtag_identified_df.select("author_id", "created_at", "public_metrics.*", "text", "tickers").withColumn(
        "tags", udf(extract_hash_tags, ArrayType(StringType()))("text"))
    time_hashextract = time() - time_hashextract

    # Filtering out results from seperate cashtag and hashtag dataframes happens here
    # count1 = hashtags_df.count()

    # hashtags_filtered_df = hashtags_df
    time_filter = time()
    # hashtags_filtered_df = hashtags_filtered_df.where(~array_contains(col("tickers"), "EBAY") | (
    #     ~array_contains(col("tags"), "BOUTIQUE") & ~array_contains(col("tags"), "SEARCHNCOLLECT") & ~array_contains(col("tags"), "ALIEXPRESS")))
    # hashtags_filtered_df = hashtags_filtered_df.where(~array_contains(
    #     col("tickers"), "EBAY") | ~hashtags_filtered_df.text.like("Check out%"))

    # hashtags_filtered_df = hashtags_filtered_df.where(~array_contains(
    #     col("tickers"), "MU") | (~array_contains(col("tags"), "EPEX")))

    # count2 = hashtags_filtered_df.count()

    # print(f"Filtered out {count1-count2} tweets from hastags_df.")
    # hashtags_df = hashtags_filtered_df
    time_filter = time() - time_filter

    #  merge two dataframe
    time_merge = time()
    # union_df = hashtags_df.union(cashtags_df).withColumn("created_at",
    #                                                      to_timestamp("created_at", "yyyy-MM-dd'T'HH:mm:ss.SSSX"))
    union_df = cashtags_df
    time_merge = time() - time_merge

    # union_df.where(size('tickers') == 0).show(10)

    # Now get popularity statistics of stocks
    time_rest = time()
    union_df = union_df.withColumn("month", month("created_at")).withColumn(
        "day", dayofmonth("created_at")).withColumn("hour", hour("created_at"))
    union_df = union_df.withColumn("created_at", to_timestamp(
        "created_at", "yyyy-MM-dd'T'HH:mm:ss.SSSX"))
    union_df = union_df.select("month", "day", "hour", "like_count", "quote_count",
                               "reply_count", "retweet_count", "text", "tickers", "tags", "created_at")
    # union_df.show(10)
    # dataframe.withColumn("song", concat_ws(",", $"song"))
    write_db(union_df.withColumn("tickers", concat_ws(",", union_df.tickers)
                                 ).withColumn("tags", concat_ws(",", union_df.tags)), "timing_union_df")
    time_tickers_tags_df = union_df.select(
        "month", "day", "hour", "tickers", "tags")
    write_db(time_tickers_tags_df.withColumn("tickers", concat_ws(",", time_tickers_tags_df.tickers)
                                             ).withColumn("tags", concat_ws(",", time_tickers_tags_df.tags)), "timing_time_tickers_tags_df")

    tickers_exploded_union_df = union_df.withColumn(
        "tickers", explode_outer("tickers"))
    tickers_exploded_union_df.createOrReplaceTempView("Tags")
    # union_df.createOrReplaceTempView("Tags")
    stocks_statistics = spark.sql(
        "with tmp_view as (select tickers, count(*) as cnt ,sum(retweet_count) as retweets from Tags group by tickers)"
        "select tickers, cnt, retweets, cnt+retweets as final_result from tmp_view order by final_result desc")
    write_db(stocks_statistics.withColumn("tickers", concat_ws(
        ",", stocks_statistics.tickers)), "top5_stock_statistics_new")
    print("top5_stock_statistics_new written to\n")
    tickers_only_df = tickers_exploded_union_df.select("tickers")
    tickers_only_df.createOrReplaceTempView("tickers_only")
    tag_freq_new_df: DataFrame = spark.sql(
        "select upper(tickers) as ticker, count(*) as cnt from tickers_only group by upper(tickers) order by cnt desc")
    write_db(tag_freq_new_df.select("ticker", "cnt").limit(5), "tag_freq_new")
    print("tag_freq_new written to\n")

    # Table with month, day, and ticker
    ticker_month_day = tickers_exploded_union_df.withColumn("t_day", date_trunc('day', 'created_at')).select(
        "t_day", tickers_exploded_union_df.tickers)
    ticker_month_day = ticker_month_day.where("t_day IS NOT NULL")
    ticker_month_day.createOrReplaceTempView("ticker_month_day")
    tickers_daily_count = spark.sql(
        "select t_day, tickers as tag, count(*) as cnt from ticker_month_day group by t_day, tag order by t_day desc, cnt desc")
    days_list = spark.sql(
        "select t_day, count(*) as cnt from ticker_month_day group by t_day order by t_day desc")
    write_db(days_list, "test_days_list")

    tickers_daily_count.createOrReplaceTempView("tags_daily_count")
    tags_daily_top5_count = spark.sql(
        "select * from (select *, row_number() over (PARTITION BY t_day ORDER BY cnt desc) rank from tags_daily_count) tmp where rank<=5 order by t_day desc, cnt desc")
    write_db(tags_daily_top5_count, "popular_tags_daily_count_new")
    print("popular_tags_daily_count_new written to\n")

    hour_ticker_tag = tickers_exploded_union_df.withColumn(
        "tags", explode_outer("tags"))
    hour_ticker_tag = hour_ticker_tag.withColumn("t_hour", date_trunc(
        "hour", "created_at")).select("t_hour", "tickers", "tags")

    hour_ticker_tag.createOrReplaceTempView("tags_hourly")
    hour_ticker_tag_count = spark.sql(
        "select t_hour, tickers as tag, tags as ref_tag, count(*) as cnt from tags_hourly group by t_hour,tag,ref_tag order by t_hour desc, cnt desc")
    hour_ticker_tag_count = hour_ticker_tag_count.where(
        "t_hour IS NOT NULL AND tag IS NOT NULL and ref_tag IS NOT NULL AND ref_tag <> 'STOCKS'")
    hour_ticker_tag_count.createOrReplaceTempView("tags_hourly_count")
    popular_tags_hourly_count = spark.sql(
        "select * from (select *, row_number() over (PARTITION BY t_hour ORDER BY cnt desc) rank from tags_hourly_count) tmp where rank<=5 order by t_hour desc, rank")
    # popular_tags_hourly_count = popular_tags_hourly_count.where(
    #     "t_hour IS NOT NULL AND tag IS NOT NULL and ref_tag IS NOT NULL")
    write_db(popular_tags_hourly_count, "popular_tags_hourly_count_new")
    # tags_hourly = contain_text.withColumn("t_hour", date_trunc('hour', 'created_at'))
    # # write_db(tags_hourly.drop("tags"), "tags_hourly")

    # tags_hourly.createOrReplaceTempView("tags_hourly")
    # # tags_hourly.groupby("t_hour", "ref_tag").agg(count("ref_tag").alias("cnt")).show()
    # tags_hourly_count = spark.sql(
    # "select t_hour, tag, ref_tag, count(*) as cnt from tags_hourly group by t_hour,tag, ref_tag order by t_hour desc, cnt desc").filter(~col("tag").isin(['EBAY']))
    # #tags_hourly_count.show()

    # tags_hourly_count.createOrReplaceTempView("tags_hourly_count")
    # popular_tags_hourly_count=spark.sql(
    # "select * from (select *, row_number() over (PARTITION BY t_hour ORDER BY cnt desc) rank from tags_hourly_count) tmp where rank<=5 order by t_hour desc, rank")

    # tags_daily_df.createOrReplaceTempView("tags_daily")

    #  time_tickers_tags_df.show(30)
    # time_tickers_tags_explode_df = time_tickers_tags_df.select()

    # print("Time tickers tags df show november")
    # time_tickers_tags_df.filter(col('month') != 10).filter(
    #     col('hour') == 3).show(30)

    union_df.createOrReplaceTempView("union")

    ticker_freq_df: DataFrame = spark.sql(
        "select tickers, count(*) as cnt from union group by tickers order by cnt desc")
    write_db(ticker_freq_df.withColumn("tickers", concat_ws(",", ticker_freq_df.tickers)
                                       ), "timing_ticker_freq_df")

    # ticker_freq_df.show(10)

    all_tickers_df = union_df.select(
        explode_outer("tickers").alias("exploded"))

    # all_tickers_df.show(30)

    time_tickers_explode_df = time_tickers_tags_df.withColumn(
        "tickers", explode_outer("tickers"))
    # time_tickers_explode_df.show(30)
    time_tickers_tags_explode_df = time_tickers_explode_df.withColumn(
        "tags", explode_outer("tags"))
    # time_tickers_tags_explode_df.show(30)
    write_db(time_tickers_tags_explode_df.withColumn("tickers", concat_ws(",", time_tickers_tags_explode_df.tickers)
                                                     ).withColumn("tags", concat_ws(",", time_tickers_tags_explode_df.tags)), "timing_time_tickers_tags_explode_df")

    time_rest = time() - time_rest
    return (time_read, time_explode, time_identify, time_hashextract, time_filter, time_merge, time_rest)


warnings.filterwarnings('ignore')

# spark = SparkSession.builder.master(
#     localJ).appName('workerTest').getOrCreate()
localJ = f'local[{2}]'
spark = SparkSession.builder.master(
    localJ).appName('workerTest').config('spark.jars.packages', 'mysql:mysql-connector-java:8.0.30').getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("FATAL")
t0 = time()
spark.catalog.clearCache()
times = process(spark, False)
# print(f'{j} workers, times:')
# print(f'{times}\n')
spark.stop()
del spark


# from time import time
# from pyspark import SparkContext
# for j in range(1,10):
#     sc = SparkContext(master=f'local[{j}]')
#     t0 = time()
#     for i in range(5):
#        sc.process()
#     print(f'{j} executors, time= {time() - t0}')
#     sc.stop()
