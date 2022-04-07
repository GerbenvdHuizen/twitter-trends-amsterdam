from pyspark.ml.feature import StopWordsRemover
from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

minutes = 60
hours = 60 * minutes
days = 24 * hours


def get_tweets_data(spark: SparkSession, path: str) -> DataFrame:
    tweets = (
        spark.read.json(path)
        .withColumn("created_at", F.to_timestamp(F.substring(F.col("created_at"), 5, 30), "MMM dd HH:mm:ss Z yyyy"))
        .filter(F.col("created_at") > ((F.unix_timestamp(F.current_timestamp()) - (3 * days)).cast("timestamp")))
    )
    return tweets


def get_hashtags(data: DataFrame) -> DataFrame:
    hashtags = (
        data.select("created_at", F.col("entities.hashtags.text").alias("hashtags"))
        .na.drop()
        .where(F.size(F.col("hashtags")) > 0)
    )
    return hashtags


def get_words(data: DataFrame) -> DataFrame:
    words = (
        data.filter(F.col("created_at").isNotNull() | F.col("text").isNotNull())
        .withColumn("text", F.lower(F.regexp_replace("text", "[^a-zA-Z\\s]", "")))
        .filter(F.trim(F.col("text")) != "")
        .withColumn("text", F.regexp_replace("text", "[\\s+]", " "))
        .withColumn("words_with_stopwords", F.split(F.col("text"), " "))
        .drop("text")
    )
    remover = StopWordsRemover(inputCol="words_with_stopwords", outputCol="words")
    removed_stopwords = remover.transform(words).drop("words_with_stopwords").where(F.size(F.col("words")) > 0)

    return removed_stopwords


def get_tweet_topics_per_window(
    data: DataFrame, topic_col: Column, period_unit: str = "hours", period: int = 1
) -> DataFrame:
    topic_by_window = (
        data.withColumn("topics", F.explode(topic_col))
        .withColumn("topics", F.lower(F.col("topics")))
        .groupBy(
            "topics",
            F.window("created_at", f"{period} {period_unit}").alias("interval"),
        )
        .count()
    )

    return topic_by_window


def get_top_n_trends(data: DataFrame, topic_col: Column, top_n: int = 5) -> DataFrame:
    window = Window.partitionBy(topic_col).orderBy("interval")
    sloped_data = (
        data.withColumn("count_lag", F.lag(F.col("count")).over(window))
        .withColumn(
            "count_lag",
            F.when(F.isnull(F.col("count_lag")), 0).otherwise(F.col("count_lag")),
        )
        .withColumn("previous_interval", F.lag(F.col("interval")).over(window))
        .withColumn(
            "is_sequential",
            F.when(
                F.col("interval").getItem("start") == F.col("previous_interval").getItem("end"),
                1,
            ).otherwise(0),
        )
        .withColumn(
            "slope",
            F.when(
                (F.col("is_sequential") == True) | (F.isnull(F.col("previous_interval"))),
                F.col("count") - F.col("count_lag"),
            ).otherwise(0),
        )
        .orderBy(F.col("slope").desc())
    )
    return sloped_data.select(topic_col, F.col("slope"), F.col("interval")).limit(top_n)


def write_output_path(data: DataFrame, output_path: str) -> None:
    try:
        data.coalesce(1).write.mode("overwrite").format("json").save(output_path)
    except Exception as e:
        print(f"{e}\nFailed to write an output file.")


def extract_subjects(tweets, hashtags_or_words):
    if hashtags_or_words == "hashtags":
        return get_hashtags(tweets)
    else:
        return get_words(tweets)
