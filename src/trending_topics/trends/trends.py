from pyspark.ml.feature import StopWordsRemover
from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

minutes = 60
hours = 60 * minutes
days = 24 * hours


def get_tweets_data(spark: SparkSession, path: str) -> DataFrame:
    """
    Get all json format tweet statuses from a directory.
    :param spark: SparkSession
    :param path: str
        Path to directory (S3 Bucket or other filesystem) with json tweets.
    :return: DataFrame
        Return a DataFrame with a converted timestamp Column. Only return the last
        72 hours of data based on the current_timestamp().
    """
    tweets = (
        spark.read.json(path)
        .withColumn("created_at", F.to_timestamp(F.substring(F.col("created_at"), 5, 30), "MMM dd HH:mm:ss Z yyyy"))
        .filter(F.col("created_at") > ((F.unix_timestamp(F.current_timestamp()) - (3 * days)).cast("timestamp")))
    )
    return tweets


def get_hashtags(data: DataFrame) -> DataFrame:
    """
    Creates a DataFrame of tweets with a list of their hashtags.
    :param data: DataFrame
        Contains tweets from the last 72 hours of available data.
    :return: DataFrame
        DataFrame of tweets with a hashtags column containing a list of known
        hashtags used. If no hashtags are available the tweet is filtered out.
    """
    hashtags = (
        data.select("created_at", F.col("entities.hashtags.text").alias("hashtags"))
        .na.drop()
        .where(F.size(F.col("hashtags")) > 0)
    )
    return hashtags


def get_words(data: DataFrame) -> DataFrame:
    """
    Creates a DataFrame of tweets with a list of words used in the tweet text.
    :param data: DataFrame
        Contains tweets from the last 72 hours of available data.
    :return: DataFrame
        DataFrame of tweets with a words column containing a list of known
        words used in the tweet text. Stopwords are filtered out and some regex
        replacement cleaning is applied.
    """
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
    """
    Creates a windows for tweets data and counts occurences of topics within
    the windows.
    :param data: DataFrame
        Contains tweets and the topics of the tweets.
    :param topic_col: Column
        Column containing the topics.
    :param period_unit: str
        Unit used for the window (e.g. hours).
    :param period: int
        Period/Amount used for the window.
    :return: DataFrame
        DataFrame where topics are grouped by a certain window and each occurence
        of the topic within the window is counted.
    """
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


def get_top_n_trends(data: DataFrame, topic_col: Column, top_n: int = 10) -> DataFrame:
    """
    Calculates the slope (trend) of a topic by counting the increase of occurences
    from window to window. Windows are checked for sequentiality. The final result
    is top N dataframe of trends and the largest slopes.
    :param data: DataFrame
        Contains topics grouped by window and counted.
    :param topic_col: Column
        Column containing the topics.
    :param top_n: int
        Top N amount of results displayed.
    :return: DataFrame
        DataFrame containing Top N results for trending topics based on their slope.
        The columns included in the final results are topic name, slope, and window.
    """
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
    """
    Writes a DataFrame to a json file in the output_path.
    :param data: DataFrame
        Contains the top N trending topics.
    :param output_path: str
        Path to write the json results to.
    :return:
    """
    try:
        data.coalesce(1).write.mode("overwrite").format("json").save(output_path)
    except Exception as e:
        print(f"{e}\nFailed to write an output file.")


def extract_subjects(tweets, hashtags_or_words) -> DataFrame:
    if hashtags_or_words == "hashtags":
        return get_hashtags(tweets)
    else:
        return get_words(tweets)
