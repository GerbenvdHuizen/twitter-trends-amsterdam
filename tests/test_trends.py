from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from trending_topics.trends import trends


def are_dataframes_equal(df_actual, df_expected):

    if df_actual.schema != df_expected.schema:
        return False

    if df_actual.collect() != df_expected.collect():
        return False

    # sorts are needed in case if disordered columns
    a_cols = sorted(df_actual.columns)
    e_cols = sorted(df_expected.columns)
    # we don't know the column names so count on the first column we find
    df_a = df_actual.groupby(a_cols).agg(F.count(a_cols[1]))
    df_e = df_expected.groupby(e_cols).agg(F.count(e_cols[1]))
    # then perform our equality checks on the dataframes with the row counts
    if not df_a.subtract(df_e).rdd.isEmpty() or not df_e.subtract(df_a).rdd.isEmpty():
        return False
    return True


# --- Test get_hashtags ---


def test_get_hashtags(spark: SparkSession):
    input_df = spark.createDataFrame(
        [
            (
                datetime.fromisoformat("2011-02-25 14:04:49"),
                {"hashtags": [{"text": "test_hashtag_one"}, {"text": "test_hashtag_two"}]},
            ),
            (datetime.fromisoformat("2011-02-25 14:04:56"), {"hashtags": [{"text": "test_hashtag_random"}]}),
            (datetime.fromisoformat("2011-02-25 14:05:02"), {"hashtags": []}),
        ],
        T.StructType(
            [
                T.StructField("created_at", T.TimestampType(), True),
                T.StructField(
                    "entities",
                    T.StructType().add(
                        T.StructField(
                            "hashtags",
                            T.ArrayType(T.StructType().add(T.StructField("text", T.StringType(), True))),
                            True,
                        )
                    ),
                    True,
                ),
            ]
        ),
    )

    output_df = trends.get_hashtags(input_df)

    expected_output = spark.createDataFrame(
        [
            (datetime.fromisoformat("2011-02-25 14:04:49"), ["test_hashtag_one", "test_hashtag_two"]),
            (datetime.fromisoformat("2011-02-25 14:04:56"), ["test_hashtag_random"]),
        ],
        T.StructType(
            [
                T.StructField("created_at", T.TimestampType(), True),
                T.StructField("hashtags", T.ArrayType(T.StringType()), True),
            ]
        ),
    )

    assert are_dataframes_equal(output_df, expected_output)


# --- Test get_words ---


def test_get_words(spark: SparkSession):
    input_df = spark.createDataFrame(
        [
            (
                datetime.fromisoformat("2011-02-25 14:04:49"),
                "@67terremoto arf, arf, arf no arribu, no arribu... Si arribuuuu!!!",
            ),
            (datetime.fromisoformat("2011-02-25 14:04:56"), "感動を返せ王子wwwwww"),
            (
                datetime.fromisoformat("2011-02-25 14:05:02"),
                "RT @IAmSteveHarvey: Having a relationship with God is an on going process. You have to work at it.",
            ),
        ],
        T.StructType(
            [
                T.StructField("created_at", T.TimestampType(), True),
                T.StructField("text", T.StringType(), True),
            ]
        ),
    )

    output_df = trends.get_words(input_df)

    expected_output = spark.createDataFrame(
        [
            (
                datetime.fromisoformat("2011-02-25 14:04:49"),
                ["terremoto", "arf", "arf", "arf", "arribu", "arribu", "si", "arribuuuu"],
            ),
            (datetime.fromisoformat("2011-02-25 14:04:56"), ["wwwwww"]),
            (
                datetime.fromisoformat("2011-02-25 14:05:02"),
                ["rt", "iamsteveharvey", "relationship", "god", "going", "process", "work"],
            ),
        ],
        T.StructType(
            [
                T.StructField("created_at", T.TimestampType(), True),
                T.StructField("words", T.ArrayType(T.StringType()), True),
            ]
        ),
    )

    assert are_dataframes_equal(output_df, expected_output)


# --- get_tweet_topics_per_window ---


def test_one_hashtag_in_timeframe(spark: SparkSession):

    input_df = spark.createDataFrame(
        [(datetime.fromisoformat("2011-02-25 15:04:49"), ["some_hashtag"])],
        T.StructType(  # Define the whole schema within a StructType
            [
                T.StructField("created_at", T.TimestampType(), True),
                T.StructField("hashtags", T.ArrayType(T.StringType()), True),
            ]
        ),
    )

    output_df = trends.get_tweet_topics_per_window(input_df, F.col("hashtags"))

    expected_output_df = spark.createDataFrame(
        [
            (
                "some_hashtag",
                {
                    "start": datetime.fromisoformat("2011-02-25 15:00:00"),
                    "end": datetime.fromisoformat("2011-02-25 16:00:00"),
                },
                1,
            )
        ],
        T.StructType(  # Define the whole schema within a StructType
            [
                T.StructField("topics", T.StringType(), True),
                T.StructField(
                    "interval",
                    T.StructType()
                    .add(T.StructField("start", T.TimestampType()))
                    .add(T.StructField("end", T.TimestampType())),
                    False,
                ),
                T.StructField("count", T.LongType(), False),
            ]
        ),
    )

    assert are_dataframes_equal(output_df, expected_output_df)


def test_multiple_hashtags_in_multiple_timeframes(spark: SparkSession):

    input_df = spark.createDataFrame(
        [
            (
                datetime.fromisoformat("2011-02-25 15:04:49"),
                ["some_hashtag", "another_hashtag", "another_one"],
            ),
            (
                datetime.fromisoformat("2011-02-25 16:04:49"),
                ["one_tag", "two_tag", "three_tag"],
            ),
            (datetime.fromisoformat("2011-02-25 23:04:49"), ["one_tag"]),
        ],
        T.StructType(  # Define the whole schema within a StructType
            [
                T.StructField("created_at", T.TimestampType(), True),
                T.StructField("hashtags", T.ArrayType(T.StringType()), True),
            ]
        ),
    )

    output_df = trends.get_tweet_topics_per_window(input_df, F.col("hashtags"))

    expected_output_df = spark.createDataFrame(
        [
            (
                "another_hashtag",
                {
                    "start": datetime.fromisoformat("2011-02-25 15:00:00"),
                    "end": datetime.fromisoformat("2011-02-25 16:00:00"),
                },
                1,
            ),
            (
                "another_one",
                {
                    "start": datetime.fromisoformat("2011-02-25 15:00:00"),
                    "end": datetime.fromisoformat("2011-02-25 16:00:00"),
                },
                1,
            ),
            (
                "some_hashtag",
                {
                    "start": datetime.fromisoformat("2011-02-25 15:00:00"),
                    "end": datetime.fromisoformat("2011-02-25 16:00:00"),
                },
                1,
            ),
            (
                "one_tag",
                {
                    "start": datetime.fromisoformat("2011-02-25 16:00:00"),
                    "end": datetime.fromisoformat("2011-02-25 17:00:00"),
                },
                1,
            ),
            (
                "three_tag",
                {
                    "start": datetime.fromisoformat("2011-02-25 16:00:00"),
                    "end": datetime.fromisoformat("2011-02-25 17:00:00"),
                },
                1,
            ),
            (
                "two_tag",
                {
                    "start": datetime.fromisoformat("2011-02-25 16:00:00"),
                    "end": datetime.fromisoformat("2011-02-25 17:00:00"),
                },
                1,
            ),
            (
                "one_tag",
                {
                    "start": datetime.fromisoformat("2011-02-25 23:00:00"),
                    "end": datetime.fromisoformat("2011-02-26 00:00:00"),
                },
                1,
            ),
        ],
        T.StructType(  # Define the whole schema within a StructType
            [
                T.StructField("topics", T.StringType(), True),
                T.StructField(
                    "interval",
                    T.StructType()
                    .add(T.StructField("start", T.TimestampType()))
                    .add(T.StructField("end", T.TimestampType())),
                    False,
                ),
                T.StructField("count", T.LongType(), False),
            ]
        ),
    )

    assert are_dataframes_equal(output_df, expected_output_df)


def test_multiple_hashtags_in_same_timeframe(spark: SparkSession):

    input_df = spark.createDataFrame(
        [
            (
                datetime.fromisoformat("2011-02-25 15:04:49"),
                ["some_hashtag", "one_tag", "another_one"],
            ),
            (
                datetime.fromisoformat("2011-02-25 15:24:49"),
                ["one_tag", "two_tag", "three_tag"],
            ),
            (datetime.fromisoformat("2011-02-25 15:54:49"), ["one_tag"]),
        ],
        T.StructType(  # Define the whole schema within a StructType
            [
                T.StructField("created_at", T.TimestampType(), True),
                T.StructField("hashtags", T.ArrayType(T.StringType()), True),
            ]
        ),
    )

    output_df = trends.get_tweet_topics_per_window(input_df, F.col("hashtags"))

    expected_output_df = spark.createDataFrame(
        [
            (
                "one_tag",
                {
                    "start": datetime.fromisoformat("2011-02-25 15:00:00"),
                    "end": datetime.fromisoformat("2011-02-25 16:00:00"),
                },
                3,
            ),
            (
                "another_one",
                {
                    "start": datetime.fromisoformat("2011-02-25 15:00:00"),
                    "end": datetime.fromisoformat("2011-02-25 16:00:00"),
                },
                1,
            ),
            (
                "some_hashtag",
                {
                    "start": datetime.fromisoformat("2011-02-25 15:00:00"),
                    "end": datetime.fromisoformat("2011-02-25 16:00:00"),
                },
                1,
            ),
            (
                "two_tag",
                {
                    "start": datetime.fromisoformat("2011-02-25 15:00:00"),
                    "end": datetime.fromisoformat("2011-02-25 16:00:00"),
                },
                1,
            ),
            (
                "three_tag",
                {
                    "start": datetime.fromisoformat("2011-02-25 15:00:00"),
                    "end": datetime.fromisoformat("2011-02-25 16:00:00"),
                },
                1,
            ),
        ],
        T.StructType(  # Define the whole schema within a StructType
            [
                T.StructField("topics", T.StringType(), True),
                T.StructField(
                    "interval",
                    T.StructType()
                    .add(T.StructField("start", T.TimestampType()))
                    .add(T.StructField("end", T.TimestampType())),
                    False,
                ),
                T.StructField("count", T.LongType(), False),
            ]
        ),
    )

    assert are_dataframes_equal(output_df, expected_output_df)


# --- get_top_n_trends ---


def test_get_top_n_trends(spark: SparkSession):

    input_df = spark.createDataFrame(
        [
            (
                "some_hashtag",
                {
                    "start": datetime.fromisoformat("2011-02-25 15:00:00"),
                    "end": datetime.fromisoformat("2011-02-25 16:00:00"),
                },
                1,
            ),
            (
                "some_hashtag",
                {
                    "start": datetime.fromisoformat("2011-02-25 16:00:00"),
                    "end": datetime.fromisoformat("2011-02-25 17:00:00"),
                },
                50,
            ),
            (
                "another_hashtag",
                {
                    "start": datetime.fromisoformat("2011-02-26 15:00:00"),
                    "end": datetime.fromisoformat("2011-02-26 16:00:00"),
                },
                5,
            ),
            (
                "another_hashtag",
                {
                    "start": datetime.fromisoformat("2011-02-26 16:00:00"),
                    "end": datetime.fromisoformat("2011-02-26 17:00:00"),
                },
                20,
            ),
            (
                "plus_de_hashtag",
                {
                    "start": datetime.fromisoformat("2011-02-27 15:00:00"),
                    "end": datetime.fromisoformat("2011-02-27 16:00:00"),
                },
                7,
            ),
        ],
        T.StructType(  # Define the whole schema within a StructType
            [
                T.StructField("topics", T.StringType(), True),
                T.StructField(
                    "interval",
                    T.StructType()
                    .add(T.StructField("start", T.TimestampType()))
                    .add(T.StructField("end", T.TimestampType())),
                    True,
                ),
                T.StructField("count", T.LongType(), False),
            ]
        ),
    )

    output_df = trends.get_top_n_trends(input_df, F.col("topics")).drop(F.col("interval"))

    expected_output_df = spark.createDataFrame(
        [
            ("some_hashtag", 49),
            ("another_hashtag", 15),
            ("plus_de_hashtag", 7),
            ("another_hashtag", 5),
            ("some_hashtag", 1),
        ],
        T.StructType(
            [
                T.StructField("topics", T.StringType(), True),
                T.StructField("slope", T.LongType(), True),
            ]
        ),
    )

    assert are_dataframes_equal(output_df, expected_output_df)
