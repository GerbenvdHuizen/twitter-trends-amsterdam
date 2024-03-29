import logging

import click
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from trending_topics.trends import trends

logging.basicConfig(level=logging.INFO)


def pipe(sdf, func, *args, **kwargs):
    """
    pipe allows us to rewrite

    func(sdf, *args, **kwargs)

    into

    sdf.pipe(func, *args, **kwargs)
    """
    return func(sdf, *args, **kwargs)


DataFrame.pipe = pipe


@click.command()
@click.argument("service", type=str, required=True)
@click.option("-input_path", default=None, type=str)
@click.option("-output_path", default=None, type=str)
@click.option("-interval_duration", default=1, type=int)
@click.option("-interval_unit", default="hours", type=str)
@click.option("-top_trending", default=10, type=int)
@click.option("-hashtags_or_words", default="hashtags", type=click.Choice(["hashtags", "words"]))
def pipeline(
    service,
    input_path,
    output_path,
    interval_duration,
    interval_unit,
    top_trending,
    hashtags_or_words,
) -> None:
    """
    Entrypoint for the trends app.
    :param service: str
        Currently only top_trends_extract is valid service (API could be added).
    :param input_path: str
    :param output_path: str
    :param interval_duration: int
    :param interval_unit: str
    :param top_trending: int
    :param hashtags_or_words: str
        Choose whether to do trend analysis on hashtags or words (default hashtags).
    """
    if service == "top_trends_extract":
        if not input_path or not output_path:
            logging.info("Cancel trends extraction process due to missing paths")
            return

        logging.info("Starting trends extraction process...")
        spark = SparkSession.builder.appName("twitter_trending_topics").getOrCreate()

        (
            trends.get_tweets_data(spark, input_path)
            .pipe(trends.extract_subjects, hashtags_or_words)
            .pipe(trends.get_tweet_topics_per_window, F.col(hashtags_or_words), interval_unit, interval_duration)
            .pipe(trends.get_top_n_trends, F.col("topics"), top_trending)
            .pipe(trends.write_output_path, output_path)
        )

        logging.info("Finished trend analysis process.")
    else:
        raise ValueError("Service name not found.")


if __name__ == "__main__":
    pipeline()
