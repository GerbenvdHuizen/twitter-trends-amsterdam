import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

import boto3
import tweepy
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO)

firehose_client = boto3.client("firehose")

# Amsterdam search query params
GENERAL_SEARCH_TERM = "lang:nl"
GEOCODE = "52.36097,4.88872,25km"

# Twitter API
BATCH_SIZE = 100
PAGE_AMOUNT = 100


def get_param(name: str, required: bool = False):
    """
    Get a parameter from aws ssm
    :param name: Name of parameter
    :param required: If set True, error is raised when it does not exist, otherwise None is returned
    :return:
    """
    ssm = boto3.client("ssm")
    try:
        response = ssm.get_parameter(
            Name=name,
            WithDecryption=True,
        )
        return response.get("Parameter", {}).get("Value")
    except ClientError as e:
        if required:
            # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/error-handling.html
            logging.warning(f"Failed to get SSM param '{name}'. ({e.response['Error']['Code']})", exc_info=e)
            raise e
        logging.info(f"Failed to get SSM param '{name}', using default. ({e.response['Error']['Code']})")
        return None


DELIVERYSTREAM_NAME = get_param("/test/trends/deliverystream")
TREND_FUNCTION_NAME = get_param("/test/trends/trend-lambda")

CONSUMER_KEY = get_param("/test/trends/consumer-key")
CONSUMER_SECRET = get_param("/test/trends/consumer-secret")
ACCESS_TOKEN_KEY = get_param("/test/trends/access-token-key")
ACCESS_TOKEN_SECRET = get_param("/test/trends/access-token-secret")


def create_api():
    consumer_key = os.getenv("CONSUMER_KEY", CONSUMER_KEY)
    consumer_secret = os.getenv("CONSUMER_SECRET", CONSUMER_SECRET)
    access_token = os.getenv("ACCESS_TOKEN", ACCESS_TOKEN_KEY)
    access_token_secret = os.getenv("ACCESS_TOKEN_SECRET", ACCESS_TOKEN_SECRET)

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth, wait_on_rate_limit=True)
    try:
        api.verify_credentials()
    except Exception as e:
        logging.error("Error creating API", exc_info=True)
        raise e
    logging.info("API created")
    return api


@dataclass
class Record:
    created_at: str
    text: str
    entities: Dict[str, Any]
    location: Optional[str]
    geo: Optional[Dict[str, Any]]

    @classmethod
    def from_dict(cls, status):
        try:
            record_object = {
                "created_at": status["created_at"],
                "text": status["text"],
                "entities": status["entities"],
                "location": status["location"],
                "geo": status["geo"],
            }
        except Exception as e:
            print(e)
            return None
        return cls(**record_object)


def process_page(page):
    records = []
    for status in page:
        # record = Record.from_dict(status._json)
        records.append({"Data": bytes((json.dumps(status._json) + "\n"), "utf-8")})

    firehose_client.put_record_batch(DeliveryStreamName=DELIVERYSTREAM_NAME, Records=records)


def lambda_handler(event, context):
    try:
        date = event["date"]
    except KeyError:
        date = datetime.now(timezone.utc)
    utc_time = date.replace(tzinfo=timezone.utc)
    datetime_wayback = utc_time - timedelta(days=1)
    until_date = datetime_wayback.strftime("%Y-%m-%d")

    # Create API object
    api = create_api()

    for page in tweepy.Cursor(
        api.search_tweets, q=GENERAL_SEARCH_TERM, geocode=GEOCODE, until=until_date, count=BATCH_SIZE
    ).pages(PAGE_AMOUNT):
        process_page(page)

    boto3.client("lambda").invoke(
        FunctionName=TREND_FUNCTION_NAME,
        InvocationType="Event",
        Payload=bytes(json.dumps({"trend_output_directory": utc_time.strftime("%Y-%m-%d")}), "utf-8"),
    )
