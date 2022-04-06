import json
import boto3
import logging

from botocore.exceptions import ClientError


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


BUCKET = get_param("/test/trends/bucketname")


def lambda_handler(event, context):
    response = {
        "statusCode": 200,
        "body": json.dumps({}),
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": '*'
        },
    }

    try:
        date = event["queryStringParameters"]["date"]
    except KeyError:
        response["statusCode"] = 400
        return response

    try:
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(BUCKET)
        for object_summary in bucket.objects.filter(Prefix=f"trends/{date}"):
            key = object_summary.key
            if key.endswith(".json"):
                s3_object = s3.Object(BUCKET, key)
                s3_clientdata = s3_object.get()['Body'].read().decode('utf-8')
                response["body"] = json.dumps([json.loads(json_object)
                                               for json_object in iter(s3_clientdata.splitlines())])

        if not response["body"]:
            response["statusCode"] = 404

    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        response["body"] = json.dumps(e.response["Error"])
        if error_code == "AccessDenied":
            response["statusCode"] = 401
        elif error_code == "InvalidLocationConstraint":
            response["statusCode"] = 422
        else:
            response["statusCode"] = 400

    return response
