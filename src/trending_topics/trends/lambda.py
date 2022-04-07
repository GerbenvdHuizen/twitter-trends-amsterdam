import logging

import boto3
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO)
client = boto3.client("emr")


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


DATA_BUCKET = get_param("/test/trends/bucketname")
DEPLOYMENT_BUCKET = get_param("/test/trends/deployment-bucketname")
EMR_ROLE = get_param("/test/trends/emr-role")
EMR_EC2_ROLE = get_param("/test/trends/emr-ec2-role")
RECORD_DIR = get_param("/test/trends/record-dir")


def lambda_handler(event, context):
    try:
        trend_output_directory = event["trend_output_directory"]
    except KeyError:
        raise ValueError("Missing date value")

    client.run_job_flow(
        Name="spark_job_cluster_trends",
        LogUri=f"s3://{DATA_BUCKET}/prefix/logs",
        ReleaseLabel="emr-6.3.0",
        Applications=[{"Name": "Spark"}],
        BootstrapActions=[
            {
                "Name": "Install Trend spotter dependencies",
                "ScriptBootstrapAction": {"Path": f"s3://{DEPLOYMENT_BUCKET}/local_pypi/install-trends.sh"},
            }
        ],
        Configurations=[
            {
                "Classification": "spark-env",
                "Configurations": [
                    {
                        "Classification": "export",
                        "Properties": {
                            "PYSPARK_PYTHON": "/usr/bin/python3",
                            "PYSPARK_DRIVER_PYTHON": "/usr/bin/python3",
                        },
                    }
                ],
            }
        ],
        VisibleToAllUsers=True,
        Instances={
            "MasterInstanceType": "m5.xlarge",
            "SlaveInstanceType": "m5.large",
            "InstanceCount": 1,
            "KeepJobFlowAliveWhenNoSteps": False,
            "TerminationProtected": False,
        },
        JobFlowRole=EMR_EC2_ROLE,
        ServiceRole=EMR_ROLE,
        Steps=[
            {
                "Name": "trend-analysis",
                "ActionOnFailure": "TERMINATE_CLUSTER",
                "HadoopJarStep": {
                    "Jar": "command-runner.jar",
                    "Args": [
                        "spark-submit",
                        "--deploy-mode",
                        "client",
                        "--master",
                        "yarn",
                        "/usr/local/lib/python3.7/site-packages/trending_topics/__main__.py",
                        "top_trends_extract",
                        "-input_path",
                        f"s3://{DATA_BUCKET}{RECORD_DIR}/*/*/*/*",
                        "-output_path",
                        f"s3://{DATA_BUCKET}/trends/{trend_output_directory}",
                    ],
                },
            }
        ],
    )
