#!/usr/bin/env bash

AWS_PROFILE=default
AWS_REGION=eu-west-1
BUCKET=$(aws ssm get-parameter --profile $AWS_PROFILE --region $AWS_REGION --name /test/trends/deployment-bucketname | jq -r .Parameter.Value)

source create_local_pypi.sh
source zip_lambdas.sh

aws s3 sync --profile $AWS_PROFILE --region $AWS_REGION ./lambda_zip s3://$BUCKET/lambdas --delete
aws s3 sync --profile $AWS_PROFILE --region $AWS_REGION ./local_pypi_zip s3://$BUCKET/local_pypi --delete