#!/usr/bin/env bash

set -euv -o pipefail

# BUCKET=$(aws ssm get-parameter --name /trends/bucket | jq -r .Parameter.Value)
BUCKET=deployment-fdx-202204-cf
aws s3 cp s3://$BUCKET/local_pypi/local_pypi.zip ./

unzip local_pypi.zip

sudo pip3 install --upgrade \
-i file://$PWD/local_pypi/simple \
--extra-index https://pypi.org/simple \
'twitter-trends'

rm -rf local_pypi local_pypi.zip
