#!/usr/bin/env bash

mkdir -p lambda_zip

cp src/trending_topics/trends/lambda.py lambda.py
zip -r lambda_zip/trend_lambda.zip lambda.py
rm lambda.py

cp src/trending_topics/api/lambda.py lambda.py
zip -r lambda_zip/api_lambda.zip lambda.py
rm lambda.py

pip3 install --target ./package tweepy
cd package
zip -r ../lambda_zip/twitter_lambda.zip .
cd ..
cp src/trending_topics/twitter/lambda.py lambda.py
zip -g lambda_zip/twitter_lambda.zip lambda.py
rm lambda.py
rm -rf package