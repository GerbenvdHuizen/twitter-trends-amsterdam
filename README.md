# twitter-trends
- EMR cleanup rule
- Deployment bucket
- Delete project


aws cloudformation deploy \
  --template ./infrastructure/cf-template.yaml \
  --profile default \
  --region eu-west-1 \
  --stack-name test-stack-cf-trends \
  --capabilities CAPABILITY_NAMED_IAM

aws cloudformation deploy \
  --template ./infrastructure/deployment-bucket.yaml \
  --profile default \
  --region eu-west-1 \
  --stack-name deployment-bucket-trends \
  --capabilities CAPABILITY_NAMED_IAM