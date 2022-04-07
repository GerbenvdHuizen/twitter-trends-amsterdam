# twitter-trends

## Deployment

1. Create an [AWS account](https://aws.amazon.com/premiumsupport/knowledge-center/create-and-activate-aws-account/).
2. Install [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html).
3. Setup [AWS credentials](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html) for CLI with a default or custom profile.
4. The `AWS_PROFILE` name will need to be configured in the release script `release.sh` and the cloudformation commands.
5. Finally run the `release.sh` script. First time deployment can take a few minutes.

## Run in the Cloud
The pipeline is kicked of daily at 01:00 am. The pipeline can be triggered manually by sending a test event to the `twitter-pipeline-function`. Due to EMR being slow in startup time, the pipeline takes about 7-8 min to fully complete.

## Run trends analysis or tests local
Running the trends analysis requires the following:
- A json file with tweet statuses on each line
- Installing the twitter-trends app in a local Python env
  - Create a python env with the method of choice (e.g. pyenv or conda)
  - `pip install poetry`
  - `poetry install -E spark`

### Run local
Command for local run:
```
python -m trending_topics top_trends_extract -input_path INPUT_PATH -output_path OUTPUT_PATH
```

### Run tests
Command for tests run:
```
poetry run pytest ./tests
```