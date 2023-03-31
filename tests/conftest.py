import boto3
import botocore
import pytest
from moto import mock_dynamodb, mock_sts


@pytest.fixture(scope="function")
def dynamodb() -> botocore.client.BaseClient:
    with mock_dynamodb():
        yield boto3.client("dynamodb")


@pytest.fixture(scope="function")
def dynamodb_in_different_account() -> botocore.client.BaseClient:
    with mock_sts():
        with mock_dynamodb():
            region_name = "us-west-2"
            sts_client = boto3.client("sts", region_name=region_name)
            role_arn = "arn:aws:iam::ACCOUNT_ID:role/ROLE_NAME"
            assumed_role = sts_client.assume_role(
                RoleArn=role_arn,
                RoleSessionName="session",
            )
            yield boto3.client(
                "dynamodb",
                region_name="us-west-2",
                aws_access_key_id=assumed_role["Credentials"]["AccessKeyId"],
                aws_secret_access_key=assumed_role["Credentials"]["SecretAccessKey"],
                aws_session_token=assumed_role["Credentials"]["SessionToken"],
            )
