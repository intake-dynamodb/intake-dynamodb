import boto3
import botocore
import pytest
from moto import mock_dynamodb


@pytest.fixture(scope="function")
def dynamodb() -> botocore.client.BaseClient:
    with mock_dynamodb():
        yield boto3.client("dynamodb")
