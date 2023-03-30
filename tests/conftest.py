import boto3
import pytest
from moto import mock_dynamodb


@pytest.fixture(scope="function")
def dynamodb():
    with mock_dynamodb():
        yield boto3.client("dynamodb")
