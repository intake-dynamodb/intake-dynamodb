import boto3
from moto import mock_dynamodb
import pytest


@pytest.fixture(scope="function")
def dynamodb():
    with mock_dynamodb():
        yield boto3.client("dynamodb")
