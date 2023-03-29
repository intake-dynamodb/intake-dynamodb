import boto3
from moto import mock_dynamodb
import pytest


@pytest.fixture(scope="function")
def dynamodb():
    with mock_dynamodb():
        yield boto3.client("dynamodb")


# @mock_dynamodb
# def example_dynamodb():
#     dynamodb = boto3.client("dynamodb")
#     dynamodb.create_table(
#         TableName="my_table",
#         KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
#         AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
#     )

#     # Put an item into the table
#     dynamodb.put_item(
#         TableName="my_table", Item={"id": {"S": "1"}, "name": {"S": "John Doe"}}
#     )
