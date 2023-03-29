import os
import pytest

from intake import open_catalog
from intake_dynamodb import DynamoDBSource


@pytest.fixture
def catalog1():
    path = os.path.dirname(__file__)
    return open_catalog(os.path.join(path, "data", "catalog.yaml"))


@pytest.fixture(scope="function")
def example_table(dynamodb):
    table_name = "example-table"
    dynamodb.create_table(
        TableName=table_name,
        KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    dynamodb.put_item(
        TableName=table_name, Item={"id": {"S": "1"}, "name": {"S": "John Doe"}}
    )


def test_dynamodb_source(example_table):
    source = DynamoDBSource(table_name="example-table", dynamodb=example_table)
    print(source)
    # source = catalog1['dynamodb_source'].get()
    # assert isinstance(source, DynamoDBSource)
