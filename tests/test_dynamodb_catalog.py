import os
import pytest
import random

from intake import open_catalog
from intake_dynamodb import DynamoDBSource


@pytest.fixture
def catalog1():
    path = os.path.dirname(__file__)
    return open_catalog(os.path.join(path, "data", "catalog.yaml"))


@pytest.fixture(scope="function")
def example_small_table(dynamodb):
    table_name = "example-table"
    dynamodb.create_table(
        TableName=table_name,
        KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    dynamodb.put_item(
        TableName=table_name,
        Item={
            "id": {"S": "1"},
            "name": {"S": "John Doe"},
            "age": {"N": "30"},
        },
    )
    dynamodb.put_item(
        TableName=table_name,
        Item={
            "id": {"S": "2"},
            "name": {"S": "Jill Doe"},
            "age": {"N": "31"},
        },
    )


@pytest.fixture(scope="function")
def example_big_table(dynamodb):
    table_name = "example-table"
    dynamodb.create_table(
        TableName=table_name,
        KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    names = ["John Doe", "Ray", "Chester", "Chad", "Jason", "Emman"]
    for i in range(0, 50_000):
        dynamodb.put_item(
            TableName=table_name,
            Item={
                "id": {"S": str(i)},
                "name": {"S": random.choice(names)},
                "age": {"N": str(random.randint(20, 80))},
            },
        )


def test_dynamodb_source(example_big_table):
    source = DynamoDBSource(table_name="example-table", dynamodb=example_big_table)
    source._scan_table()
    print(source.npartitions)
    # print(source.npartitions)
    # assert isinstance(source, DynamoDBSource)
    # print(source)
    # print(source._scan_table())
    # print(self.npartitions)

    # source = catalog1['dynamodb_source'].get()
    # assert isinstance(source, DynamoDBSource)
