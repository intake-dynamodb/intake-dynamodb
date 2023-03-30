import os
import random
from decimal import Decimal

import dask.bag as db
import pandas._testing as pd_testing
import pytest
from dask.dataframe.utils import assert_eq as dask_dataframe_assert_eq
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
    for i in range(0, 2):
        dynamodb.put_item(
            TableName=table_name,
            Item={
                "id": {"S": str(i)},
                "name": {"S": ["John Doe", "Jill Doe"][i]},
                "age": {"N": str(i + 30)},
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
    names = ["John Doe", "Jill Doe"]
    for i in range(0, 50_000):
        dynamodb.put_item(
            TableName=table_name,
            Item={
                "id": {"S": str(i)},
                "name": {"S": random.choice(names)},
                "age": {"N": str(random.randint(20, 80))},
            },
        )


def test_dynamodb_source(example_small_table):
    source = DynamoDBSource(table_name="example-table", dynamodb=example_small_table)
    assert isinstance(source, DynamoDBSource)


def test_dynamodb_scan(example_small_table):
    source = DynamoDBSource(table_name="example-table", dynamodb=example_small_table)
    items = source._scan_table()
    assert items == [
        {"id": "0", "name": "John Doe", "age": Decimal("30")},
        {"id": "1", "name": "Jill Doe", "age": Decimal("31")},
    ]


def test_dynamodb_scan_with_filter(example_small_table):
    source = DynamoDBSource(
        table_name="example-table",
        dynamodb=example_small_table,
        filter_expression="age = :age_value",
        filter_expression_value=30,
    )
    items = source._scan_table()
    assert items == [{"id": "0", "name": "John Doe", "age": Decimal("30")}]


def test_dynamodb_to_dask(example_small_table):
    source = DynamoDBSource(table_name="example-table", dynamodb=example_small_table)
    actual_ddf = source.to_dask()
    expected_ddf = db.from_sequence(
        [
            {"id": "0", "name": "John Doe", "age": Decimal("30")},
            {"id": "1", "name": "Jill Doe", "age": Decimal("31")},
        ],
        npartitions=1,
    ).to_dataframe()
    dask_dataframe_assert_eq(actual_ddf, expected_ddf)


def test_dynamodb_read(example_small_table):
    source = DynamoDBSource(table_name="example-table", dynamodb=example_small_table)
    actual_df = source.read()
    expected_df = (
        db.from_sequence(
            [
                {"id": "0", "name": "John Doe", "age": Decimal("30")},
                {"id": "1", "name": "Jill Doe", "age": Decimal("31")},
            ],
            npartitions=1,
        )
        .to_dataframe()
        .compute()
    )
    pd_testing.assert_equal(actual_df, expected_df)
