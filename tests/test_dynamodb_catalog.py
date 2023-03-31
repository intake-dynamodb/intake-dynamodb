import random
from decimal import Decimal

import botocore
import dask.bag as db
import dask.dataframe as dd
import intake
import pandas as pd
import pandas._testing as pd_testing
import pytest
from dask.dataframe.utils import assert_eq as dask_dataframe_assert_eq
from intake.source.base import DataSource

from intake_dynamodb import DynamoDBSource


@pytest.fixture(scope="function")
def example_small_table(dynamodb: botocore.client.BaseClient) -> str:
    table_name = "example-small-table"
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
    return table_name


@pytest.fixture(scope="function")
def example_small_table_expected_ddf() -> dd.DataFrame:
    return db.from_sequence(
        [
            {"id": "0", "name": "John Doe", "age": Decimal("30")},
            {"id": "1", "name": "Jill Doe", "age": Decimal("31")},
        ],
        npartitions=1,
    ).to_dataframe()


@pytest.fixture(scope="function")
def example_big_table(dynamodb: botocore.client.BaseClient) -> str:
    table_name = "example-big-table"
    dynamodb.create_table(
        TableName=table_name,
        KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    names = ["John Doe", "Jill Doe"]
    for i in range(0, 45_000):
        dynamodb.put_item(
            TableName=table_name,
            Item={
                "id": {"S": str(i)},
                "name": {"S": random.choice(names)},
                "age": {"N": str(random.randint(20, 80))},
            },
        )
    return table_name


@pytest.fixture(scope="function")
def example_small_table_different_account(
    dynamodb_in_different_account: botocore.client.BaseClient,
) -> str:
    table_name = "example-small-table-different-account"
    dynamodb_in_different_account.create_table(
        TableName=table_name,
        KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    for i in range(0, 2):
        dynamodb_in_different_account.put_item(
            TableName=table_name,
            Item={
                "id": {"S": str(i)},
                "name": {"S": ["John Doe", "Jill Doe"][i]},
                "age": {"N": str(i + 30 + 1)},
            },
        )
    return table_name


@pytest.fixture(scope="function")
def example_small_table_different_account_expected_ddf() -> dd.DataFrame:
    return db.from_sequence(
        [
            {"id": "0", "name": "John Doe", "age": Decimal("31")},
            {"id": "1", "name": "Jill Doe", "age": Decimal("32")},
        ],
        npartitions=1,
    ).to_dataframe()


@pytest.fixture
def yaml_catalog() -> DataSource:
    return intake.open_catalog("tests/test.yaml")


def test_dynamodb_source(example_small_table):
    source = DynamoDBSource(table_name=example_small_table)
    assert isinstance(source, DynamoDBSource)


def test_dynamodb_scan(example_small_table):
    source = DynamoDBSource(table_name=example_small_table)
    items = source._scan_table()
    assert items == [
        {"id": "0", "name": "John Doe", "age": Decimal("30")},
        {"id": "1", "name": "Jill Doe", "age": Decimal("31")},
    ]


def test_dynamodb_scan_filtered(example_small_table):
    source = DynamoDBSource(
        table_name=example_small_table,
        filter_expression="age = :age_value",
        filter_expression_value=30,
    )
    items = source._scan_table()
    assert items == [
        {"id": "0", "name": "John Doe", "age": Decimal("30")},
    ]


def test_dynamodb_to_dask(
    example_small_table,
    example_small_table_expected_ddf,
):
    source = DynamoDBSource(table_name=example_small_table)
    actual_ddf = source.to_dask()
    dask_dataframe_assert_eq(actual_ddf, example_small_table_expected_ddf)


def test_dynamodb_read(
    example_small_table,
    example_small_table_expected_ddf,
):
    source = DynamoDBSource(table_name=example_small_table)
    actual_df = source.read()
    pd_testing.assert_equal(
        actual_df,
        example_small_table_expected_ddf.compute(),
    )


def test_dynamodb_to_paritioned_dask(example_big_table):
    source = DynamoDBSource(table_name=example_big_table)
    ddf = source.to_dask()
    assert ddf.npartitions == 2


def test_dynamodb_in_different_account(
    example_small_table_different_account,
    example_small_table_different_account_expected_ddf,
):
    source = DynamoDBSource(
        table_name=example_small_table_different_account,
        sts_role_arn="arn:aws:iam::ACCOUNT_ID:role/ROLE_NAME",
        region_name="us-west-2",
    )
    actual_df = source.read()
    pd_testing.assert_equal(
        actual_df,
        example_small_table_different_account_expected_ddf.compute(),
    )


def test_yaml_small_table(
    yaml_catalog,
    example_small_table,
    example_small_table_expected_ddf,
):
    source = yaml_catalog.example_small_table
    actual_df = source.read()
    pd_testing.assert_equal(
        actual_df,
        example_small_table_expected_ddf.compute(),
    )


def test_yaml_small_table_filtered(
    yaml_catalog,
    example_small_table,
):
    source = yaml_catalog.example_small_table_filtered
    actual_df = source.read()
    expected_df = pd.DataFrame(
        [
            {"id": "0", "name": "John Doe", "age": Decimal("30")},
        ]
    )
    pd_testing.assert_equal(
        actual_df,
        expected_df,
    )


def test_yaml_big_table(
    yaml_catalog,
    example_big_table,
):
    source = yaml_catalog.example_big_table
    ddf = source.to_dask()
    assert ddf.npartitions == 2


def test_yaml_different_account(
    yaml_catalog,
    example_small_table_different_account,
    example_small_table_different_account_expected_ddf,
):
    source = yaml_catalog.example_small_table_different_account
    actual_ddf = source.to_dask()
    dask_dataframe_assert_eq(
        actual_ddf, example_small_table_different_account_expected_ddf
    )
