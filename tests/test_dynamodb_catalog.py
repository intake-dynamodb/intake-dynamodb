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

from intake_dynamodb import DynamoDBSource, DynamoDBJSONSource


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
                "id": {"S": f"{i}"},
                "name": {"S": ["John Doe", "Jill Doe"][i]},
                "age": {"N": f"{i + 30}"},
            },
        )
    return table_name


@pytest.fixture(scope="function")
def example_small_items() -> list[dict[str, dict[str, str]]]:
    return [
        {"id": {"S": "0"}, "name": {"S": "John Doe"}, "age": {"N": "30"}},
        {"id": {"S": "1"}, "name": {"S": "Jill Doe"}, "age": {"N": "31"}},
    ]


@pytest.fixture(scope="function")
def example_small_table_expected_ddf(example_small_items) -> dd.DataFrame:
    return db.from_sequence(
        example_small_items,
        npartitions=1,
    ).to_dataframe()


@pytest.fixture(scope="function")
def example_small_table_expected_ddf_no_demical() -> dd.DataFrame:
    return db.from_sequence(
        [
            {"id": "0", "name": "John Doe", "age": "30"},
            {"id": "1", "name": "Jill Doe", "age": "31"},
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
    for i in range(0, 45_000):
        dynamodb.put_item(
            TableName=table_name,
            Item={
                "id": {"S": f"{i}"},
                "name": {"S": "immortal person"},
                "age": {"N": f"{i}"},
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
                "id": {"S": f"{i}"},
                "name": {"S": ["John Doe", "Jill Doe"][i]},
                "age": {"N": f"{i + 30 + 1}"},
            },
        )
    return table_name


@pytest.fixture(scope="function")
def example_small_items_different_account() -> list[dict[str, dict[str, str]]]:
    return [
        {"id": {"S": "0"}, "name": {"S": "John Doe"}, "age": {"N": "31"}},
        {"id": {"S": "1"}, "name": {"S": "Jill Doe"}, "age": {"N": "32"}},
    ]


@pytest.fixture(scope="function")
def example_small_table_different_account_expected_ddf(
    example_small_items_different_account,
) -> dd.DataFrame:
    return db.from_sequence(
        example_small_items_different_account,
        npartitions=1,
    ).to_dataframe()


@pytest.fixture(scope="function")
def example_bucket(s3):
    bucket_name = "example-bucket"
    s3.create_bucket(Bucket=bucket_name)
    for file in [
        "0123456789-abcdefgh/manifest-summary.json",
        "0123456789-abcdefgh/manifest-files.json",
        "0123456789-abcdefgh/data/abcdefghijklmnopqrstuvwxyz.json.gz",
        "0123456789-abcdefgh2/manifest-summary.json",
        "0123456789-abcdefgh2/manifest-files.json",
        "0123456789-abcdefgh2/data/abcdefghijklmnopqrstuvwxyz.json.gz",
        "0123456789-abcdefgh2/data/abcdefghijklmnopqrstuvwxyz2.json.gz",
    ]:
        with open(
            f"tests/AWSDynamoDB/{file}",
            "rb",
        ) as f:
            s3.upload_fileobj(
                f,
                bucket_name,
                f"AWSDynamoDB/{file}",
            )
    return bucket_name


@pytest.fixture
def yaml_catalog() -> DataSource:
    return intake.open_catalog("tests/test.yaml")


def test_dynamodb_source(example_small_table):
    source = DynamoDBSource(table_name=example_small_table)
    assert isinstance(source, DynamoDBSource)


def test_dynamodb_scan(example_small_table, example_small_items):
    source = DynamoDBSource(table_name=example_small_table)
    items = source._scan_table()
    assert items == example_small_items


# def test_dynamodb_scan_filtered(example_small_table):
#     source = DynamoDBSource(
#         table_name=example_small_table,
#         filter_expression="age = :age_value",
#         filter_expression_value=30,
#     )
#     items = source._scan_table()
#     assert items == [
#         {"id": "0", "name": "John Doe", "age": Decimal("30")},
#     ]


# def test_dynamodb_to_dask(
#     example_small_table,
#     example_small_table_expected_ddf,
# ):
#     source = DynamoDBSource(table_name=example_small_table)
#     actual_ddf = source.to_dask()
#     dask_dataframe_assert_eq(actual_ddf, example_small_table_expected_ddf)


# def test_dynamodb_read(
#     example_small_table,
#     example_small_table_expected_ddf,
# ):
#     source = DynamoDBSource(table_name=example_small_table)
#     actual_df = source.read()
#     pd_testing.assert_equal(
#         actual_df,
#         example_small_table_expected_ddf.compute(),
#     )


# @pytest.mark.slow
# def test_dynamodb_to_paritioned_dask(example_big_table):
#     source = DynamoDBSource(table_name=example_big_table)
#     ddf = source.to_dask()
#     assert ddf.npartitions == 2


# def test_dynamodb_in_different_account(
#     example_small_table_different_account,
#     example_small_table_different_account_expected_ddf,
# ):
#     source = DynamoDBSource(
#         table_name=example_small_table_different_account,
#         sts_role_arn="arn:aws:iam::ACCOUNT_ID:role/ROLE_NAME",
#         region_name="us-west-2",
#     )
#     actual_df = source.read()
#     pd_testing.assert_equal(
#         actual_df,
#         example_small_table_different_account_expected_ddf.compute(),
#     )


# def test_yaml_small_table(
#     yaml_catalog,
#     example_small_table,
#     example_small_table_expected_ddf,
# ):
#     source = yaml_catalog.example_small_table
#     actual_df = source.read()
#     pd_testing.assert_equal(
#         actual_df,
#         example_small_table_expected_ddf.compute(),
#     )


# def test_yaml_small_table_filtered(
#     yaml_catalog,
#     example_small_table,
# ):
#     source = yaml_catalog.example_small_table_filtered
#     actual_df = source.read()
#     expected_df = pd.DataFrame(
#         [
#             {"id": "0", "name": "John Doe", "age": Decimal("30")},
#         ]
#     )
#     pd_testing.assert_equal(
#         actual_df,
#         expected_df,
#     )


# @pytest.mark.slow
# def test_yaml_big_table(
#     yaml_catalog,
#     example_big_table,
# ):
#     source = yaml_catalog.example_big_table
#     ddf = source.to_dask()
#     assert ddf.npartitions == 2


# def test_yaml_different_account(
#     yaml_catalog,
#     example_small_table_different_account,
#     example_small_table_different_account_expected_ddf,
# ):
#     source = yaml_catalog.example_small_table_different_account
#     actual_ddf = source.to_dask()
#     dask_dataframe_assert_eq(
#         actual_ddf, example_small_table_different_account_expected_ddf
#     )


# def test_dynamodbjson_source(example_bucket):
#     source = DynamoDBJSONSource(
#         s3_path=f"s3://{example_bucket}/AWSDynamoDB/0123456789-abcdefgh",
#     )
#     assert isinstance(source, DynamoDBJSONSource)


# def test_dynamodbjson_small_s3_export(
#     example_bucket,
#     s3,
#     example_small_table_expected_ddf_no_demical,
# ):
#     source = DynamoDBJSONSource(
#         s3_path=f"s3://{example_bucket}/AWSDynamoDB/0123456789-abcdefgh",
#     )
#     actual_df = source.read()
#     pd_testing.assert_equal(
#         actual_df,
#         example_small_table_expected_ddf_no_demical.compute(),
#     )


# def test_dynamodbjson_small_s3_export_yaml(
#     yaml_catalog,
#     example_bucket,
#     s3,
#     example_small_table,
# ):
#     source_s3_export = yaml_catalog.example_small_s3_export
#     s3_export_df = source_s3_export.read()
#     source_dynamodb = yaml_catalog.example_small_table
#     dynamodb_df = source_dynamodb.read()
#     # s3 export returns object (str) and dynamodb return Decimal type
#     s3_export_df["age"] = s3_export_df["age"].astype(int)
#     dynamodb_df["age"] = dynamodb_df["age"].astype(int)
#     pd_testing.assert_equal(s3_export_df, dynamodb_df)


# def test_dynamodbjson_partitioned_s3_export(
#     example_bucket,
#     s3,
#     example_small_table_expected_ddf_no_demical,
# ):
#     source = DynamoDBJSONSource(
#         s3_path=f"s3://{example_bucket}/AWSDynamoDB/0123456789-abcdefgh2",
#     )
#     actual_df = source.read()
#     pd_testing.assert_equal(
#         actual_df,
#         example_small_table_expected_ddf_no_demical.compute(),
#     )


# def test_dynamodbjson_partitioned_s3_export_yaml(
#     yaml_catalog,
#     example_bucket,
#     s3,
#     example_small_table,
# ):
#     source_paritioned_s3_export = yaml_catalog.example_partitioned_s3_export
#     s3_export_df = source_paritioned_s3_export.read()
#     source_dynamodb = yaml_catalog.example_small_table
#     dynamodb_df = source_dynamodb.read()
#     # s3 export returns object (str) and dynamodb return Decimal type
#     s3_export_df["age"] = s3_export_df["age"].astype(int)
#     dynamodb_df["age"] = dynamodb_df["age"].astype(int)
#     pd_testing.assert_equal(s3_export_df, dynamodb_df)
