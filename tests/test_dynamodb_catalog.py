import botocore
import dask
import dask.dataframe as dd
import intake
import pandas as pd
import pandas._testing as pd_testing
import pytest
from dask.dataframe.utils import assert_eq as dask_dataframe_assert_eq
from intake.source.base import DataSource

from intake_dynamodb import DynamoDBJSONSource, DynamoDBSource


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
                "nm": {"S": ["John Doe", "Jill Doe"][i]},
                "ag": {"N": f"{i + 30}"},
            },
        )
    return table_name


@pytest.fixture(scope="function")
def example_small_table_items() -> list[dict[str, dict[str, str]]]:
    _l = [
        {"id": {"S": "0"}, "nm": {"S": "John Doe"}, "ag": {"N": "30"}},
        {"id": {"S": "1"}, "nm": {"S": "Jill Doe"}, "ag": {"N": "31"}},
    ]
    return _l


@pytest.fixture(scope="function")
def example_small_table_items_filtered() -> list[dict[str, dict[str, str]]]:
    _l = [
        {"id": {"S": "0"}, "nm": {"S": "John Doe"}, "ag": {"N": "30"}},
    ]
    return _l


@pytest.fixture(scope="function")
def example_small_table_df(example_small_table_items) -> pd.DataFrame:
    return pd.json_normalize(example_small_table_items)


@pytest.fixture(scope="function")
def example_small_table_items_filtered_df(
    example_small_table_items_filtered,
) -> pd.DataFrame:
    return pd.json_normalize(example_small_table_items_filtered)


@pytest.fixture(scope="function")
def example_small_table_expected_ddf(
    example_small_table_df,
) -> dd.DataFrame:
    return dd.from_pandas(example_small_table_df, 1)


@pytest.fixture(scope="function")
def example_small_table_expected_ddf_filtered(
    example_small_table_items_filtered_df,
) -> dd.DataFrame:
    return dd.from_pandas(example_small_table_items_filtered_df, 1)


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
                "nm": {"S": "immortal person"},
                "ag": {"N": f"{i}"},
            },
        )
    return table_name


@pytest.fixture(scope="function")
def example_big_table_items() -> list[dict[str, dict[str, str]]]:
    _l = []
    for i in range(0, 45_000):
        _l.append(
            {
                "id": {"S": f"{i}"},
                "nm": {"S": "immortal person"},
                "ag": {"N": f"{i}"},
            }
        )
    return _l


@pytest.fixture(scope="function")
def example_big_table_df(example_big_table_items) -> pd.DataFrame:
    return pd.json_normalize(example_big_table_items)


@pytest.fixture(scope="function")
def example_big_table_expected_ddf(
    example_big_table_df,
) -> dd.DataFrame:
    return dd.from_pandas(example_big_table_df, 1)


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
                "nm": {"S": ["John Doe", "Jill Doe"][i]},
                "ag": {"N": f"{i + 30 + 1}"},
            },
        )
    return table_name


@pytest.fixture(scope="function")
def example_small_items_different_account() -> list[dict[str, dict[str, str]]]:
    _l = [
        {"id": {"S": "0"}, "nm": {"S": "John Doe"}, "ag": {"N": "31"}},
        {"id": {"S": "1"}, "nm": {"S": "Jill Doe"}, "ag": {"N": "32"}},
    ]
    return _l


@pytest.fixture(scope="function")
def example_small_table_different_account_expected_df(
    example_small_items_different_account,
) -> pd.DataFrame:
    return pd.json_normalize(example_small_items_different_account)


@pytest.fixture(scope="function")
def example_small_table_different_account_expected_ddf(
    example_small_table_different_account_expected_df,
) -> dd.DataFrame:
    return dd.from_pandas(example_small_table_different_account_expected_df, 1)


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


#######################################
########### END OF FIXTURES ###########
#######################################


def test_dynamodb_source(example_small_table):
    source = DynamoDBSource(table_name=example_small_table)
    assert isinstance(source, DynamoDBSource)


def test_dynamodb_scan(example_small_table, example_small_table_items):
    source = DynamoDBSource(
        table_name=example_small_table,
        region_name="us-east-1",
    )
    items = source._scan_table()
    assert items == example_small_table_items


def test_dynamodb_scan_limit(
    example_small_table,
    example_small_table_items,
):
    source = DynamoDBSource(
        table_name=example_small_table,
        region_name="us-east-1",
        limit=1,
    )
    items = source._scan_table()
    assert items == [example_small_table_items[0]]


def test_dynamodb_scan_filtered_num(
    example_small_table, example_small_table_items_filtered
):
    source = DynamoDBSource(
        table_name=example_small_table,
        region_name="us-east-1",
        filter_expression="ag = :ag_value",
        filter_expression_value=30,
    )
    items = source._scan_table()
    assert items == example_small_table_items_filtered


def test_dynamodb_scan_filtered_str(
    example_small_table, example_small_table_items_filtered
):
    source = DynamoDBSource(
        table_name=example_small_table,
        region_name="us-east-1",
        filter_expression="nm = :nm_value",
        filter_expression_value="John Doe",
    )
    items = source._scan_table()
    assert items == example_small_table_items_filtered


def test_dynamodb_to_dask(example_small_table):
    source = DynamoDBSource(
        table_name=example_small_table,
        region_name="us-east-1",
    )
    actual_dask = source.to_dask()
    expected_dask = dask.delayed(source.read())
    pd_testing.assert_equal(
        actual_dask.compute(),
        expected_dask.compute(),
    )


def test_dynamodb_to_dask_df(
    example_small_table,
    example_small_table_expected_ddf,
):
    source = DynamoDBSource(
        table_name=example_small_table,
        region_name="us-east-1",
    )
    actual_dask_df = source.to_dask_df()
    dask_dataframe_assert_eq(actual_dask_df, example_small_table_expected_ddf)


def test_dynamodb_read(
    example_small_table,
    example_small_table_expected_ddf,
):
    source = DynamoDBSource(
        table_name=example_small_table,
        region_name="us-east-1",
    )
    actual_df = source.read()
    pd_testing.assert_equal(
        actual_df,
        example_small_table_expected_ddf.compute(),
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


def test_yaml_small_table_limit(
    yaml_catalog,
    example_small_table,
    example_small_table_expected_ddf,
):
    source = yaml_catalog.example_small_table_limit
    actual_df = source.read()
    expected_df = example_small_table_expected_ddf.compute().loc[[0]]
    pd_testing.assert_equal(
        actual_df,
        expected_df,
    )


def test_yaml_small_table_filtered_num(
    yaml_catalog,
    example_small_table,
    example_small_table_expected_ddf_filtered,
):
    source = yaml_catalog.example_small_table_filtered_num
    actual_df = source.read()
    expected_df = example_small_table_expected_ddf_filtered.compute()
    pd_testing.assert_equal(
        actual_df,
        expected_df,
    )


def test_yaml_small_table_filtered_str(
    yaml_catalog,
    example_small_table,
    example_small_table_expected_ddf_filtered,
):
    source = yaml_catalog.example_small_table_filtered_str
    actual_df = source.read()
    expected_df = example_small_table_expected_ddf_filtered.compute()
    pd_testing.assert_equal(
        actual_df,
        expected_df,
    )


@pytest.mark.slow
def test_dynamodb_multi_scan(example_big_table):
    source = DynamoDBSource(
        table_name=example_big_table,
        region_name="us-east-1",
    )
    source._get_n_table_scans() == 2


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


@pytest.mark.slow
def test_yaml_big_table(
    yaml_catalog,
    example_big_table,
    example_big_table_expected_ddf,
):
    source = yaml_catalog.example_big_table
    actual_ddf = source.to_dask_df()
    dask_dataframe_assert_eq(
        actual_ddf,
        example_big_table_expected_ddf,
    )


def test_yaml_different_account(
    yaml_catalog,
    example_small_table_different_account,
    example_small_table_different_account_expected_ddf,
):
    source = yaml_catalog.example_small_table_different_account
    actual_ddf = source.to_dask_df()
    dask_dataframe_assert_eq(
        actual_ddf, example_small_table_different_account_expected_ddf
    )


def test_dynamodbjson_source(example_bucket):
    source = DynamoDBJSONSource(
        s3_path=f"s3://{example_bucket}/AWSDynamoDB/0123456789-abcdefgh",
    )
    assert isinstance(source, DynamoDBJSONSource)


@pytest.mark.s3test
def test_dynamodbjson_small_s3_export(
    example_bucket,
    s3,
    example_small_table_expected_ddf,
):
    source = DynamoDBJSONSource(
        s3_path=f"s3://{example_bucket}/AWSDynamoDB/0123456789-abcdefgh",
    )
    actual_df = source.read()
    pd_testing.assert_equal(
        actual_df,
        example_small_table_expected_ddf.compute(),
    )


@pytest.mark.s3test
def test_dynamodbjson_small_s3_export_yaml(
    yaml_catalog,
    example_bucket,
    s3,
    example_small_table,
):
    source_s3_export = yaml_catalog.example_small_s3_export
    s3_export_df = source_s3_export.read()
    source_dynamodb = yaml_catalog.example_small_table
    dynamodb_df = source_dynamodb.read()
    pd_testing.assert_equal(s3_export_df, dynamodb_df)


@pytest.mark.s3test
def test_dynamodbjson_small_s3_export_yaml_with_kwargs(
    yaml_catalog,
    example_bucket,
    s3,
    example_small_table,
):
    source_s3_export = yaml_catalog.example_small_s3_export_with_kwargs
    s3_export_df = source_s3_export.read()
    source_dynamodb = yaml_catalog.example_small_table
    dynamodb_df = source_dynamodb.read()
    pd_testing.assert_equal(s3_export_df, dynamodb_df)


@pytest.mark.s3test
def test_dynamodbjson_partitioned_s3_export(
    example_bucket,
    s3,
    example_small_table_expected_ddf,
):
    source = DynamoDBJSONSource(
        s3_path=f"s3://{example_bucket}/AWSDynamoDB/0123456789-abcdefgh2",
    )
    actual_df = source.read()
    pd_testing.assert_equal(
        actual_df,
        example_small_table_expected_ddf.compute(),
    )


@pytest.mark.s3test
def test_dynamodbjson_partitioned_s3_export_yaml(
    yaml_catalog,
    example_bucket,
    s3,
    example_small_table,
):
    source_paritioned_s3_export = yaml_catalog.example_partitioned_s3_export
    s3_export_df = source_paritioned_s3_export.read()
    source_dynamodb = yaml_catalog.example_small_table
    dynamodb_df = source_dynamodb.read()
    pd_testing.assert_equal(s3_export_df, dynamodb_df)
