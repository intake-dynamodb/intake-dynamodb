from __future__ import annotations

import json
import numbers
from time import sleep
from typing import Any, Optional

import botocore.session
import dask
import dask.dataframe as dd
import pandas as pd
import s3fs
from botocore.exceptions import ClientError
from intake.source.base import DataSource, Schema

RETRY_EXCEPTIONS = (
    "ProvisionedThroughputExceededException",
    "ThrottlingException",
)
MAX_RETRIES = 30


class DynamoDBSource(DataSource):
    """Extracting data from AWS DynamoDB"""

    container = "dataframe"
    name = "dynamodb"
    partition_access = True

    def __init__(
        self,
        table_name: str,
        sts_role_arn: Optional[str] = None,
        region_name: Optional[str] = None,
        limit: Optional[int] = None,
        filter_expression: Optional[str] = None,
        filter_expression_value: Optional[Any] = None,
        **kwargs,
    ):
        """
        Parameters
        ----------
        table_name: str
            The DynamoDB table to load.
        sts_role_arn: str (optional)
            STS RoleArn if reading a DynamoDB table in another AWS account.
        region_name: str (optional)
            The region of the DynamoDB table if reading a DynamoDB table in another
            AWS account.
        limit: int (optional)
            Limit the return items.
        filter_expression: str (optional)
            Filter expression to pass to table.scan() e.g. 'age = :age_threshold'
        filter_expression_value: Any (optional)
            Number or string e.g. 30
        """
        self.table_name = table_name
        self.sts_role_arn = sts_role_arn
        self.region_name = region_name
        self.limit = limit
        self.filter_expression = filter_expression
        self.filter_expression_value = filter_expression_value

        self.metadata = kwargs.pop("metadata", {})

    def _connect(
        self,
    ):
        if self.sts_role_arn is None and self.region_name is None:
            self.dynamodb = botocore.session.Session().create_client("dynamodb")
        if self.sts_role_arn is None and self.region_name is not None:
            self.dynamodb = botocore.session.Session().create_client(
                "dynamodb",
                region_name=self.region_name,
            )
        if self.sts_role_arn is not None and self.region_name is not None:
            self.sts = botocore.session.Session().create_client("sts")
            _sts_session = self.sts.assume_role(
                RoleArn=self.sts_role_arn,
                RoleSessionName="session",
            )
            creds = _sts_session["Credentials"]
            self.dynamodb = botocore.session.Session().create_client(
                "dynamodb",
                aws_access_key_id=creds["AccessKeyId"],
                aws_secret_access_key=creds["SecretAccessKey"],
                aws_session_token=creds["SessionToken"],
                region_name=self.region_name,
            )

    def _scan_table(
        self,
    ) -> list[dict[str, str]]:
        """Perform a scan operation on table and optionally filter."""
        self._connect()
        retries = 0
        _no_filter = (
            self.filter_expression is None and self.filter_expression_value is None
        )
        _no_limit = self.limit is None
        if _no_filter:
            if _no_limit:
                response = self.dynamodb.scan(TableName=self.table_name)
            else:
                response = self.dynamodb.scan(
                    TableName=self.table_name,
                    Limit=self.limit,
                )
                if response["Count"] == self.limit:
                    response.pop("LastEvaluatedKey")
        else:
            _key = self.filter_expression.split(" ")[-1]  # type: ignore[union-attr]
            if isinstance(self.filter_expression_value, numbers.Number):
                _val_dtype = "N"
                self.filter_expression_value = str(self.filter_expression_value)
            else:
                _val_dtype = "S"
            _value = {_val_dtype: self.filter_expression_value}
            _expression_attribute_values = {_key: _value}
            if _no_limit:
                response = self.dynamodb.scan(
                    TableName=self.table_name,
                    FilterExpression=self.filter_expression,
                    ExpressionAttributeValues=_expression_attribute_values,
                )
            else:
                response = self.dynamodb.scan(
                    TableName=self.table_name,
                    FilterExpression=self.filter_expression,
                    ExpressionAttributeValues=_expression_attribute_values,
                    Limit=self.limit,
                )
                if response["Count"] == self.limit:
                    response.pop("LastEvaluatedKey")
        items = response["Items"]
        self.table_scan_calls = 1

        while "LastEvaluatedKey" in response:
            try:
                if _no_filter:
                    response = self.dynamodb.scan(
                        TableName=self.table_name,
                        ExclusiveStartKey=response["LastEvaluatedKey"],
                    )
                else:
                    response = self.dynamodb.scan(
                        TableName=self.table_name,
                        ExclusiveStartKey=response["LastEvaluatedKey"],
                        FilterExpression=self.filter_expression,
                        ExpressionAttributeValues=_expression_attribute_values,
                    )
                items.extend(response["Items"])
                self.table_scan_calls += 1
                retries = 0  # if successful, reset count
            except ClientError as err:
                if err.response["Error"]["Code"] not in RETRY_EXCEPTIONS:
                    raise
                sleep(2**retries)
                if retries < MAX_RETRIES:
                    retries += 1  # TODO max limit
        self.n_items = len(items)
        return items

    def _get_schema(self) -> Schema:
        if not hasattr(self, "dataframe"):
            self.to_dask_df()

        dtypes = self.dataframe._meta.dtypes.to_dict()
        dtypes = {n: str(t) for (n, t) in dtypes.items()}
        return Schema(
            datashape=None,
            dtype=dtypes,
            shape=(None, len(dtypes)),
            npartitions=self.dataframe.npartitions,
            extra_metadata={},
        )

    def _get_n_table_scans(self) -> int:
        if not hasattr(self, "table_scan_calls"):
            self._scan_table()
        return self.table_scan_calls

    def to_dask(self):
        # No easy way to do this as have to scan the table first
        # If parallel is desired see DynamoDBJSONSource
        # Simple approach is to return a delayed object
        # See to_dask_df to return a dataframe
        return self.to_dask_delayed()

    def to_dask_delayed(self):
        self.delayed = dask.delayed(self.read())
        return self.delayed

    def to_dask_df(
        self,
        partitions: int = 1,
    ) -> dd.DataFrame:
        self.read()
        self.dataframe: dd.DataFrame = dd.from_pandas(self.pd_dataframe, partitions)
        return self.dataframe

    def read(self) -> pd.DataFrame:
        self.table_items = self._scan_table()
        self.pd_dataframe = pd.json_normalize(self.table_items)
        return self.pd_dataframe


class DynamoDBJSONSource(DataSource):
    """Extracting data from AWS DynamoDBJSON e.g.
    after an s3 export"""

    container = "dataframe"
    name = "dynamodbjson"
    partition_access = True

    def __init__(
        self,
        s3_path: str,
        storage_options: Optional[dict] = {},
        config_kwargs: Optional[dict] = {},
        **kwargs,
    ):
        """
        Parameters
        ----------
        s3_path: str
            s3 path of the dynamodb s3 export dump. usually contains hash
            e.g. "s3://example-bucket/AWSDynamoDB/0123456789-abcdefgh".
        storage_options: dict (optional)
            options for the s3 path e.g. {"profile": "dev"}
        config_kwargs: dict (optional)
            See config_kwargs in s3fs
        """
        self.s3_path = s3_path
        self.storage_options = storage_options
        self.config_kwargs = config_kwargs

        self.metadata = kwargs.pop("metadata", {})

    def _s3_path_properties(self):
        self.s3_bucket = self.s3_path.split("//")[-1].split("/")[0]
        fs = s3fs.S3FileSystem(
            config_kwargs=self.config_kwargs,
        )
        with fs.open(
            f"{self.s3_path}/manifest-summary.json",
            mode="r",
            storage_options=self.storage_options,
        ) as f:
            manifest_summary_json = json.load(f)
        self.export_time = manifest_summary_json["exportTime"]
        with fs.open(
            f"{self.s3_path}/manifest-files.json",
            mode="r",
            storage_options=self.storage_options,
        ) as f:
            manifest_files_jsonl = list(map(json.loads, f))
        self.data_files = []
        for file in manifest_files_jsonl:
            self.data_files.append(f"s3://{self.s3_bucket}/{file['dataFileS3Key']}")
        self.npartitions = len(self.data_files)

    @dask.delayed
    def _parse_dynamodbjson(self, data_file: str) -> pd.DataFrame:
        fs = s3fs.S3FileSystem(
            config_kwargs=self.config_kwargs,
        )
        with fs.open(
            data_file,
            mode="r",
            compression="gzip",
            storage_options=self.storage_options,
        ) as f:
            data = list(map(json.loads, f))
        data = list(map(lambda x: x["Item"], data))
        df = pd.json_normalize(data)
        return df

    def _get_schema(self) -> Schema:
        if not hasattr(self, "dataframe"):
            self.to_dask()

        dtypes = self.dataframe._meta.dtypes.to_dict()
        dtypes = {n: str(t) for (n, t) in dtypes.items()}
        return Schema(
            datashape=None,
            dtype=dtypes,
            shape=(None, len(dtypes)),
            npartitions=self.dataframe.npartitions,
            extra_metadata={},
        )

    def to_dask(self) -> dd.DataFrame:
        self._s3_path_properties()
        self.dataframe = dd.from_delayed(
            [self._parse_dynamodbjson(data_file) for data_file in self.data_files]
        )
        return self.dataframe

    def read(self) -> pd.DataFrame:
        self._get_schema()
        return self.dataframe.compute().reset_index(drop=True)
