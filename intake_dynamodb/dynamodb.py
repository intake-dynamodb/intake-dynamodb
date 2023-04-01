from __future__ import annotations

from time import sleep
from typing import Any, Optional

import boto3
import dask.bag as db
import dask.dataframe as dd
import pandas as pd
import s3fs
from botocore.exceptions import ClientError
from intake.source.base import DataSource, Schema
from intake.source.jsonfiles import JSONFileSource, JSONLinesFileSource

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
        filter_expression: str (optional)
            Filter expression to pass to table.scan() e.g. 'age = :age_threshold'
        filter_expression_value: Any (optional)
            Value used in filter_expression such as 30
        """
        self.table_name = table_name
        self.sts_role_arn = sts_role_arn
        self.region_name = region_name
        self.filter_expression = filter_expression
        self.filter_expression_value = filter_expression_value

        self.metadata = kwargs.pop("metadata", {})

    def _connect(
        self,
    ):
        if self.sts_role_arn is None and self.region_name is None:
            self.dynamodb = boto3.resource("dynamodb")
        else:
            self.sts = boto3.client("sts")
            _sts_session = self.sts.assume_role(
                RoleArn=self.sts_role_arn,
                RoleSessionName="session",
            )
            creds = _sts_session["Credentials"]
            self.dynamodb = boto3.resource(
                "dynamodb",
                aws_access_key_id=creds["AccessKeyId"],
                aws_secret_access_key=creds["SecretAccessKey"],
                aws_session_token=creds["SessionToken"],
                region_name=self.region_name,
            )

    def _scan_table(
        self,
    ) -> list[dict[str, Any]]:
        """Perform a scan operation on table and optionally filter."""
        self._connect()
        retries = 0
        table = self.dynamodb.Table(self.table_name)  # type: ignore[union-attr]
        _no_filter = (
            self.filter_expression is None and self.filter_expression_value is None
        )
        if _no_filter:
            response = table.scan()
        else:
            _key = self.filter_expression.split(" ")[-1]  # type: ignore[union-attr]
            _expression_attribute_values = {_key: self.filter_expression_value}
            response = table.scan(
                FilterExpression=self.filter_expression,
                ExpressionAttributeValues=_expression_attribute_values,
            )
        items = response["Items"]
        self.npartitions = 1

        while "LastEvaluatedKey" in response:
            try:
                if _no_filter:
                    response = table.scan(
                        ExclusiveStartKey=response["LastEvaluatedKey"]
                    )
                else:
                    response = table.scan(
                        ExclusiveStartKey=response["LastEvaluatedKey"],
                        FilterExpression=self.filter_expression,
                        ExpressionAttributeValues=_expression_attribute_values,
                    )
                items.extend(response["Items"])
                self.npartitions += 1
                retries = 0  # if successful, reset count
            except ClientError as err:
                if err.response["Error"]["Code"] not in RETRY_EXCEPTIONS:
                    raise
                sleep(2**retries)
                if retries < MAX_RETRIES:
                    retries += 1  # TODO max limit
        return items

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

    def _get_partition(self, i: int) -> pd.DataFrame:
        self._get_schema()
        return self.dataframe.get_partition(i).compute()

    def to_dask(self) -> dd.DataFrame:
        table_items = self._scan_table()
        self.dataframe = db.from_sequence(
            table_items,
            npartitions=self.npartitions,
        ).to_dataframe()
        return self.dataframe

    def read(self) -> pd.DataFrame:
        self._get_schema()
        return self.dataframe.compute()

    def _close(self) -> None:
        self.dataframe = None


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
        **kwargs,
    ):
        """
        Parameters
        ----------
        s3_path: str
            top directory of the dynamodb s3 export dump. usually as hash
            such as "s3://BUCKET/AWSDynamoDB/0123456789-abcdefg".
            The suffix is the hash of the dump.
        storage_options: dict (optional)
            options for the s3 path e.g. {"profile": "dev"}
        """
        self.s3_path = s3_path
        self.storage_options = storage_options

        self.metadata = kwargs.pop("metadata", {})

    def _s3_path_properties(self):
        manifest_summary_json = JSONFileSource(
            f"{self.s3_path}/manifest-summary.json",
            storage_options=self.storage_options,
        )
        self.export_time = manifest_summary_json.read()["exportTime"]
        manifest_files_json = JSONLinesFileSource(
            f"{self.s3_path}/manifest-files.json",
            storage_options=self.storage_options,
        )

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
        # map JSONLinesFileSource to the individual files
