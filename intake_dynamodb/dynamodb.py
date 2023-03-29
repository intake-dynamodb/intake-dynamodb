from typing import Any, Optional

from intake.source.base import DataSource, Schema

import pandas as pd
import dask.dataframe as dd
from time import sleep
import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError


RETRY_EXCEPTIONS = ("ProvisionedThroughputExceededException", "ThrottlingException")
MAX_RETRIES = 30


class DynamoDBSource(DataSource):
    """Common behaviours for plugins in this repo"""

    container = "dataframe"
    name = "dynamodb"
    partition_access = True

    def __init__(
        self,
        table_name: str,
        dynamodb: Optional[boto3.resources.base.ServiceResource] = None,
        sts_role_arn: Optional[str] = None,
        region_name: Optional[str] = None,
        **kwargs,
    ):
        """
        Parameters
        ----------
        table_name: str
            The DynamoDB table to load.
        dynamodb: boto3.resources.factory.dynamodb.ServiceResource (optional)
            The dynamodb resource if already defined.
        sts_role_arn: str (optional)
            STS RoleArn if reading a DynamoDB table in another AWS account.
        region_name: str (optional)
            The region of the DynamoDB table if reading a DynamoDB table in another
            AWS account.
        """
        self.table_name = table_name
        self.dynamodb = dynamodb
        self.sts_role_arn = sts_role_arn
        self.region_name = region_name

        self.dataframe = None
        self.npartitions = None
        self.metadata = kwargs.pop("metadata", {})

    def _connect(
        self,
    ):
        if self.dynamodb is not None:
            pass
        else:
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
        filter_key: Optional[str] = None,
        filter_value: Optional[Any] = None,
    ) -> list[dict[Any, Any]]:
        """
        Perform a scan operation on table.
        Can specify filter_key (col name) and its value to be filtered.
        """
        self._connect()
        retries = 0
        table = self.dynamodb.Table(self.table_name)
        if filter_key is None and filter_value is None:
            response = table.scan()
        else:
            response = table.scan(FilterExpression=Key(filter_key).eq(filter_value))
        data = response["Items"]
        self.npartitions = 1

        while "LastEvaluatedKey" in response:
            try:
                if filter_key is None or filter_value is None:
                    response = table.scan(
                        ExclusiveStartKey=response["LastEvaluatedKey"]
                    )
                else:
                    response = table.scan(
                        ExclusiveStartKey=response["LastEvaluatedKey"],
                        FilterExpression=Key(filter_key).eq(filter_value),
                    )
                data.extend(response["Items"])
                self.npartitions += 1
                retries = 0  # if successful, reset count
            except ClientError as err:
                if err.response["Error"]["Code"] not in RETRY_EXCEPTIONS:
                    raise
                sleep(2**retries)
                if retries < MAX_RETRIES:
                    retries += 1  # TODO max limit

        return data

    def _open_dataset(self):
        table_contents = self._scan_table(self.table_name)
        table_dataframe = pd.DataFrame.from_dict(table_contents)
        table_dataframe = table_dataframe.set_index("key")
        self.dataframe = dd.from_pandas(table_dataframe, npartitions=self.npartitions)

    def _get_schema(self):
        if self.dataframe is None:
            self._open_dataset()

        dtypes = self.dataframe._meta.dtypes.to_dict()
        dtypes = {n: str(t) for (n, t) in dtypes.items()}
        return Schema(
            datashape=None,
            dtype=dtypes,
            shape=(None, len(dtypes)),
            npartitions=self.dataframe.npartitions,
            extra_metadata={},
        )

    def _get_partition(self, i: int):
        self._get_schema()
        return self.dataframe.get_partition(i).compute()

    def read(self):
        self._get_schema()
        return self.dataframe.compute()

    def to_dask(self):
        self._get_schema()
        return self.dataframe

    def _close(self):
        self.dataframe = None
