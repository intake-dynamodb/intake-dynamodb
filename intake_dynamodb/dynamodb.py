# -*- coding: utf-8 -*-
from . import __version__
from intake.source.base import DataSource, Schema

import json
import dask.dataframe as dd
from datetime import datetime, timedelta
from time import sleep
import boto3
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError


RETRY_EXCEPTIONS = ('ProvisionedThroughputExceededException',
                    'ThrottlingException')
MAX_RETRIES = 30


class DynamoDBSource(DataSource):
    """Common behaviours for plugins in this repo"""
    name = 'dynamodb'
    version = __version__
    container = 'dataframe'
    partition_access = True

    def __init__(self, table_name, metadata=None):
        """
        Parameters
        ----------
        table_name : str
            The DynamoDB table to load.
        """
        self._table_name = table_name
        self._dataframe = None
        self._dynamodb = boto3.resource('dynamodb')

    def _scan_table(self, table_name, filter_key=None, filter_value=None):
        """
        Perform a scan operation on table.
        Can specify filter_key (col name) and its value to be filtered.
        """
        retries=0
        table = self._dynamodb.Table(table_name)
        if filter_key is None or filter_value is None:
            response = table.scan()
        else:
            response = table.scan(FilterExpression=Key(filter_key).eq(filter_value))
        data = response['Items']

        while 'LastEvaluatedKey' in response:
            try:
                if filter_key is None or filter_value is None:
                    response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
                else:
                    response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'], FilterExpression=Key(filter_key).eq(filter_value))
                data.extend(response['Items'])
                retries = 0          # if successful, reset count
            except ClientError as err:
                if err.response['Error']['Code'] not in RETRY_EXCEPTIONS:
                    raise
                sleep(2 ** retries)
                if retries < MAX_RETRIES:
                    retries += 1     # TODO max limit

        return data

    def _open_dataset(self):
        self._dataframe = dd.DataFrame.from_dict(self._scan_table(self.table_name)).set_index('key')

    def _get_schema(self):
        if self._dataframe is None:
            self._open_dataset()

        dtypes = self._dataframe._meta.dtypes.to_dict()
        dtypes = {n: str(t) for (n, t) in dtypes.items()}
        return Schema(datashape=None,
                      dtype=dtypes,
                      shape=(None, len(dtypes)),
                      npartitions=self._dataframe.npartitions,
                      extra_metadata={})

    def _get_partition(self, i):
        self._get_schema()
        return self._dataframe.get_partition(i).compute()

    def read(self):
        self._get_schema()
        return self._dataframe.compute()

    def to_dask(self):
        self._get_schema()
        return self._dataframe

    def _close(self):
        self._dataframe = None
