
# -*- coding: utf-8 -*-
import numpy as np
import os
import pytest

from intake import open_catalog
from .util import dataset  # noqa

from intake_dynamodb import DynamoDBSource


@pytest.fixture
def catalog1():
    path = os.path.dirname(__file__)
    return open_catalog(os.path.join(path, 'data', 'catalog.yaml'))


def test_catalog(catalog1, dataset):
    source = catalog1['dynamodb_source'].get()
    assert isinstance(source, DynamoDBSource)
