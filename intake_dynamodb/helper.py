# delete once https://github.com/dask-contrib/dask-awkward/pull/209
# is merged and released
from typing import Any
import awkward as ak
import dask
from dask.dataframe.core import DataFrame as DaskDataFrame
from dask.dataframe.core import new_dd_object
from dask_awkward.lib.core import (
    empty_typetracer,
    map_partitions,
)


def to_dask_dataframe(
    array,
    **kwargs: Any,
) -> DaskDataFrame:
    """Convert :class:`dask_awkward.Array` collection to :class:`~dask.dataframe.DataFrame`.

    Parameters
    ----------
    array : dask_awkward.Array
        Array collection to be converted.
    **kwargs : Any
        Additional arguments passed to :func:`ak.to_dataframe`.

    Returns
    -------
    dask.dataframe.DataFrame
        Resulting DataFrame collection.
    """
    (array,) = dask.optimize(array)
    intermediate = map_partitions(
        ak.to_dataframe,
        array,
        meta=empty_typetracer(),
        label="to-dataframe",
        **kwargs,
    )
    meta = ak.to_dataframe(array._meta.layout.form.length_zero_array(), **kwargs)
    return new_dd_object(
        intermediate.dask,
        intermediate.name,
        meta,
        intermediate.divisions,
    )
