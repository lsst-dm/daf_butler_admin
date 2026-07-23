# This file is part of daf_butler_admin.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

from __future__ import annotations

__all__ = ["update_datastore_checksums"]

from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from typing import cast

from lsst.daf.butler import Butler, DatasetRef
from lsst.daf.butler.datastore import DatasetRefURIs
from lsst.daf.butler.datastore.stored_file_info import StoredFileInfo
from lsst.daf.butler.datastores.fileDatastore import FileDatastore
from lsst.daf.butler.registry.interfaces import DatabaseInsertMode
from lsst.resources import ResourcePath
from lsst.utils.logging import getLogger

_LOG = getLogger(__name__)


def _compute_and_populate_checksum(
    datastore: FileDatastore, uris: dict[DatasetRef, DatasetRefURIs], dataset_ref: DatasetRef
) -> tuple[DatasetRef, StoredFileInfo] | None:
    """Compute checksum and update the record's stored info if checksum
    doesn't exist.

    Parameters
    ----------
    datastore : `FileDatastore`
        Datastore.
    uris : `dict[DatasetRef, DatasetRefURIs]`
        Dictionary containing the dataset reference to its URI.
    dataset_ref : `DatasetRef`
        Dataset reference.

    Returns
    -------
    `Optional[tuple[DatasetRef, StoredFileInfo]]`
        Dataset reference with its updated stored info.
    """
    stored_info = datastore.getStoredItemsInfo(dataset_ref)[0]

    if stored_info.checksum:
        return None
    else:
        primary_uri = uris[dataset_ref].primaryURI

        if primary_uri is None:
            raise ValueError(f"No URI found for dataset: {dataset_ref}")

        resource_path = ResourcePath(primary_uri.path)

        checksum = FileDatastore.computeChecksum(resource_path, algorithm="md5")

        return dataset_ref, stored_info.update(checksum=checksum)


def update_datastore_checksums(
    repo: str,
    dataset_type: Iterable[str],
    collections: Iterable[str],
    where: str,
    find_first: bool,
    limit: int,
) -> None:
    """Compute the checksum for the specified datasets in parallel
    and update the file_datastore_records table.

    Parameters
    ----------
    repo : `str`
        URI of butler repository to update.
    dataset_type : `Iterable[str]`
        Names of dataset types.
    collections : `Iterable[str]`
        Names of collections.
    where : `str`
        Query string.
    find_first : `bool`
        Whether to find the first match or not.
    limit : `int`
        Limit the number of results to be returned. A value of 0 means
        unlimited. A negative value is used to specify a cap where a warning
        is issued if that cap is hit.
    """
    butler = Butler.from_config(repo, writeable=True)
    datastore = cast(FileDatastore, butler._datastore)

    for dt in dataset_type:
        dataset_references = butler.query_datasets(
            dt, collections, find_first=find_first, where=where, limit=limit
        )

        uris = datastore.getManyURIs(dataset_references)

        prepare_checksum_task = partial(_compute_and_populate_checksum, datastore, uris)

        with ThreadPoolExecutor() as executor:
            results = executor.map(prepare_checksum_task, uris.keys())

        updated_stored_info = {}
        for r in results:
            if r is not None:
                ref, info = r
                updated_stored_info[ref] = info

        if updated_stored_info:
            datastore.addStoredItemInfo(
                list(updated_stored_info.keys()),
                list(updated_stored_info.values()),
                DatabaseInsertMode.REPLACE,
            )
            _LOG.info(f"Updated checksum for {len(updated_stored_info)} datasets.")
