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

__all__ = ["update_storage_class"]

from lsst.daf.butler import Butler, DatasetType, StorageClass
from lsst.daf.butler.direct_butler import DirectButler
from lsst.daf.butler.registry.datasets.byDimensions import ByDimensionsDatasetRecordStorageManagerUUID


def update_storage_class(
    repo: str, update: bool, dataset_type: str, storage_class: str, to_storage_class: str
) -> None:
    """Update storage class definition for some dataset types.

    Parameters
    ----------
    repo : `str`
        URI of butler repository to update.
    update : `bool`
        Perform actual updates if `True`, print actions otherwise.
    dataset_type : `str`
        Dataset type name or glob to match multiple dataset types.
    storage_class : `str`
        Name of the existing storage class in the dataset type.
    to_storage_class : `str`
        Name of the storage class to assign to matching dataset types.
    """
    # Connect to the butler.
    butler = Butler.from_config(repo, writeable=True)
    assert isinstance(butler, DirectButler), "This script requires DirectButler."

    try:
        old_storage_class = butler.storageClasses.getStorageClass(storage_class)
    except KeyError:
        raise ValueError(f"Storage class {storage_class} does not exist") from None
    try:
        new_storage_class = butler.storageClasses.getStorageClass(to_storage_class)
    except KeyError:
        raise ValueError(f"Storage class {to_storage_class} does not exist") from None

    # The code below may need to import Python types, check that it works.
    _check_import(old_storage_class)
    _check_import(new_storage_class)

    # Check that storage classes are compatible
    if not new_storage_class.can_convert(old_storage_class):
        raise TypeError(f"Storage class {to_storage_class} cannot convert from {storage_class}")

    dataset_types = [
        ds_type
        for ds_type in butler.registry.queryDatasetTypes(expression=dataset_type)
        if ds_type.storageClass.name == storage_class
    ]
    if not dataset_types:
        print("No matching dataset types were found.")
    elif not update:
        print("Will update storage class for following dataset types:")
        for ds_type in dataset_types:
            print(ds_type)
        print("\nDatabase was not updated - use --update option to apply these changes.")
    else:
        _update(butler, dataset_types, to_storage_class)


def _check_import(storage_class: StorageClass) -> None:
    """Check that Python type for this StorageClass can be imported.

    Parameters
    ----------
    storage_class : `lsst.daf.butler.StorageClass`
        Storage class to be checked.
    """
    try:
        storage_class.pytype
    except ImportError as exc:
        raise RuntimeError(
            f"Import failed for Python type of storage class {storage_class}."
            " Please make sure that corresponding package is set up."
        ) from exc


def _update(butler: DirectButler, dataset_types: list[DatasetType], storage_class: str) -> None:
    """Update database definition of dataset types with new storage class.

    Parameters
    ----------
    butler : `lsst.daf.butler.DirectButler`
        Data butler to be updated.
    dataset_types : `list` [`lsst.daf.butler.DatasetType`]
        List of data set types to update.
    storage_class : `str`
        Name of the storage class.

    Notes
    -----
    There is no Butler or Registry interface for this operation, this has to be
    done using their internals.
    """
    # We need SqlRegistry.
    registry = butler._registry
    dataset_manager = registry._managers.datasets
    assert isinstance(dataset_manager, ByDimensionsDatasetRecordStorageManagerUUID), (
        "Unexpected type of dataset manager"
    )

    dataset_type_table = dataset_manager._static.dataset_type

    rows = [{"ds_name": ds_type.name, "storage_class": storage_class} for ds_type in dataset_types]
    count = registry._db.update(dataset_type_table, {"name": "ds_name"}, *rows)
    print(f"Updated {count} dataset type record{'' if count == 1 else 's'} in database.")
