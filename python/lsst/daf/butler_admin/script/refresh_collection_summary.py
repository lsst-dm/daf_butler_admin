# This file is part of daf_butler_migrate.
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

__all__ = ["refresh_collection_summary"]

import logging
from collections.abc import Iterable

from lsst.daf.butler import Butler, CollectionType

_LOG = logging.getLogger(__name__)


def refresh_collection_summary(repo: str, update: bool, tagged: bool) -> None:
    """Refresh contents of the collection summary tables.

    Parameters
    ----------
    repo : `str`
        URI of butler repository to update.
    update : `bool`
        Perform actual updates if `True`, print actions otherwise.
    tagged : `bool`
        Only check tagged collections, ignored if ``update`` is `True`.
    """
    # Connect to the butler.
    butler = Butler.from_config(repo, writeable=True)

    registry = butler.registry
    if update:
        registry.refresh_collection_summaries()
    else:
        # There are no registry methods to compare summaries with actual
        # contents, use brute force by scanning all collections (this takes
        # long time). Note that it could result in false alarms due to
        # possible concurrent updates.
        collection_types: Iterable[CollectionType] = (
            [CollectionType.TAGGED] if tagged else CollectionType.all()
        )
        collections = sorted(registry.queryCollections(collectionTypes=collection_types, includeChains=False))
        for collection in collections:
            collection_type = registry.getCollectionType(collection)
            summary = registry.getCollectionSummary(collection)
            summary_types = set(summary.dataset_types.names)
            dataset_types = {
                ref.datasetType.name for ref in registry.queryDatasets(..., collections=collection)
            }
            diff = summary_types - dataset_types
            if diff:
                print(
                    f"Summary for {collection_type.name} collection {collection} "
                    f"contains {len(diff)} extra dataset types."
                )
            diff = dataset_types - summary_types
            if diff:
                print(
                    f"Summary for {collection_type.name} collection {collection} "
                    f"contains {len(diff)} missing dataset types."
                )
            if dataset_types == summary_types:
                print(
                    f"Summary for {collection_type.name} collection {collection} is consistent "
                    f"with {len(dataset_types)} dataset types."
                )
