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

__all__ = ["empty_trash"]


import logging

from lsst.daf.butler import Butler

_LOG = logging.getLogger(__name__)


def empty_trash(repo: str, verbose: bool, dry_run: bool) -> None:
    """Empty the datastore trash table.

    Parameters
    ----------
    repo : `str`
        URI of butler repository to update.
    verbose : `bool`
        If `True` report the datasets that were removed.
    dry_run : `bool`
        If `True` report how many datasets would be removed but do not
        remove them.
    """
    # Connect to the butler.
    butler = Butler.from_config(repo, writeable=True)

    try:
        removed = butler._datastore.emptyTrash(dry_run=dry_run)
    except AttributeError:
        print("Butler repository does not have a datastore that can support trash emptying")
        return

    if verbose and removed:
        print("Removed the following:")
        for uri in sorted(removed):
            print(uri)
