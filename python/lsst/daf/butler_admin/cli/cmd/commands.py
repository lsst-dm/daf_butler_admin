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

from typing import Any

import click
from lsst.daf.butler.cli.opt import repo_argument
from lsst.daf.butler.cli.utils import ButlerCommand

from ... import script


@click.group
def admin() -> None:
    """Run butler administration tasks."""
    pass


@admin.command(cls=ButlerCommand)
@click.option("--update", help="Execute updates, by default only print statistics.", is_flag=True)
@click.option(
    "--tagged", help="Only check tagged collections, ignored if --update is specified.", is_flag=True
)
@repo_argument(required=True)
def refresh_collection_summary(**kwargs: Any) -> None:
    """Refresh contents of the collection summary tables."""
    script.refresh_collection_summary(**kwargs)
