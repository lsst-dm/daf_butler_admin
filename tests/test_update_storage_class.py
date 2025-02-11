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

import os
import tempfile
import unittest

from lsst.daf.butler import Butler, DatasetType, DimensionGroup
from lsst.daf.butler.tests.utils import makeTestTempDir, removeTestTempDir
from lsst.daf.butler_admin.script import update_storage_class

TESTDIR = os.path.abspath(os.path.dirname(__file__))


class TestUpdateStorageClass(unittest.TestCase):
    """Test case for update_storage_class script."""

    def setUp(self) -> None:
        self.root = makeTestTempDir(TESTDIR)

    def tearDown(self) -> None:
        removeTestTempDir(self.root)

    def make_butler(self) -> str:
        """Make a Butler instance with universe of specific version."""
        butler_root = tempfile.mkdtemp(dir=self.root)
        Butler.makeRepo(butler_root)
        return butler_root

    def test_update(self) -> None:
        """Create few dataset types and update their storage class."""
        butler_root = self.make_butler()

        # Register few dataset types. For this test we want to use storage
        # classes that do not require setting up additional packages.
        butler = Butler.from_config(butler_root, writeable=True)
        dimensions = DimensionGroup(butler.dimensions)
        initial_storage_classes = {
            "a_metadata": "StructuredDataDict",
            "b_other": "StructuredDataDict",
            "c_metadata": "StructuredDataDict",
        }

        for name, storage_class in initial_storage_classes.items():
            butler.registry.registerDatasetType(
                DatasetType(name=name, dimensions=dimensions, storageClass=storage_class)
            )

        # Check storage classes.
        for name, storage_class in initial_storage_classes.items():
            self.assertEqual(butler.get_dataset_type(name).storageClass.name, storage_class)

        # Update one.
        update_storage_class(
            repo=butler_root,
            update=True,
            dataset_type="*_metadata",
            storage_class="StructuredDataDict",
            to_storage_class="Packages",
        )

        # Need new Butler instance to avoid caching issues.
        butler = Butler.from_config(butler_root, writeable=True)

        new_storage_classes = {
            "a_metadata": "Packages",
            "b_other": "StructuredDataDict",
            "c_metadata": "Packages",
        }
        for name, storage_class in new_storage_classes.items():
            self.assertEqual(butler.get_dataset_type(name).storageClass.name, storage_class)

        # Update everything.
        update_storage_class(
            repo=butler_root,
            update=True,
            dataset_type="*",
            storage_class="StructuredDataDict",
            to_storage_class="Packages",
        )

        butler = Butler.from_config(butler_root, writeable=True)

        new_storage_classes = {
            "a_metadata": "Packages",
            "b_other": "Packages",
            "c_metadata": "Packages",
        }
        for name, storage_class in new_storage_classes.items():
            self.assertEqual(butler.get_dataset_type(name).storageClass.name, storage_class)

        # Non-convertible classes.
        with self.assertRaisesRegex(TypeError, "Storage class ButlerLogRecords cannot convert from Packages"):
            update_storage_class(
                repo=butler_root,
                update=True,
                dataset_type="*",
                storage_class="Packages",
                to_storage_class="ButlerLogRecords",
            )

        # Unknown class name.
        with self.assertRaisesRegex(ValueError, "Storage class NotAClass does not exist"):
            update_storage_class(
                repo=butler_root,
                update=True,
                dataset_type="*",
                storage_class="Packages",
                to_storage_class="NotAClass",
            )


if __name__ == "__main__":
    unittest.main()
