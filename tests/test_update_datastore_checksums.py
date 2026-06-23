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

import hashlib
import os
import tempfile
import unittest
from typing import cast

from lsst.daf.butler import Butler, DatasetRef, FileDataset
from lsst.daf.butler.datastores.fileDatastore import FileDatastore
from lsst.daf.butler.tests import DatasetTestHelper, addDataIdValue, addDatasetType, registerMetricsExample
from lsst.daf.butler.tests.utils import makeTestTempDir, removeTestTempDir
from lsst.daf.butler_admin.script import update_datastore_checksums

TESTDIR = os.path.abspath(os.path.dirname(__file__))


class TestUpdateDatastoreChecksums(unittest.TestCase, DatasetTestHelper):
    """Test case for update_datastore_checksums script."""

    def setUp(self) -> None:
        self.root = makeTestTempDir(TESTDIR)
        config = Butler.makeRepo(self.root)
        self.butler = Butler.from_config(config, run="test_coll")

        addDataIdValue(self.butler, "instrument", "sim")
        registerMetricsExample(self.butler)
        addDatasetType(self.butler, "test_type", {"instrument"}, "StructuredDataNoComponents")

        with tempfile.NamedTemporaryFile("w+", suffix=".txt", dir=self.root, delete=False) as tf:
            tf.write("Temp file")
            self.tf_path = tf.name

        self.ref = self.makeDatasetRef(
            "test_type",
            self.butler.dimensions.conform(("instrument",)),
            "StructuredDataNoComponents",
            {"instrument": "sim"},
            run="test_coll",
        )

        self.file_dataset = FileDataset(self.tf_path, self.ref)

    def tearDown(self) -> None:
        removeTestTempDir(self.root)

    def test_checksum_update(self) -> None:
        """Ingest dataset and updates its checksum value."""
        self.butler.ingest(self.file_dataset)

        def _check_checksum(ref: DatasetRef) -> str | None:
            datastore = cast(FileDatastore, self.butler._datastore)
            return datastore.getStoredItemsInfo(ref)[0].checksum

        # Check: initially no checksum should exist
        self.assertIsNone(_check_checksum(self.ref))

        with open(self.tf_path, "rb") as f:
            expected_checksum = hashlib.md5(f.read()).hexdigest()

        # Trigger checksum computation for file
        update_datastore_checksums(self.root, ["test_type"], ["test_coll"], "", True, limit=1)

        actual_checksum = _check_checksum(self.ref)

        self.assertEqual(
            actual_checksum, expected_checksum, f"Checksums differ: {actual_checksum} != {expected_checksum}"
        )

        self.tearDown()


if __name__ == "__main__":
    unittest.main()
