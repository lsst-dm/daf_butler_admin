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
import unittest

from lsst.daf.butler import Butler
from lsst.daf.butler.tests import (
    DatasetTestHelper,
    MetricsExample,
    addDataIdValue,
    addDatasetType,
    registerMetricsExample,
)
from lsst.daf.butler.tests.utils import makeTestTempDir, removeTestTempDir
from lsst.daf.butler_admin.script import empty_trash

TESTDIR = os.path.abspath(os.path.dirname(__file__))


class TestEmptyTrash(unittest.TestCase, DatasetTestHelper):
    """Test empty-trash script interface."""

    def setUp(self) -> None:
        self.root = makeTestTempDir(TESTDIR)
        config = Butler.makeRepo(self.root)
        self.butler = Butler.from_config(config, run="test")

        self.instruments = [f"cam{n}" for n in range(10)]
        for inst in self.instruments:
            addDataIdValue(self.butler, "instrument", inst)
        registerMetricsExample(self.butler)
        addDatasetType(self.butler, "metrics", {"instrument"}, "StructuredDataNoComponents")

        self.refs = []
        for inst in self.instruments:
            ref = self.makeDatasetRef(
                "metrics",
                self.butler.dimensions.conform(("instrument",)),
                "StructuredDataNoComponents",
                {"instrument": inst},
                run="test",
            )
            self.refs.append(ref)

        # Store some datasets in the butler.
        for i, ref in enumerate(self.refs):
            m = MetricsExample({"something": i})
            self.butler.put(m, ref)

    def tearDown(self) -> None:
        removeTestTempDir(self.root)

    def test_empty_trash(self) -> None:
        """Simple trash emptying.

        Must use low-level API to prevent pruneDatasets from automatically
        removing.
        """
        uris = [self.butler.getURI(ref) for ref in self.refs]
        self.butler._datastore.trash(self.refs)
        self.butler._registry.removeDatasets(self.refs)

        for uri in uris:
            self.assertTrue(uri.exists())

        # Pretend to remove.
        with self.assertLogs(level="INFO") as cm:
            empty_trash(self.root, dry_run=True, verbose=False)
        self.assertIn("Would have Removed 10", "\n".join(cm.output))
        for uri in uris:
            self.assertTrue(uri.exists())

        # Do the removal.
        with self.assertLogs(level="INFO") as cm:
            empty_trash(self.root, dry_run=False, verbose=False)
        self.assertIn("Removed 10", "\n".join(cm.output))
        for uri in uris:
            self.assertFalse(uri.exists(), str(uri))


if __name__ == "__main__":
    unittest.main()
