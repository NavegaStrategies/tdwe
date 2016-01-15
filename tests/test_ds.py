import os
import unittest
import tdwe
import pandas as pd

import logging
logging.basicConfig(level=logging.INFO)
logging.getLogger('suds').setLevel(logging.INFO)
logging.getLogger('suds.transport').setLevel(logging.DEBUG)


class TestTDWE(unittest.TestCase):
    def setUp(self):
        un = os.environ["TDWE_USER"]
        pw = os.environ["TDWE_PASS"]
        self.dwe = tdwe.TDWE(username=un, password=pw)
        # print(self.dwe.client)

    def test_system_info(self):
        r = self.dwe.system_info()
        self.assertIn("Version", r)
        self.assertIsInstance(r["Version"], list)

    def test_sources(self):
        r = self.dwe.sources()
        self.assertIsInstance(r, list)
        self.assertIn("Datastream", r)

    def test_request_single(self):
        # Basic check
        r = self.dwe.request_single("USGDP...D")
        self.assertEqual(r["StatusCode"], 0)
        self.assertEqual(r["StatusType"], "Connected")
        self.assertEqual(r["StatusMessage"], None)
        self.assertIn("Source", r)
        self.assertEqual(r["Source"], "Datastream")
        self.assertEqual(r["Instrument"], "USGDP...D")
        self.assertIn("Field", r["Fields"])

    def test_request_many(self):
        resp = self.dwe.request_many(["USGDP...D", "USCONPRCE"])
        print(resp)
        self.assertIn("Record", resp)
        r = resp["Record"][0]
        self.assertEqual(len(resp["Record"]), 2)
        self.assertEqual(r["StatusCode"], 0)
        self.assertEqual(r["StatusType"], "Connected")
        self.assertEqual(r["StatusMessage"], None)
        self.assertIn("Source", r)
        self.assertEqual(r["Source"], "Datastream")
        self.assertEqual(r["Instrument"], "USGDP...D")
        self.assertIn("Field", r["Fields"])

    def test_status(self):
        resp = self.dwe.request_single("USGDP...D")
        status = self.dwe.status(resp)
        self.assertIn("Code", status)
        self.assertIn("Message", status)
        self.assertIn("Type", status)
        self.assertIn("Instrument", status)
        self.assertIn("Source", status)

        with self.assertRaises(AttributeError) as e:
            resp = self.dwe.request_single("???")
            status = self.dwe.status(resp)
            if status["Code"] != tdwe.STATUS_CODE_OK:
                raise AttributeError(status)

    def test_parse(self):
        resp = self.dwe.request_single("USGDP...D")
        data, meta = self.dwe.parse_record_to_dataframe(resp)
        self.assertTrue(len(data) > 0)
        self.assertIsInstance(data, pd.DataFrame)
        self.assertEqual(meta["SYMBOL"], "USGDP...D")

