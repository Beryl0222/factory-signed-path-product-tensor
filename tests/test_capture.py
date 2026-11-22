"""核对合成采集的交换记录与原始尺寸。"""
import json
from pathlib import Path
import unittest

import numpy as np

from caseio import load_case


class CaptureTests(unittest.TestCase):
    def setUp(self):
        data = Path(__file__).resolve().parents[1] / "data"
        self.records, self.tensors = load_case(data / "capture.json")
        self.capacity = json.loads((data / "capacity.json").read_text(encoding="utf-8"))

    def test_capture_contract(self):
        r, a = self.records, self.tensors
        self.assertEqual(r["shape"], [5, 4, 6])
        self.assertEqual(r["left"][0][1:3], r["left"][1][1:3])
        self.assertNotEqual(r["left"][0][0], r["left"][1][0])
        self.assertIn(r["edits"][0]["right_remove"][0], {row[0] for row in r["right"]})
        self.assertGreater(self.capacity["n"], 10000)
