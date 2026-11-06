"""标注流水线纯函数单测（不需要数据库）。

运行::

    cd /users/Min/FediMiss && python3 -m unittest tests.test_labeling
"""

from __future__ import annotations

import json
import unittest
from datetime import datetime, timedelta, timezone

from fedimiss.labeling.agreement import (binary_bot_metrics, cohen_kappa,
                                         confusion_table, per_label_metrics,
                                         wilson_interval)
from fedimiss.labeling.evidence import _normalize_text, _timing_stats
from fedimiss.labeling.llm_label import extract_json, normalize_result, render_evidence


def _ts(hours_offset: float) -> datetime:
    base = datetime(2025, 1, 1, tzinfo=timezone.utc)
    return base + timedelta(hours=hours_offset)


class TimingStatsTest(unittest.TestCase):
    def test_regular_hourly_bot(self):
        times = [_ts(float(i)) for i in range(48)]  # 整点一条，持续两天
        stats = _timing_stats(times)
        self.assertAlmostEqual(stats["gap_cv"], 0.0, places=4)
        self.assertEqual(stats["modal_interval_minutes"], 60)
        self.assertAlmostEqual(stats["modal_interval_share"], 1.0, places=4)
        self.assertEqual(stats["active_hours"], 24)

    def test_empty(self):
        stats = _timing_stats([])
        self.assertIsNone(stats["gap_cv"])
        self.assertEqual(stats["active_days"], 0)


class NormalizeTextTest(unittest.TestCase):
    def test_url_and_case(self):
        self.assertEqual(_normalize_text("看 https://a.b/c 呀！"), "看<url>呀！")
        self.assertEqual(_normalize_text("OK OK"), "okok")


class ExtractJsonTest(unittest.TestCase):
    def test_fenced_and_prefix(self):
        text = "结论如下：\n```json\n{\"label\": \"bot\", \"evidence\": [\"a\"]}\n```\n"
        self.assertEqual(extract_json(text)["label"], "bot")

    def test_none(self):
        self.assertIsNone(extract_json("no json here"))


class NormalizeResultTest(unittest.TestCase):
    def test_no_evidence_downgrade(self):
        out = normalize_result({"label": "bot", "confidence": "high", "rule_ids": ["3.1-1"]})
        self.assertEqual(out["label"], "uncertain")
        self.assertEqual(out["uncertain_reason"], "model_no_evidence")

    def test_invalid_label(self):
        out = normalize_result({"label": "maybe", "evidence": ["x"]})
        self.assertEqual(out["label"], "uncertain")

    def test_uncertain_needs_reason(self):
        out = normalize_result({"label": "uncertain", "confidence": "low", "evidence": ["x"]})
        self.assertEqual(out["uncertain_reason"], "unspecified")


class AgreementTest(unittest.TestCase):
    def test_kappa_bounds(self):
        left = ["bot", "human", "uncertain", "bot"]
        self.assertEqual(cohen_kappa(left, left), 1.0)
        # 完全错开：观察一致率 0，期望一致率 0.5 → κ = -1
        self.assertEqual(cohen_kappa(["bot", "human"], ["human", "bot"]), -1.0)

    def test_per_label_and_binary(self):
        pairs = [("bot", "bot"), ("bot", "human"), ("human", "human"),
                 ("uncertain", "uncertain"), ("bot", "bot")]
        metrics = per_label_metrics(pairs)
        self.assertEqual(metrics["bot"]["support"], 3)
        self.assertAlmostEqual(metrics["bot"]["precision"], 1.0, places=4)
        self.assertAlmostEqual(metrics["bot"]["recall"], 2 / 3, places=4)
        binary = binary_bot_metrics(pairs)
        self.assertEqual((binary["tp"], binary["fp"], binary["fn"]), (2, 0, 1))

    def test_confusion_and_wilson(self):
        matrix = confusion_table([("bot", "bot"), ("human", "bot")])
        self.assertEqual(matrix[0][0], 1)
        self.assertEqual(matrix[1][0], 1)
        low, high = wilson_interval(9, 10)
        self.assertLess(low, 0.9)
        self.assertGreater(high, 0.9)


class RenderEvidenceTest(unittest.TestCase):
    def test_renders_samples_and_sections(self):
        pack = json.loads(json.dumps({
            "account_key": "a@b",
            "profile": {"display_name": "x"},
            "timing": {"gap_cv": 0.1},
            "samples": [{"created_at": "2025-01-01T00:00:00Z", "text": "hi",
                         "has_media": True, "note_key": "b#1"}],
        }))
        text = render_evidence(pack)
        self.assertIn("account_key: a@b", text)
        self.assertIn("gap_cv", text)
        self.assertIn("[media]", text)


if __name__ == "__main__":
    unittest.main()
