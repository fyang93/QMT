"""Offline checks for the QMT websocket universe contract."""
import importlib.util
import locale
import sys
import types
import unittest
from pathlib import Path
from queue import Queue
from unittest.mock import patch


service_path = Path(__file__).with_name("QMT_SERVICE.py")
qmt = importlib.util.module_from_spec(importlib.util.spec_from_loader("qmt_service", loader=None))
source = service_path.read_text(encoding="utf-8").replace("# -*- coding: gbk -*-", "# -*- coding: utf-8 -*-", 1)
class _TornadoStub:
    pass


web = types.ModuleType("tornado.web")
web.Application = web.RequestHandler = web.WebSocketHandler = _TornadoStub
web.HTTPError = type("HTTPError", (Exception,), {})
ioloop = types.ModuleType("tornado.ioloop")
ioloop.IOLoop = ioloop.PeriodicCallback = _TornadoStub
httpserver = types.ModuleType("tornado.httpserver")
httpserver.HTTPServer = _TornadoStub
websocket = types.ModuleType("tornado.websocket")
websocket.WebSocketHandler = _TornadoStub
with patch.object(locale, "setlocale"), patch.dict(sys.modules, {
    "tornado": types.ModuleType("tornado"), "tornado.web": web,
    "tornado.ioloop": ioloop, "tornado.httpserver": httpserver,
    "tornado.websocket": websocket,
}):
    exec(compile(source, str(service_path), "exec"), qmt.__dict__)


class UniverseProtocolTest(unittest.TestCase):
    def setUp(self):
        qmt._UNIVERSE_SUBSCRIBED.clear()
        qmt._BACKFILL_PENDING.clear()
        qmt._BACKFILL_QUEUE = Queue()
        self.client = qmt.MarketWebSocketHandler.__new__(qmt.MarketWebSocketHandler)
        self.client.subscriptions = {}
        self.client.sent = []
        self.client.send_json = self.client.sent.append
        self.client.refresh_subscriptions = lambda: None

    def test_false_and_true_scope_keeps_full_history_universe(self):
        self.client.universe = {}
        rows = [
            {"symbol": "600000.SH", "subscribed": False},
            {"symbol": "000300.SH", "subscribed": True},  # benchmark
        ]
        self.client.handle_configure({"universe": rows})

        self.assertEqual(self.client.universe, {"600000.SH": False, "000300.SH": True})
        self.assertEqual(qmt._UNIVERSE_SUBSCRIBED, self.client.universe)
        self.assertEqual(qmt._BACKFILL_PENDING, {
            ("600000.SH", "1d"), ("600000.SH", "5m"),
            ("000300.SH", "1d"), ("000300.SH", "5m"), ("000300.SH", "1m"),
        })
        self.assertEqual(self.client.sent[0]["symbols"], 2)
        self.assertEqual(self.client.sent[0]["realtime"], 1)

    def test_invalid_or_legacy_boolean_fails_without_replacing_scope(self):
        self.client.universe = {"OLD.SH": True}
        for subscribed in ("true", 1, None):
            with self.subTest(subscribed=subscribed):
                self.client.handle_configure({"universe": [{"symbol": "600000.SH", "subscribed": subscribed}]})
                self.assertEqual(self.client.universe, {"OLD.SH": True})
                self.assertEqual(self.client.sent[-1]["type"], "error")
        for row in (
            {"symbol": "600000.SH", "status": "active"},
            {"symbol": "600000.SH", "active": True},
        ):
            with self.subTest(row=row):
                self.client.handle_configure({"universe": [row]})
                self.assertEqual(self.client.universe, {"OLD.SH": True})
                self.assertEqual(self.client.sent[-1]["type"], "error")

    def test_over_limit_realtime_scope_is_rejected_not_truncated(self):
        rows = [{"symbol": "S%d.SH" % i, "subscribed": True} for i in range(51)]
        self.client.universe = {}
        self.client.handle_configure({"universe": rows})
        self.assertEqual(self.client.universe, {})
        self.assertEqual(self.client.sent[-1]["type"], "error")


if __name__ == "__main__":
    unittest.main()
