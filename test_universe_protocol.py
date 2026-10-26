"""Offline checks; no QMT runtime, account, network or native Tornado required."""
import importlib.util
import json
import locale
import sys
import tempfile
import types
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import Mock, patch


service_path = Path(__file__).with_name("QMT_SERVICE.py")
qmt = importlib.util.module_from_spec(importlib.util.spec_from_loader("qmt_service", loader=None))
qmt.__file__ = str(service_path)
source = service_path.read_text(encoding="utf-8").replace("# -*- coding: gbk -*-", "# -*- coding: utf-8 -*-", 1)


class _TornadoStub:
    def __init__(self, *args, **settings):
        self.settings = settings


web = types.ModuleType("tornado.web")
web.Application = web.RequestHandler = _TornadoStub
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
        for name in ("_UNIVERSE_SUBSCRIBED", "_UNIVERSE_CLIENTS", "_REALTIME_SUBSCRIPTIONS",
                     "_REALTIME_RELEASE_AT", "_BACKFILL_RESULTS", "_HISTORY_CLIENT_ACKS"):
            getattr(qmt, name).clear()
        qmt._BACKFILL_PENDING.clear()
        qmt._BACKFILL_RUNNING.clear()
        qmt._HISTORY_MANIFESTS.clear()
        qmt._WS_CLIENTS.clear()
        qmt._HISTORY_SEQUENCE = 0
        qmt._REALTIME_LAST_5M = 0
        qmt._IOLOOP_LAST_TICK = None
        qmt._IOLOOP_LAG_SAMPLES.clear()
        qmt._REALTIME_LAST_PUSH_AT = None
        qmt._REALTIME_PUSHED_SYMBOL_COUNT = 0
        qmt._BACKFILL_STOP.clear()
        qmt._BACKFILL_STATE_PATH = None
        qmt._IOLOOP = types.SimpleNamespace(add_callback=lambda function, *args: function(*args))
        self.ctx = Mock()
        self.ctx.subscribe_quote.side_effect = range(1, 1000)
        self.ctx.get_market_data_ex.side_effect = lambda fields, stocks, *args: {stock: {"close": {"20261026093100": 10}} for stock in stocks}
        qmt._APP = types.SimpleNamespace(ContextInfo=self.ctx)
        self.client = self.make_client("first")

    def make_client(self, name):
        client = qmt.MarketWebSocketHandler.__new__(qmt.MarketWebSocketHandler)
        client.initialize()
        client.request = types.SimpleNamespace(remote_ip="10.0.0." + str(len(qmt._WS_CLIENTS) + 1))
        client.application = qmt._APP
        client.sent = []
        client.send_json = client.sent.append
        client.close = Mock(side_effect=lambda **kwargs: client.on_close())
        client.client_id = name
        qmt._WS_CLIENTS.add(client)
        return client

    def configure(self, client, universe, **options):
        client.handle_configure({"client_id": client.client_id, "universe": [
            {"symbol": stock, "subscribed": subscribed} for stock, subscribed in universe.items()
        ], **options})

    def test_false_and_true_scope_keeps_full_history_universe(self):
        self.configure(self.client, {"600000.SH": False, "000300.SH": True})
        self.assertEqual(qmt._UNIVERSE_SUBSCRIBED, {"600000.SH": False, "000300.SH": True})
        self.assertEqual(qmt._BACKFILL_PENDING, {
            ("600000.SH", "1d"), ("600000.SH", "5m"),
            ("000300.SH", "1d"), ("000300.SH", "5m"), ("000300.SH", "1m"),
        })
        self.assertEqual(self.client.sent[0]["symbols"], 2)
        self.assertEqual(self.client.sent[0]["realtime"], 1)

    def test_invalid_or_legacy_boolean_fails_without_replacing_scope(self):
        self.configure(self.client, {"OLD.SH": True})
        for row in (
            {"symbol": "600000.SH", "subscribed": "true"},
            {"symbol": "600000.SH", "subscribed": 1},
            {"symbol": "600000.SH", "subscribed": None},
            {"symbol": "600000.SH", "status": "active"},
        ):
            self.client.handle_configure({"universe": [row]})
            self.assertEqual(self.client.universe, {"OLD.SH": True})
            self.assertEqual(self.client.sent[-1]["type"], "error")

    def test_combined_limit_is_rejected_without_mutating_other_client(self):
        self.configure(self.client, {"S%d.SH" % i: True for i in range(50)})
        second = self.make_client("second")
        self.configure(second, {"NEW.SH": True})
        self.assertEqual(second.sent[-1]["type"], "error")
        self.assertNotIn("second", qmt._UNIVERSE_CLIENTS)
        self.assertEqual(len(qmt._UNIVERSE_SUBSCRIBED), 50)
        self.assertEqual(self.ctx.subscribe_quote.call_count, 100)

    def test_retiring_quotes_count_toward_global_native_limit(self):
        with patch.object(qmt, "_QMT_REALTIME_SYMBOL_LIMIT", 2), patch.object(qmt, "monotonic", return_value=1000):
            self.configure(self.client, {"A.SH": True})
            self.configure(self.client, {"A.SH": False})
            second = self.make_client("second")
            self.configure(second, {"B.SH": True, "C.SH": True})
        self.assertEqual(second.sent[-1]["type"], "error")
        self.assertEqual(set(qmt._REALTIME_SUBSCRIPTIONS), {("A.SH", "1m"), ("A.SH", "5m")})

    def test_empty_configured_client_cannot_change_identity_and_leave_ghost_state(self):
        self.configure(self.client, {})
        self.client.handle_configure({"client_id": "different", "universe": []})
        self.assertEqual(self.client.sent[-1]["type"], "error")
        self.assertEqual(set(qmt._UNIVERSE_CLIENTS), {"first"})

    def test_overlapping_clients_subscribe_and_read_once_and_filter_delivery(self):
        self.configure(self.client, {"A.SH": True, "B.SH": True})
        second = self.make_client("second")
        self.configure(second, {"B.SH": True, "C.SH": True})
        self.assertEqual(qmt._UNIVERSE_SUBSCRIBED, {"A.SH": True, "B.SH": True, "C.SH": True})
        self.assertEqual(self.ctx.subscribe_quote.call_count, 6)
        with patch.object(qmt, "monotonic", return_value=1000):
            qmt.push_global_market_data()
        calls = self.ctx.get_market_data_ex.call_args_list
        self.assertEqual(len(calls), 2)
        self.assertEqual([call.args[2] for call in calls], ["1m", "5m"])
        for call in calls:
            self.assertEqual(call.args[1], ["A.SH", "B.SH", "C.SH"])
            self.assertEqual(call.args[5:], (2, "none", False, False))
        self.assertEqual(self.client.sent[-1]["stocks"], ["A.SH", "B.SH"])
        self.assertEqual(second.sent[-1]["stocks"], ["B.SH", "C.SH"])
        with patch.object(qmt, "monotonic", return_value=1001):
            qmt.push_global_market_data()
        self.assertEqual(self.ctx.get_market_data_ex.call_count, 3)  # 5m waits five seconds.

    def test_disconnect_grace_keeps_quotes_and_other_client_live(self):
        self.configure(self.client, {"A.SH": True, "B.SH": True})
        second = self.make_client("second")
        self.configure(second, {"B.SH": True})
        with patch.object(qmt, "monotonic", return_value=1000):
            self.client.on_close()
            qmt.push_global_market_data()
        self.ctx.unsubscribe_quote.assert_not_called()
        self.assertEqual(second.sent[-1]["stocks"], ["B.SH"])
        with patch.object(qmt, "monotonic", return_value=1119):
            qmt.push_global_market_data()
        self.ctx.unsubscribe_quote.assert_not_called()
        with patch.object(qmt, "monotonic", return_value=1120):
            qmt.push_global_market_data()
        self.assertEqual(qmt._UNIVERSE_SUBSCRIBED, {"B.SH": True})
        self.assertEqual(set(qmt._REALTIME_SUBSCRIPTIONS), {("B.SH", "1m"), ("B.SH", "5m")})
        self.assertEqual(self.ctx.unsubscribe_quote.call_count, 2)

    def test_same_client_id_reconnect_replaces_state_and_old_close_cannot_release_new(self):
        self.configure(self.client, {"A.SH": True})
        replacement = self.make_client("first")
        self.configure(replacement, {"A.SH": True, "B.SH": False})
        self.client.close.assert_called_once()
        self.assertIs(qmt._UNIVERSE_CLIENTS["first"]["connection"], replacement)
        self.assertIsNone(qmt._UNIVERSE_CLIENTS["first"]["expires"])
        self.assertEqual(self.ctx.subscribe_quote.call_count, 2)
        self.assertEqual(replacement.sent[0]["queued"], 2)
        self.assertEqual(len(qmt._UNIVERSE_CLIENTS), 1)

    def test_explicit_reference_removal_also_has_unsubscribe_grace(self):
        self.configure(self.client, {"A.SH": True})
        with patch.object(qmt, "monotonic", return_value=1000):
            self.configure(self.client, {"A.SH": False})
        self.ctx.unsubscribe_quote.assert_not_called()
        with patch.object(qmt, "monotonic", return_value=1120):
            qmt.push_global_market_data()
        self.assertEqual(self.ctx.unsubscribe_quote.call_count, 2)

    def test_two_clients_queue_and_download_same_pair_once(self):
        self.configure(self.client, {"A.SH": True})
        second = self.make_client("second")
        self.configure(second, {"A.SH": True})
        self.assertEqual(second.sent[0]["queued"], 0)
        downloads = []

        def download(stocks, period, start, end):
            downloads.append((stocks, period, start, end))
            qmt._BACKFILL_STOP.set()

        with patch.object(qmt, "trading_hours", return_value=True), patch.object(qmt, "schedule_incremental_backfill"), patch.object(qmt, "download_history_range", side_effect=download):
            qmt.backfill_worker()
        self.assertEqual(len(downloads), 1)
        self.assertEqual(downloads[0][:2], (["A.SH"], "1m"))
        self.assertTrue(all(downloads[0][2:]))
        self.assertNotIn(("A.SH", "1m"), qmt._BACKFILL_PENDING)
        self.assertEqual(qmt._BACKFILL_PENDING, {("A.SH", "1d"), ("A.SH", "5m")})
        for client in (self.client, second):
            self.assertEqual(client.sent[-1]["type"], "history_ready")

    def test_trading_hours_include_beijing_weekday_boundaries(self):
        for text, expected in (("2026-10-26T09:14:59+08:00", False), ("2026-10-26T01:15:00+00:00", True),
                               ("2026-10-26T15:05:59+08:00", True), ("2026-10-26T15:06:00+08:00", False),
                               ("2026-10-25T10:00:00+08:00", False)):
            self.assertEqual(qmt.trading_hours(datetime.fromisoformat(text)), expected)

    def test_trading_batch_defers_archives_and_unsubscribed_minutes(self):
        self.configure(self.client, {"A.SH": True, "B.SH": False})
        qmt._BACKFILL_PENDING.add(("B.SH", "1m"))
        with patch.object(qmt, "trading_hours", return_value=True):
            self.assertEqual(qmt.take_backfill_batch(), [("A.SH", "1m")])
            self.assertEqual(qmt.take_backfill_batch(), [])
        self.assertEqual(qmt._BACKFILL_PENDING, {("A.SH", "1d"), ("A.SH", "5m"), ("B.SH", "1d"), ("B.SH", "5m")})
        with patch.object(qmt, "trading_hours", return_value=False):
            self.assertEqual({period for stock, period in qmt.take_backfill_batch()}, {"1d"})

    def test_trading_http_download_is_rejected_before_any_native_call(self):
        handler = qmt.HistoryDataHandler.__new__(qmt.HistoryDataHandler)
        handler.json_body = lambda: {"stock_code": "A.SH", "period": "1d", "download": True}
        handler.set_header = Mock()
        handler.ctx = Mock(return_value=self.ctx)
        with patch.object(qmt, "trading_hours", return_value=True), patch.object(qmt, "download_history_range") as downloader:
            with self.assertRaises(qmt.HTTPError) as error:
                handler.post()
        self.assertEqual(error.exception.args[0], 503)
        downloader.assert_not_called()
        handler.ctx.assert_not_called()

    def test_local_history_reads_are_paged(self):
        qmt.validate_history_page(["A.SH"] * 5, "5m", "20261001000000", "20261007235959", -1)
        for stocks, period, start, end, count in ((["A.SH"] * 6, "1m", "", "", 1),
                (["A.SH"], "5m", "20261001000000", "20261009235959", -1),
                (["A.SH"], "1d", "", "", -1), (["A.SH"], "1d", "", "", 100000)):
            with self.assertRaises(qmt.HTTPError):
                qmt.validate_history_page(stocks, period, start, end, count)

    def test_manifest_ack_is_per_client_and_replay_is_filtered(self):
        self.configure(self.client, {"A.SH": False, "B.SH": True})
        second = self.make_client("second")
        self.configure(second, {"B.SH": True, "C.SH": False})
        qmt._announce_history(["B.SH", "A.SH"], "1d", "20260101000000", "20261026150000", False)
        manifest = qmt._HISTORY_MANIFESTS[0]
        self.assertEqual(self.client.sent[-1]["symbols"], ["A.SH", "B.SH"])
        self.assertEqual(second.sent[-1]["symbols"], ["B.SH"])
        self.client.handle_history_ack({"id": manifest["id"], "bridge_id": qmt._BRIDGE_ID})
        self.assertIsNone(qmt.history_for_client(manifest, self.client))
        self.assertEqual(qmt.history_for_client(manifest, second)["symbols"], ["B.SH"])
        self.assertEqual(len(qmt._HISTORY_MANIFESTS), 1)
        self.configure(self.client, self.client.universe)
        self.assertEqual(self.client.sent[-1]["type"], "configured")
        self.configure(second, second.universe)
        self.assertEqual(second.sent[-1]["type"], "history_ready")
        third = self.make_client("third")
        self.configure(third, {"C.SH": False})
        self.assertEqual(third.sent[-1]["type"], "configured")

    def test_manifest_merges_bounded_and_incremental_and_caps_archive(self):
        qmt._announce_history(["B.SH", "A.SH"], "5m", "20260101000000", "20261025150000", False)
        old_id = qmt._HISTORY_MANIFESTS[0]["id"]
        qmt._announce_history(["A.SH", "B.SH"], "5m", "20261020000000", "20261026150000", True)
        self.assertEqual(len(qmt._HISTORY_MANIFESTS), 1)
        merged = qmt._HISTORY_MANIFESTS[0]
        self.assertNotEqual(merged["id"], old_id)
        self.assertFalse(merged["incremental"])
        self.assertEqual(merged["start_time"], (datetime(2026, 10, 26, 15) - timedelta(days=180)).strftime("%Y%m%d%H%M%S"))
        self.assertEqual(merged["end_time"], "20261026150000")
        with patch.object(qmt, "_HISTORY_MANIFEST_LIMIT", 2):
            qmt._announce_history(["C.SH"], "5m", "20260101000000", "20261026150000", False)
            qmt._announce_history(["D.SH"], "5m", "20260101000000", "20261026150000", False)
        self.assertEqual(len(qmt._HISTORY_MANIFESTS), 2)

    def test_ack_with_old_bridge_namespace_cannot_ack_current_work(self):
        self.configure(self.client, {"A.SH": True})
        qmt._announce_history(["A.SH"], "1m", "20261020000000", "20261026100000", False)
        manifest = qmt._HISTORY_MANIFESTS[0]
        self.client.handle_history_ack({"id": manifest["id"], "bridge_id": "old-process"})
        self.assertEqual(self.client.sent[-1]["type"], "error")
        self.assertIsNotNone(qmt.history_for_client(manifest, self.client))

    def test_persisted_success_is_reused_without_download_and_reannounced_after_restart(self):
        with tempfile.TemporaryDirectory() as directory:
            qmt._BACKFILL_STATE_PATH = str(Path(directory) / "state.json")
            qmt._BACKFILL_RESULTS["A.SH:1d"] = {"status": "success", "start_time": "20000101000000", "end_time": "20991231150000"}
            qmt._BACKFILL_RESULTS["B.SH:1d"] = {"status": "failed"}
            qmt.save_backfill_results()
            qmt._BACKFILL_RESULTS.clear()
            qmt.load_backfill_results()
            self.assertEqual(set(qmt._BACKFILL_RESULTS), {"A.SH:1d"})
            self.configure(self.client, {"A.SH": False})
            self.assertNotIn(("A.SH", "1d"), qmt._BACKFILL_PENDING)
            self.assertIn(("A.SH", "5m"), qmt._BACKFILL_PENDING)
            manifests = [item for item in self.client.sent if item["type"] == "history_ready"]
            self.assertEqual(manifests[-1]["period"], "1d")
            self.assertEqual(manifests[-1]["symbols"], ["A.SH"])
            self.configure(self.client, {"A.SH": False}, refresh_history=True)
            self.assertNotIn("A.SH:1d", qmt._BACKFILL_RESULTS)
            self.assertIn(("A.SH", "1d"), qmt._BACKFILL_PENDING)
            qmt._BACKFILL_RESULTS.clear()
            qmt.load_backfill_results()
            self.assertNotIn("A.SH:1d", qmt._BACKFILL_RESULTS)

    def test_expanded_scope_gets_new_manifest_after_acking_filtered_archive(self):
        self.configure(self.client, {"A.SH": False})
        second = self.make_client("second")
        self.configure(second, {"B.SH": False})
        for stock in ("A.SH", "B.SH"):
            qmt._BACKFILL_RESULTS[stock + ":1d"] = {"status": "success", "start_time": "20000101000000", "end_time": "20991231150000"}
        qmt._announce_history(["A.SH", "B.SH"], "1d", "20260101000000", "20261026150000", False)
        old = self.client.sent[-1]
        self.client.handle_history_ack({"id": old["id"]})
        self.configure(self.client, {"A.SH": False, "B.SH": False})
        fresh = [item for item in self.client.sent if item["type"] == "history_ready" and item["id"] != old["id"] and "B.SH" in item["symbols"]]
        self.assertTrue(fresh)

    def test_stale_persisted_download_rebuilds_full_window_not_just_incremental_tail(self):
        start, end = qmt.backfill_range("1d")
        qmt._BACKFILL_RESULTS["A.SH:1d"] = {"status": "success", "start_time": "20000101000000", "end_time": (datetime.now(qmt._QMT_TIMEZONE) - timedelta(days=20)).strftime("%Y%m%d%H%M%S")}
        self.configure(self.client, {"A.SH": False})
        manifest = self.client.sent[-1]
        self.assertEqual(manifest["type"], "history_ready")
        self.assertFalse(manifest["incremental"])
        self.assertEqual(manifest["start_time"], start)
        self.assertIn(("A.SH", "1d"), qmt._BACKFILL_PENDING)

    def test_evicted_unacked_manifest_is_rebuilt_on_unchanged_configure(self):
        self.configure(self.client, {"A.SH": False})
        qmt._BACKFILL_RESULTS["A.SH:1d"] = {"status": "success", "start_time": "20000101000000", "end_time": "20991231150000"}
        with patch.object(qmt, "_HISTORY_MANIFEST_LIMIT", 2):
            for stock in ("A.SH", "B.SH", "C.SH"):
                qmt._announce_history([stock], "1d", *qmt.backfill_range("1d"), False)
            self.assertNotIn("A.SH", {stock for item in qmt._HISTORY_MANIFESTS for stock in item["symbols"]})
            self.configure(self.client, self.client.universe)
        self.assertEqual(self.client.sent[-1]["type"], "history_ready")
        self.assertEqual(self.client.sent[-1]["symbols"], ["A.SH"])
        self.assertFalse(self.client.sent[-1]["incremental"])
        self.assertEqual(len(qmt._HISTORY_MANIFESTS), 2)

    def test_daily_page_limit_accepts_existing_366_day_research_caller(self):
        qmt.validate_history_page(["A.SH"], "1d", "20251027000000", "20261026235959", 366)

    def test_http_repairs_share_pair_reservations_and_reuse_completed_ranges(self):
        with patch.object(qmt, "download_history_range") as download:
            for _ in range(2):
                qmt.download_history_page(["A.SH"], "1d", "20260101000000", "20261026150000")
        download.assert_called_once_with(["A.SH"], "1d", "20260101000000", "20261026150000")
        qmt._BACKFILL_RUNNING.add(("A.SH", "1d"))
        with patch.object(qmt, "download_history_range") as download:
            with self.assertRaises(qmt.HTTPError):
                qmt.download_history_page(["A.SH"], "1d", "20260101000000", "20261027150000")
        download.assert_not_called()

    def test_partial_http_range_cannot_claim_a_full_background_window(self):
        with patch.object(qmt, "download_history_range"):
            qmt.download_history_page(["A.SH"], "1d", "20991230000000", "20991231150000")
        self.assertFalse(qmt.backfill_result_current("A.SH", "1d"))
        self.configure(self.client, {"A.SH": False})
        self.assertIn(("A.SH", "1d"), qmt._BACKFILL_PENDING)

    def test_warm_and_cold_symbols_do_not_force_a_shared_180_day_redownload(self):
        self.configure(self.client, {"A.SH": False, "B.SH": False})
        qmt._BACKFILL_RESULTS["B.SH:1d"] = {"status": "success", "start_time": "20000101000000", "end_time": (datetime.now(qmt._QMT_TIMEZONE) - timedelta(days=20)).strftime("%Y%m%d%H%M%S")}
        with patch.object(qmt, "trading_hours", return_value=False):
            batch = qmt.take_backfill_batch()
        self.assertEqual(batch, [("A.SH", "1d")])
        self.assertIn(("B.SH", "1d"), qmt._BACKFILL_PENDING)

    def test_failed_incremental_download_preserves_completed_window(self):
        self.configure(self.client, {"A.SH": True})
        original = {"status": "success", "start_time": "20000101000000", "end_time": (datetime.now(qmt._QMT_TIMEZONE) - timedelta(days=2)).strftime("%Y%m%d%H%M%S")}
        qmt._BACKFILL_RESULTS["A.SH:1m"] = original

        def fail(*args):
            qmt._BACKFILL_STOP.set()
            raise RuntimeError("temporary native failure")

        with patch.object(qmt, "trading_hours", return_value=True), patch.object(qmt, "schedule_incremental_backfill"), patch.object(qmt, "download_history_range", side_effect=fail):
            qmt.backfill_worker()
        result = qmt._BACKFILL_RESULTS["A.SH:1m"]
        self.assertEqual(result["status"], "success")
        self.assertEqual(result["end_time"], original["end_time"])
        self.assertIn("temporary native failure", result["last_error"])
        self.assertFalse(qmt._BACKFILL_RUNNING)

    def test_disjoint_cached_range_is_not_warm_or_merged_across_a_gap(self):
        previous = {"status": "success", "start_time": "20000101000000", "end_time": "20000102150000"}
        qmt._BACKFILL_RESULTS["A.SH:1d"] = previous
        self.assertFalse(qmt.has_backfill_window("A.SH", "1d"))
        result = qmt.completed_download_range(previous, "20260429000000", "20261026150000")
        self.assertEqual(result, {"start_time": "20260429000000", "end_time": "20261026150000"})
        qmt._BACKFILL_RESULTS["A.SH:1d"] = dict(status="success", **result)
        with patch.object(qmt, "download_history_range") as download:
            qmt.download_history_page(["A.SH"], "1d", "20260201000000", "20260202150000")
        download.assert_called_once()  # The never-downloaded gap is not falsely treated as cached.

    def test_running_items_cannot_be_reclaimed_from_pending(self):
        self.configure(self.client, {"A.SH": True})
        qmt._BACKFILL_RUNNING.add(("A.SH", "1m"))
        with patch.object(qmt, "trading_hours", return_value=True):
            self.assertEqual(qmt.take_backfill_batch(), [])
        self.assertIn(("A.SH", "1m"), qmt._BACKFILL_PENDING)
        self.assertIn(("A.SH", "1m"), qmt._BACKFILL_RUNNING)

    def test_failed_http_repair_releases_and_requeues_before_worker_claim(self):
        self.configure(self.client, {"A.SH": True})
        with patch.object(qmt, "download_history_range", side_effect=RuntimeError("native failure")):
            with self.assertRaisesRegex(RuntimeError, "native failure"):
                qmt.download_history_page(["A.SH"], "1m", "20261020000000", "20261026150000")
        self.assertFalse(qmt._BACKFILL_RUNNING)
        self.assertIn(("A.SH", "1m"), qmt._BACKFILL_PENDING)
        with patch.object(qmt, "trading_hours", return_value=True):
            self.assertEqual(qmt.take_backfill_batch(), [("A.SH", "1m")])
        self.assertEqual(qmt._BACKFILL_RUNNING, {("A.SH", "1m")})

    def test_expansion_does_not_promote_a_partial_cache_to_full_window(self):
        self.configure(self.client, {"A.SH": False})
        second = self.make_client("second")
        self.configure(second, {"B.SH": False})
        for stock in ("A.SH", "B.SH"):
            qmt._BACKFILL_RESULTS[stock + ":1d"] = {"status": "success", "start_time": "20991230000000", "end_time": "20991231150000"}
        qmt._announce_history(["A.SH", "B.SH"], "1d", "20991230000000", "20991231150000", False)
        self.client.handle_history_ack({"id": self.client.sent[-1]["id"]})
        count = len(qmt._HISTORY_MANIFESTS)
        self.configure(self.client, {"A.SH": False, "B.SH": False})
        self.assertEqual(len(qmt._HISTORY_MANIFESTS), count)
        self.assertFalse(qmt.has_backfill_window("B.SH", "1d"))

    def test_native_downloads_have_explicit_ranges_and_incremental_flag(self):
        download = Mock()
        with patch.dict(qmt.__dict__, {"download_history_data2": download}):
            qmt.download_history_range(["A.SH"], "1d", "20260101000000", "20261026150000")
        download.assert_called_once_with(["A.SH"], "1d", "20260101000000", "20261026150000", incrementally=True)

    def test_account_orders_and_deals_broadcast_to_all_authenticated_clients(self):
        second = self.make_client("second")  # No configure required for account-only clients.
        for kind in ("account_status", "order_update", "deal_update"):
            qmt._broadcast({"type": kind, "data": {"example": True}})
            self.assertEqual(self.client.sent[-1]["type"], kind)
            self.assertEqual(second.sent[-1]["type"], kind)

    def test_raw_http_subscription_mutations_cannot_bypass_shared_ownership(self):
        for handler_type in (qmt.SubscribeQuoteHandler, qmt.UnsubscribeQuoteHandler):
            handler = handler_type.__new__(handler_type)
            with self.assertRaises(qmt.HTTPError) as error:
                handler.post()
            self.assertEqual(error.exception.args[0], 409)
        self.ctx.subscribe_quote.assert_not_called()
        self.ctx.unsubscribe_quote.assert_not_called()

    def health(self, now=1000):
        handler = qmt.HealthHandler.__new__(qmt.HealthHandler)
        handler.request = types.SimpleNamespace(headers={"X-Token": qmt.TOKEN})
        handler.application = object()  # No ContextInfo or account access is available.
        handler.write = Mock()
        handler.prepare()
        with patch.object(qmt, "monotonic", return_value=now):
            handler.get()
        return json.loads(handler.write.call_args.args[0])

    def test_health_route_requires_header_token(self):
        with patch.object(qmt, "Application") as app:
            qmt.make_app()
        self.assertIn(("/api/sys/health", qmt.HealthHandler), app.call_args.args[0])
        handler = qmt.HealthHandler.__new__(qmt.HealthHandler)
        for headers in ({}, {"X-Token": "wrong"}):
            handler.request = types.SimpleNamespace(headers=headers)
            with self.assertRaises(qmt.HTTPError) as error:
                handler.prepare()
            self.assertEqual(error.exception.args[0], 401)
        self.health()  # A valid token is accepted.

    def test_health_does_not_wait_for_backfill_lock(self):
        class NeverAcquire:
            def __enter__(self):
                raise AssertionError("health must not acquire the backfill lock")

            def __exit__(self, *args):
                pass

        with patch.object(qmt, "_BACKFILL_LOCK", NeverAcquire()):
            payload = self.health()
        self.assertEqual(payload["backfill"], {"pending": 0, "running": 0})

    def test_health_empty_and_unknown_facts_are_not_inferred(self):
        qmt._WS_CLIENTS.clear()
        payload = self.health()
        self.assertEqual(payload, {
            "bridge_id": qmt._BRIDGE_ID,
            "started_at": qmt._STARTED_AT,
            "ioloop_lag_ms": {"latest": None, "max_60s": None},
            "realtime": {"last_push_at": None, "pushed_symbol_count": 0, "symbol_count": 0, "subscription_count": 0},
            "universe_size": 0,
            "backfill": {"pending": 0, "running": 0},
            "clients": [],
        })
        self.assertIsNotNone(datetime.fromisoformat(payload["started_at"]).utcoffset())
        self.assertNotIn(qmt.TOKEN, json.dumps(payload))

    def test_health_lists_unconfigured_connected_and_grace_without_expiring(self):
        self.configure(self.client, {"A.SH": True, "B.SH": False})
        unconfigured = self.make_client("unconfigured")
        with patch.object(qmt, "monotonic", return_value=100):
            self.client.on_close()
        expiry = qmt._UNIVERSE_CLIENTS["first"]["expires_at"]
        qmt._BACKFILL_PENDING.remove(("A.SH", "1m"))
        qmt._BACKFILL_RUNNING.add(("A.SH", "1m"))
        before = dict(qmt._UNIVERSE_CLIENTS["first"])
        with patch.object(qmt, "_APP", object()), patch.object(qmt, "expire_universe_clients", side_effect=AssertionError), patch.object(qmt, "refresh_global_subscriptions", side_effect=AssertionError):
            payload = self.health(now=1000)  # Even overdue grace is only observed, never mutated.
        self.assertEqual(payload["clients"], [
            {"client_id": "first", "addr": "10.0.0.1", "state": "grace", "expires_at": expiry},
            {"client_id": "unconfigured", "addr": unconfigured.request.remote_ip, "state": "connected", "expires_at": None},
        ])
        self.assertEqual(qmt._UNIVERSE_CLIENTS["first"], before)
        self.assertEqual(payload["universe_size"], 2)
        self.assertEqual(payload["realtime"]["symbol_count"], 1)
        self.assertEqual(payload["realtime"]["subscription_count"], 2)  # 1m + 5m native handles.
        self.assertEqual(payload["backfill"], {"pending": 4, "running": 1})
        self.assertIsNotNone(datetime.fromisoformat(expiry).utcoffset())
        self.ctx.get_market_data_ex.assert_not_called()

    def test_health_reconnect_discards_old_grace_address_and_expiry(self):
        self.configure(self.client, {"A.SH": True})
        self.client.on_close()
        replacement = self.make_client("first")
        replacement.request.remote_ip = "10.0.0.9"
        self.configure(replacement, {"A.SH": True})
        self.assertEqual(self.health()["clients"], [
            {"client_id": "first", "addr": "10.0.0.9", "state": "connected", "expires_at": None},
        ])

    def test_ioloop_lag_sample_uses_monotonic_and_retains_only_60_seconds(self):
        qmt._IOLOOP_LAST_TICK = 100
        with patch.object(qmt, "monotonic", side_effect=[101, 104.5, 105, 165.1]):
            qmt.sample_ioloop_lag()
            self.assertEqual(qmt._IOLOOP_LAG_SAMPLES[-1][1], 0)
            qmt.sample_ioloop_lag()
            self.assertEqual(qmt._IOLOOP_LAG_SAMPLES[-1][1], 2500)
            qmt.sample_ioloop_lag()
            self.assertEqual(qmt._IOLOOP_LAG_SAMPLES[-1][1], -500)
            self.assertEqual(self.health(now=105)["ioloop_lag_ms"], {"latest": -500, "max_60s": 2500})
            qmt.sample_ioloop_lag()
        self.assertEqual(len(qmt._IOLOOP_LAG_SAMPLES), 1)
        self.assertAlmostEqual(qmt._IOLOOP_LAG_SAMPLES[-1][1], 59100)

    def test_ioloop_lag_uses_actual_intervals_and_preserves_negative_observations(self):
        qmt._IOLOOP_LAST_TICK = 0
        with patch.object(qmt, "monotonic", side_effect=[1.2, 2.3, 5.8, 6.3]):
            for _ in range(4):
                qmt.sample_ioloop_lag()
            lags = [sample[1] for sample in qmt._IOLOOP_LAG_SAMPLES]
        self.assertAlmostEqual(lags[0], 200)
        self.assertAlmostEqual(lags[1], 100)
        self.assertAlmostEqual(lags[2], 2500)
        self.assertAlmostEqual(lags[3], -500)
        self.assertEqual(qmt._IOLOOP_LAST_TICK, 6.3)

    def test_health_rolling_max_filters_old_samples_without_mutating_them(self):
        qmt._IOLOOP_LAG_SAMPLES.extend([(39, 9000), (40, 500), (99, 1)])
        self.assertEqual(self.health(now=100)["ioloop_lag_ms"], {"latest": 1, "max_60s": 500})
        self.assertEqual(self.health(now=160)["ioloop_lag_ms"], {"latest": 1, "max_60s": None})
        self.assertEqual(len(qmt._IOLOOP_LAG_SAMPLES), 3)

    def test_realtime_push_facts_deduplicate_periods_and_overlapping_clients(self):
        self.configure(self.client, {"A.SH": True, "B.SH": True, "C.SH": False})
        second = self.make_client("second")
        self.configure(second, {"B.SH": True})
        self.client.send_json = Mock(return_value=True)
        second.send_json = Mock(return_value=True)
        with patch.object(qmt, "monotonic", return_value=1000):
            qmt.push_global_market_data()
        payload = self.health()
        self.assertEqual(payload["realtime"]["pushed_symbol_count"], 2)
        self.assertEqual(payload["realtime"]["symbol_count"], 2)
        self.assertEqual(payload["realtime"]["subscription_count"], 4)
        self.assertEqual(self.client.send_json.call_count, 2)
        self.assertIsNotNone(datetime.fromisoformat(payload["realtime"]["last_push_at"]).utcoffset())
        before = payload["realtime"]
        self.ctx.get_market_data_ex.return_value = None
        self.ctx.get_market_data_ex.side_effect = None
        qmt.push_global_market_data()
        self.assertEqual(self.health()["realtime"], before)

    def test_no_realtime_push_facts_for_failed_empty_or_absent_delivery(self):
        self.configure(self.client, {"A.SH": True})
        self.client.send_json = Mock(return_value=False)
        qmt.push_global_market_data()
        self.assertIsNone(self.health()["realtime"]["last_push_at"])
        self.assertEqual(self.health()["realtime"]["pushed_symbol_count"], 0)
        self.client.send_json.assert_called()
        self.client.send_json.reset_mock()
        self.client.send_json.return_value = True
        self.ctx.get_market_data_ex.side_effect = None
        self.ctx.get_market_data_ex.return_value = {}
        qmt.push_global_market_data()
        self.client.send_json.assert_not_called()
        self.ctx.get_market_data_ex.return_value = {"A.SH": {"close": {"example": 10}}}
        self.client.on_close()
        qmt.push_global_market_data()
        self.assertIsNone(self.health()["realtime"]["last_push_at"])
        self.assertEqual(self.health()["realtime"]["pushed_symbol_count"], 0)

    def test_websocket_send_reports_only_queued_writes(self):
        client = qmt.MarketWebSocketHandler.__new__(qmt.MarketWebSocketHandler)
        client.ws_connection = None
        self.assertFalse(client.send_json({"type": "bar"}))
        client.ws_connection = object()
        future = Mock()
        client.write_message = Mock(return_value=future)
        self.assertTrue(client.send_json({"type": "bar"}))
        future.add_done_callback.assert_called_once_with(client._consume_write_result)
        client.write_message.side_effect = RuntimeError("closed")
        self.assertFalse(client.send_json({"type": "bar"}))

    def test_health_sampler_stops_with_server(self):
        callbacks = [Mock() for _ in range(3)]
        with patch.object(qmt, "_REALTIME_PUSHER", callbacks[0]), patch.object(qmt, "_ACCOUNT_PUSHER", callbacks[1]), patch.object(qmt, "_HEALTH_SAMPLER", callbacks[2]), patch.object(qmt, "IOLoop") as loop:
            qmt._stop_server_on_ioloop("test")
        for callback in callbacks:
            callback.stop.assert_called_once()
        loop.current.return_value.stop.assert_called_once()

    def test_health_sampler_lifecycle_on_loop_exit_start_failure_and_reinit(self):
        for failure in (None, RuntimeError("loop failed")):
            with self.subTest(failure=failure):
                old_sampler = Mock()
                callbacks = []

                def periodic(function, interval):
                    callback = Mock(function=function, interval=interval)
                    callbacks.append(callback)
                    return callback

                with patch.object(qmt, "_IOLOOP", None), patch.object(qmt, "_SERVER", None), patch.object(qmt, "_REALTIME_PUSHER", None), patch.object(qmt, "_ACCOUNT_PUSHER", None), patch.object(qmt, "_HEALTH_SAMPLER", old_sampler), patch.object(qmt, "IOLoop") as loop, patch.object(qmt, "HTTPServer"), patch.object(qmt, "start_backfill_worker"), patch.object(qmt, "PeriodicCallback", side_effect=periodic):
                    loop.return_value.start.side_effect = failure
                    qmt.init(Mock())
                    old_sampler.stop.assert_called_once()
                    self.assertEqual(callbacks[0].function, qmt.sample_ioloop_lag)
                    self.assertEqual(callbacks[0].interval, 1000)
                    for callback in callbacks:
                        callback.start.assert_called_once()
                        callback.stop.assert_called_once()
                    loop.return_value.close.assert_called_once_with(all_fds=True)
                    self.assertIsNone(qmt._HEALTH_SAMPLER)
                    self.assertIsNone(qmt._IOLOOP)

    def test_health_sampler_stops_if_later_callback_start_fails(self):
        sampler, pusher = Mock(), Mock()
        pusher.start.side_effect = RuntimeError("callback start failed")
        with patch.object(qmt, "_IOLOOP", None), patch.object(qmt, "_SERVER", None), patch.object(qmt, "_HEALTH_SAMPLER", None), patch.object(qmt, "_REALTIME_PUSHER", None), patch.object(qmt, "_ACCOUNT_PUSHER", None), patch.object(qmt, "IOLoop"), patch.object(qmt, "HTTPServer"), patch.object(qmt, "start_backfill_worker"), patch.object(qmt, "PeriodicCallback", side_effect=[sampler, pusher]):
            qmt.init(Mock())
            sampler.stop.assert_called_once()
            pusher.stop.assert_called_once()
            self.assertIsNone(qmt._HEALTH_SAMPLER)

    def test_application_sends_websocket_pings(self):
        app = qmt.make_app()
        self.assertEqual(app.settings["websocket_ping_interval"], 30)
        self.assertEqual(app.settings["websocket_ping_timeout"], 30)


if __name__ == "__main__":
    unittest.main()
