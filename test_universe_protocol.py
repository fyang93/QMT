"""Offline checks; no QMT runtime, account, network or native Tornado required."""
import importlib.util
import json
import locale
import sys
import threading
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
class HTTPError(Exception):
    def __init__(self, status_code, log_message=None, *args):
        self.status_code = status_code
        self.log_message = log_message
        super().__init__(*args)


web.HTTPError = HTTPError
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
                     "_REALTIME_RELEASE_AT", "_DOWNLOADS", "_DOWNLOAD_JOBS", "_DOWNLOAD_QUEUE"):
            getattr(qmt, name).clear()
        qmt._WS_CLIENTS.clear()
        qmt._BROADCAST_QUEUE.clear()
        qmt._BROADCAST_SCHEDULED = False
        qmt._REALTIME_LAST_5M = 0
        qmt._IOLOOP_LAST_TICK = None
        qmt._IOLOOP_LAG_SAMPLES.clear()
        qmt._REALTIME_LAST_PUSH_AT = None
        qmt._REALTIME_PUSHED_SYMBOL_COUNT = 0
        qmt._DOWNLOAD_STOP.clear()
        qmt._DOWNLOAD_WAKE.clear()
        qmt._IOLOOP = types.SimpleNamespace(add_callback=lambda function, *args: function(*args))
        self.ctx = Mock()
        self.ctx.subscribe_quote.side_effect = range(1, 1000)
        self.ctx.get_market_data_ex.side_effect = lambda fields, stocks, *args: {stock: {"close": {"20261026093100": 10}} for stock in stocks}
        qmt._APP = types.SimpleNamespace(ContextInfo=self.ctx, accountID="offline")
        self.client = self.make_client("first")

    def handler(self, kind, body):
        handler = kind.__new__(kind)
        handler.request = types.SimpleNamespace(body=json.dumps(body).encode("utf-8"), headers={"X-Token": qmt.TOKEN})
        handler.application = qmt._APP
        handler.write = Mock()
        handler.set_status = Mock()
        return handler

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
        self.assertNotIn("queued", replacement.sent[0])
        self.assertEqual(len(qmt._UNIVERSE_CLIENTS), 1)


    def test_explicit_reference_removal_also_has_unsubscribe_grace(self):
        self.configure(self.client, {"A.SH": True})
        with patch.object(qmt, "monotonic", return_value=1000):
            self.configure(self.client, {"A.SH": False})
        self.ctx.unsubscribe_quote.assert_not_called()
        with patch.object(qmt, "monotonic", return_value=1120):
            qmt.push_global_market_data()
        self.assertEqual(self.ctx.unsubscribe_quote.call_count, 2)


    def test_local_history_reads_are_paged(self):
        qmt.validate_history_page(["A.SH"] * 5, "5m", "20261001000000", "20261007235959", -1)
        for stocks, period, start, end, count in ((["A.SH"] * 6, "1m", "", "", 1),
                (["A.SH"], "5m", "20261001000000", "20261009235959", -1),
                (["A.SH"], "1d", "", "", -1), (["A.SH"], "1d", "", "", 100000)):
            with self.assertRaises(qmt.HTTPError):
                qmt.validate_history_page(stocks, period, start, end, count)


    def test_daily_page_limit_accepts_existing_366_day_research_caller(self):
        qmt.validate_history_page(["A.SH"], "1d", "20251027000000", "20261026235959", 366)


    def test_raw_http_subscription_mutations_cannot_bypass_shared_ownership(self):
        for handler_type in (qmt.SubscribeQuoteHandler, qmt.UnsubscribeQuoteHandler):
            handler = handler_type.__new__(handler_type)
            with self.assertRaises(qmt.HTTPError) as error:
                handler.post()
            self.assertEqual(error.exception.status_code, 409)
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
            self.assertEqual(error.exception.status_code, 401)
        self.health()  # A valid token is accepted.


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

                with patch.object(qmt, "_IOLOOP", None), patch.object(qmt, "_SERVER", None), patch.object(qmt, "_REALTIME_PUSHER", None), patch.object(qmt, "_ACCOUNT_PUSHER", None), patch.object(qmt, "_HEALTH_SAMPLER", old_sampler), patch.object(qmt, "IOLoop") as loop, patch.object(qmt, "HTTPServer"), patch.object(qmt, "start_download_worker"), patch.object(qmt, "PeriodicCallback", side_effect=periodic):
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
        with patch.object(qmt, "_IOLOOP", None), patch.object(qmt, "_SERVER", None), patch.object(qmt, "_HEALTH_SAMPLER", None), patch.object(qmt, "_REALTIME_PUSHER", None), patch.object(qmt, "_ACCOUNT_PUSHER", None), patch.object(qmt, "IOLoop"), patch.object(qmt, "HTTPServer"), patch.object(qmt, "start_download_worker"), patch.object(qmt, "PeriodicCallback", side_effect=[sampler, pusher]):
            qmt.init(Mock())
            sampler.stop.assert_called_once()
            pusher.stop.assert_called_once()
            self.assertIsNone(qmt._HEALTH_SAMPLER)


    def test_application_sends_websocket_pings(self):
        app = qmt.make_app()
        self.assertEqual(app.settings["websocket_ping_interval"], 30)
        self.assertEqual(app.settings["websocket_ping_timeout"], 30)

    def download_body(self, client, symbols, period="1d", start="20260101000000", end="20260102150000"):
        return {"client_id": client, "symbols": symbols, "period": period,
                "start_time": start, "end_time": end}

    def test_configure_only_changes_realtime_scope_and_preserves_selected_streams(self):
        self.configure(self.client, {"600000.SH": True, "000001.SZ": False}, streams=["account", "deal"])
        self.client.handle_subscribe({"stocks": "000001.SZ"})
        self.client.handle_unsubscribe({"stocks": "000001.SZ"})
        self.assertEqual(self.client.streams, {"account", "deal"})
        self.assertFalse(qmt._DOWNLOADS)
        self.assertFalse(qmt._DOWNLOAD_JOBS)
        self.assertFalse(qmt._DOWNLOAD_QUEUE)

    def test_websocket_rejects_nonfinite_and_invalid_unicode_before_mutating_scope(self):
        for extra in ({"client_id": "\ud800"}, {"unused": float("nan")}):
            self.client.on_message(json.dumps({"action": "configure", "universe": [], **extra}))
            self.assertEqual(self.client.sent[-1]["type"], "error")
            self.assertFalse(qmt._UNIVERSE_CLIENTS)
        self.ctx.subscribe_quote.assert_not_called()

    def test_download_global_and_client_limits_do_not_block_other_clients_or_duplicates(self):
        with patch.object(qmt, "_DOWNLOAD_LIMIT", 3), patch.object(qmt, "_DOWNLOAD_CLIENT_LIMIT", 2):
            for symbol in ("600000.SH", "600001.SH"):
                self.handler(qmt.DownloadHandler, self.download_body("a", [symbol])).post()
            first_id = next(iter(qmt._DOWNLOADS))
            duplicate = self.handler(qmt.DownloadHandler, self.download_body("a", ["600000.SH"]))
            duplicate.post()
            self.assertEqual(json.loads(duplicate.write.call_args.args[0])["id"], first_id)
            with self.assertRaises(qmt.HTTPError) as caught:
                self.handler(qmt.DownloadHandler, self.download_body("a", ["600002.SH"])).post()
            self.assertEqual(caught.exception.status_code, 429)
            self.assertIn("client", caught.exception.log_message)
            self.handler(qmt.DownloadHandler, self.download_body("b", ["600002.SH"])).post()
            with self.assertRaises(qmt.HTTPError) as caught:
                self.handler(qmt.DownloadHandler, self.download_body("c", ["600003.SH"])).post()
            self.assertEqual(caught.exception.status_code, 429)
            self.assertIn("global", caught.exception.log_message)
            self.assertEqual(len(qmt._DOWNLOADS), 3)

    def test_completed_download_retention_evicts_jobs_and_queues_but_not_active_requests(self):
        active = self.handler(qmt.DownloadHandler, self.download_body("active", ["000001.SZ", "000002.SZ"]))
        active.post()
        active_id = json.loads(active.write.call_args.args[0])["id"]
        with patch.object(qmt, "_DOWNLOAD_COMPLETED_LIMIT", 2), patch.dict(qmt.__dict__, {"download_history_data2": Mock()}):
            for index in range(4):
                handler = self.handler(qmt.DownloadHandler, self.download_body("other", ["60000%s.SH" % index]))
                handler.post()
                request_id = json.loads(handler.write.call_args.args[0])["id"]
                qmt.run_download(qmt._DOWNLOADS[request_id]["jobs"][0])
            self.assertEqual(len(qmt._DOWNLOADS), 3)
            self.assertIn(active_id, qmt._DOWNLOADS)
            self.assertEqual(len(qmt._DOWNLOAD_JOBS), 4)
            self.assertEqual(list(qmt._DOWNLOAD_QUEUE), ["active"])
            self.assertEqual(len(qmt._DOWNLOAD_QUEUE["active"]), 1)
            for job in list(qmt._DOWNLOADS[active_id]["jobs"]):
                qmt.run_download(job)
            self.assertEqual(len(qmt._DOWNLOADS), 2)
            self.assertIn(active_id, qmt._DOWNLOADS)
            self.assertEqual(qmt._DOWNLOADS[active_id]["status"], "success")
            self.assertFalse(qmt._DOWNLOAD_QUEUE)

    def test_download_has_no_300_symbol_limit_and_grace_expiry_does_not_cancel_work(self):
        symbols = ["%06d.SH" % index for index in range(301)]
        self.assertEqual(len(qmt.validate_download(self.download_body("a", symbols))[1]), 301)
        self.configure(self.client, {"600000.SH": True})
        self.handler(qmt.DownloadHandler, self.download_body("first", ["600000.SH"])).post()
        job = qmt.take_download()
        health = self.handler(qmt.HealthHandler, {})
        health.get()
        client = next(row for row in json.loads(health.write.call_args.args[0])["clients"] if row["client_id"] == "first")
        self.assertEqual((client["state"], client["realtime"], client["subscription_count"], client["running"]), ("connected", 1, 2, 1))
        with patch.object(qmt, "monotonic", return_value=1000):
            self.client.on_close()
        with patch.object(qmt, "monotonic", return_value=1121):
            qmt.push_global_market_data()
        self.assertNotIn("first", qmt._UNIVERSE_CLIENTS)
        with patch.dict(qmt.__dict__, {"download_history_data2": Mock()}):
            qmt.run_download(job)
        self.assertEqual(next(iter(qmt._DOWNLOADS.values()))["status"], "success")

    def test_websocket_write_buffers_release_on_failure_and_reject_slow_connections(self):
        from concurrent.futures import Future
        client = self.client
        client.ws_connection = object()
        client.write_message = Mock(side_effect=RuntimeError("write failed"))
        send = qmt.MarketWebSocketHandler.send_json
        self.assertFalse(send(client, {"type": "bar"}))
        self.assertEqual(client.write_buffer_bytes, 0)
        pending = Future()
        client.write_message = Mock(return_value=pending)
        self.assertTrue(send(client, {"type": "bar", "data": {"close": float("nan")}}))
        self.assertGreater(client.write_buffer_bytes, 0)
        pending.set_exception(RuntimeError("socket closed"))
        self.assertEqual(client.write_buffer_bytes, 0)
        with patch.object(qmt, "_WRITE_LIMIT", 1):
            self.assertFalse(send(client, {"type": "bar"}))
        client.close.assert_called_once()

    def test_download_deduplicates_cross_clients_returns_correlated_status_and_lists(self):
        handler_a = self.handler(qmt.DownloadHandler, self.download_body("a", ["600000.SH", "000001.SZ"]))
        handler_a.get_argument = Mock(return_value="a")
        handler_a.post()
        payload_a = json.loads(handler_a.write.call_args.args[0])
        handler_b = self.handler(qmt.DownloadHandler, self.download_body("b", ["000001.SZ", "600000.SH"]))
        handler_b.set_status = Mock()
        handler_b.post()
        payload_b = json.loads(handler_b.write.call_args.args[0])
        self.assertNotEqual(payload_a["id"], payload_b["id"])
        self.assertIs(qmt._DOWNLOADS[payload_a["id"]]["jobs"][0], qmt._DOWNLOADS[payload_b["id"]]["jobs"][0])
        self.assertEqual(payload_a["status"], "queued")
        self.assertEqual(payload_a["client_id"], "a")
        self.assertEqual(payload_a["progress"], {"total": 2, "completed": 0, "failed": 0})
        listed = self.handler(qmt.DownloadHandler, {})
        listed.get_argument = Mock(side_effect=lambda key, default="": "a" if key == "client_id" else "")
        listed.get()
        self.assertEqual(json.loads(listed.write.call_args.args[0])["requests"][0]["id"], payload_a["id"])

    def test_download_queue_is_fair_and_one_native_typeerror_does_not_repeat(self):
        def submit(client, symbols):
            handler = self.handler(qmt.DownloadHandler, self.download_body(client, symbols))
            handler.post()
            return json.loads(handler.write.call_args.args[0])["id"]
        aid = submit("A", ["600000.SH", "600001.SH", "600002.SH"])
        bid = submit("B", ["000001.SZ"])
        cid = submit("C", ["600000.SH", "600002.SH"])
        events = []
        def native(symbols, period, start, end, **kwargs):
            self.assertFalse(kwargs["incrementally"])
            events.append(symbols[0])
            if symbols[0] == "600000.SH":
                raise TypeError("inside native implementation")
            return True
        with patch.dict(qmt.__dict__, {"download_history_data2": native}), patch.object(qmt._DOWNLOAD_WAKE, "wait", side_effect=lambda timeout: qmt._DOWNLOAD_STOP.set()):
            qmt.download_worker()
        self.assertEqual(events, ["600000.SH", "000001.SZ", "600002.SH", "600001.SH"])
        self.assertEqual(qmt._DOWNLOADS[aid]["status"], "failed")
        self.assertIn("inside native implementation", qmt._DOWNLOADS[aid]["error"])
        self.assertEqual(qmt._DOWNLOADS[bid]["status"], "success")
        self.assertEqual(qmt._DOWNLOADS[cid]["status"], "failed")
        self.assertTrue(qmt._DOWNLOADS[bid]["finished_at"])

    def test_download_validation_and_history_download_are_strict(self):
        for body in (
                self.download_body("a", ["bad***"]),
                self.download_body("a", ["600000.SH"], "x"),
                self.download_body("a", ["600000.SH"], end="20991231150000")):
            with self.assertRaises(qmt.HTTPError):
                qmt.validate_download(body)
        handler = self.handler(qmt.HistoryDataHandler, {"stock_code": "600000.SH", "download": True})
        with self.assertRaises(qmt.HTTPError) as caught:
            handler.post()
        self.assertEqual(caught.exception.status_code, 400)
        self.ctx.get_market_data_ex.assert_not_called()

    def test_universe_rejects_invalid_and_duplicate_symbols_without_native_side_effect(self):
        for rows in (
            [{"symbol": "bad***", "subscribed": True}],
            [{"symbol": "600000.SH", "subscribed": True}, {"symbol": "600000.SH", "subscribed": False}],
        ):
            with self.assertRaises(ValueError):
                qmt.parse_universe(rows)
        self.ctx.subscribe_quote.assert_not_called()

    def test_broadcast_streams_isolation_and_bad_client_do_not_stop_delivery(self):
        self.client.streams = {"order"}
        self.client.send_json = Mock(side_effect=RuntimeError("closed"))
        second = self.make_client("second")
        second.streams = {"deal"}
        second.send_json = Mock()
        qmt._broadcast({"type": "order_update"}, "order")
        self.assertEqual(second.send_json.call_count, 0)
        qmt._broadcast({"type": "deal_update"}, "deal")
        second.send_json.assert_called_once()
        self.assertEqual(self.client.write_buffer_bytes, 0)

    def test_readonly_market_data_ex_does_not_implicitly_subscribe(self):
        handler = self.handler(qmt.MarketDataExHandler, {"fields": ["close"], "stock_code": ["600000.SH"]})
        self.ctx.get_market_data_ex.return_value = {"600000.SH": {"close": 10}}
        handler.post()
        self.assertEqual(self.ctx.get_market_data_ex.call_args.args[-2:], (False, False))

    def test_write_error_uses_tornado_log_message_without_exposing_server_errors(self):
        handler = qmt.BaseHandler.__new__(qmt.BaseHandler)
        handler._reason = "Bad Request"
        handler.finish = Mock()
        error = qmt.HTTPError(400, "specific detail")
        handler.write_error(400, exc_info=(None, error, None))
        self.assertEqual(json.loads(handler.finish.call_args.args[0])["error"], "specific detail")
        handler.write_error(500, exc_info=(None, RuntimeError("secret"), None))
        self.assertEqual(json.loads(handler.finish.call_args.args[0])["error"], "internal bridge error")

    def test_input_numeric_and_list_validation_rejects_nan_and_wrong_types(self):
        for value in (float("nan"), float("inf"), True, "oops"):
            with self.assertRaises(qmt.HTTPError):
                qmt.number(value, "price")
        with self.assertRaises(qmt.HTTPError):
            qmt.parse_list(["good", None])
        with self.assertRaises(qmt.HTTPError):
            qmt.validate_body({"client_id": "\ud800"})
