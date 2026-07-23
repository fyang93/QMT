# -*- coding: gbk -*-
import json
import locale
import re
from datetime import datetime, timedelta, timezone
from queue import Empty, Queue
from threading import Event, Lock, Thread
from time import monotonic
from tornado.web import Application, RequestHandler, HTTPError
from tornado.ioloop import IOLoop, PeriodicCallback
from tornado.httpserver import HTTPServer
from tornado.websocket import WebSocketHandler
import logging

# 自定义
ACCOUNT_ID = "YOUR_QMT_ACCOUNT_ID"
TOKEN = "YOUR_QMT_HTTP_TOKEN"
PORT = 10086

# 明确时间范围可修复本地缓存缺口；不要用空 start_time 触发“从最后一条开始”的隐式增量下载。
BACKFILL_PERIOD_DAYS = {"1d": 60, "5m": 180, "1m": 10}
_BACKGROUND_BACKFILL_PERIODS = ("1d", "5m")
INCREMENTAL_INTERVAL_SECONDS = 300
_QMT_FIELDS = ("open", "high", "low", "close", "volume", "amount")
_QMT_TIMEZONE = timezone(timedelta(hours=8))


# ===================================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
locale.setlocale(locale.LC_CTYPE, 'chinese')

_APP = None
_SERVER = None
_IOLOOP = None
_STOPPING = False
_WS_CLIENTS = set()
_BACKFILL_CONTEXT = None
_BACKFILL_PENDING = set()
_BACKFILL_RESULTS = {}
_BACKFILL_RUNNING = None
_BACKFILL_QUEUE = Queue()
_BACKFILL_LOCK = Lock()
_BACKFILL_STOP = Event()
_BACKFILL_THREAD = None
_BACKFILL_NEXT_INCREMENTAL = 0.0
_BACKFILL_LAST_INCREMENTAL_DATE = ""
_UNIVERSE_STATUS = {}
_BACKFILL_FACTOR_SENT = set()
_HISTORY_DOWNLOAD_BATCH_SIZE = 300
_HISTORY_MANIFESTS = []
_HISTORY_SEQUENCE = 0


def safe_call(func, *args, **kwargs):
    try:
        return func(*args, **kwargs)
    except Exception as e:
        logger.error(f"{func.__name__} 调用失败: {e}")
        return None


def parse_bool(value, default=False):
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ("1", "true", "yes", "y", "on")


def parse_list(value):
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        return [str(v).strip() for v in value if str(v).strip()]
    return [v.strip() for v in str(value).split(",") if v.strip()]


def to_jsonable(value):
    if value is None:
        return None
    if hasattr(value, "to_dict"):
        return value.to_dict()
    if isinstance(value, dict):
        result = {}
        for k, v in value.items():
            result[str(k)] = to_jsonable(v)
        return result
    if isinstance(value, (list, tuple)):
        return [to_jsonable(v) for v in value]
    return value


def backfill_range(period, start_time="", end_time=""):
    now = datetime.now(_QMT_TIMEZONE)
    days = BACKFILL_PERIOD_DAYS.get(period)
    if days is None:
        raise ValueError("background backfill only supports 1d, 5m, and 1m")
    return (
        start_time or (now - timedelta(days=days)).strftime("%Y%m%d%H%M%S"),
        end_time or now.strftime("%Y%m%d%H%M%S"),
    )


def download_history_range(stocks, period, start_time, end_time):
    stocks = parse_list(stocks)
    downloader = globals().get("download_history_data2")
    if downloader is not None:
        try:
            downloader(stocks, period, start_time, end_time, incrementally=True)
        except TypeError:
            downloader(stocks, period, start_time, end_time)
        return
    downloader = globals().get("download_history_data")
    if downloader is None:
        raise RuntimeError("download_history_data2/download_history_data not found")
    for stock in stocks:
        try:
            downloader(stock, period, start_time, end_time, incrementally=True)
        except TypeError:
            downloader(stock, period, start_time, end_time)


def download_history_incremental(stocks, period):
    stocks = parse_list(stocks)
    downloader = globals().get("download_history_data2")
    if downloader is not None:
        try:
            downloader(stocks, period, "", "", incrementally=True)
        except TypeError:
            downloader(stocks, period, "", "")
        return
    downloader = globals().get("download_history_data")
    if downloader is None:
        raise RuntimeError("download_history_data2/download_history_data not found")
    for stock in stocks:
        try:
            downloader(stock, period, "", "", incrementally=True)
        except TypeError:
            downloader(stock, period, "", "")


def _queue_broadcast(payload, *, history=False, factors=False):
    loop = _IOLOOP
    if loop is None:
        return
    loop.add_callback(_broadcast, payload, history, factors)


def _broadcast(payload, history=False, factors=False):
    for client in list(_WS_CLIENTS):
        if history and not getattr(client, "wants_history", False):
            continue
        if factors and not getattr(client, "wants_factors", False):
            continue
        client.send_json(payload)


def _announce_history(stocks, period, start_time, end_time, incremental):
    global _HISTORY_SEQUENCE
    today = datetime.now(_QMT_TIMEZONE).strftime("%Y%m%d")
    factor_batch = {}
    for stock in stocks:
        factor_key = stock + ":" + today
        if factor_key in _BACKFILL_FACTOR_SENT:
            continue
        factors = safe_call(_BACKFILL_CONTEXT.get_divid_factors, stock)
        if factors:
            factor_batch[stock] = to_jsonable(factors)
        _BACKFILL_FACTOR_SENT.add(factor_key)
    if factor_batch:
        _queue_broadcast({"type": "factors", "data": factor_batch}, factors=True)
    with _BACKFILL_LOCK:
        _HISTORY_SEQUENCE += 1
        manifest = {
            "type": "history_ready",
            "id": str(_HISTORY_SEQUENCE),
            "period": period,
            "symbols": stocks,
            "start_time": start_time,
            "end_time": end_time,
            "incremental": incremental,
            "count": 1 if incremental else -1,
        }
        if incremental:
            _HISTORY_MANIFESTS[:] = [
                item for item in _HISTORY_MANIFESTS
                if not (item.get("incremental") and item.get("period") == period and item.get("symbols") == stocks)
            ]
        _HISTORY_MANIFESTS.append(manifest)
    _queue_broadcast(manifest, history=True)


def history_coverage(ctx, stock, period, start_time, end_time):
    data = ctx.get_market_data_ex(
        ["close"], [stock], period, start_time, end_time, -1, "none", False, False
    )
    frame = data.get(stock) if isinstance(data, dict) else None
    if frame is None or len(frame) == 0:
        raise RuntimeError("download completed but no local bars found")
    index = list(frame.index)
    return {"bars": len(frame), "first": str(index[0]), "last": str(index[-1])}


def set_universe_targets(rows, refresh=False):
    desired = {}
    for row in rows if isinstance(rows, (list, tuple)) else []:
        if isinstance(row, str):
            symbol, status = row, "inactive"
        else:
            symbol, status = row.get("symbol", ""), row.get("status", "inactive")
        symbol = str(symbol).upper().strip()
        if symbol:
            desired[symbol] = str(status).lower().strip() or "inactive"
    queued = 0
    with _BACKFILL_LOCK:
        previous = dict(_UNIVERSE_STATUS)
        _UNIVERSE_STATUS.clear()
        _UNIVERSE_STATUS.update(desired)
        added = set(desired) if refresh else set(desired) - set(previous)
        promoted = {stock for stock, status in desired.items() if status in {"active", "exit_pending"} and previous.get(stock) not in {"active", "exit_pending"}}
        for stock in sorted(added | promoted, key=lambda symbol: (desired[symbol] not in {"active", "exit_pending"}, symbol)):
            periods = (_BACKGROUND_BACKFILL_PERIODS if stock in added else ()) + (("1m",) if desired[stock] in {"active", "exit_pending"} else ())
            for period in periods:
                item = (stock, period)
                if item not in _BACKFILL_PENDING and item != _BACKFILL_RUNNING:
                    _BACKFILL_PENDING.add(item)
                    _BACKFILL_QUEUE.put(item)
                    queued += 1
    return queued


def set_backfill_targets(stocks, refresh=False):
    return set_universe_targets([{"symbol": stock} for stock in parse_list(stocks)], refresh)


def schedule_incremental_backfill():
    global _BACKFILL_NEXT_INCREMENTAL, _BACKFILL_LAST_INCREMENTAL_DATE
    now = datetime.now(_QMT_TIMEZONE)
    today = now.strftime("%Y%m%d")
    # ponytail: the close-time repair covers missed bars without competing with live subscriptions.
    if now.weekday() >= 5 or now.strftime("%H%M") < "1510":
        return
    with _BACKFILL_LOCK:
        if _BACKFILL_LAST_INCREMENTAL_DATE == today or _BACKFILL_PENDING or _BACKFILL_RUNNING or monotonic() < _BACKFILL_NEXT_INCREMENTAL:
            return
        missing = [
            (stock, period)
            for stock in sorted(_UNIVERSE_STATUS)
            for period in _BACKGROUND_BACKFILL_PERIODS + (("1m",) if _UNIVERSE_STATUS[stock] in {"active", "exit_pending"} else ())
            if (_BACKFILL_RESULTS.get(stock + ":" + period) or {}).get("status") != "success"
            or (_BACKFILL_RESULTS.get(stock + ":" + period) or {}).get("trade_date") != today
        ]
        if not missing:
            _BACKFILL_LAST_INCREMENTAL_DATE = today
            return
        for item in missing:
            _BACKFILL_PENDING.add(item)
            _BACKFILL_QUEUE.put(item)
        _BACKFILL_NEXT_INCREMENTAL = monotonic() + INCREMENTAL_INTERVAL_SECONDS
        queued = len(_BACKFILL_PENDING)
    if queued:
        logger.info("QMT intraday incremental backfill queued: tasks=%s", queued)


def backfill_worker():
    global _BACKFILL_RUNNING
    while not _BACKFILL_STOP.is_set():
        schedule_incremental_backfill()
        try:
            item = _BACKFILL_QUEUE.get(timeout=0.5)
        except Empty:
            continue
        stock, period = item
        with _BACKFILL_LOCK:
            if stock not in _UNIVERSE_STATUS or item not in _BACKFILL_PENDING or (period == "1m" and _UNIVERSE_STATUS[stock] not in {"active", "exit_pending"}):
                _BACKFILL_PENDING.discard(item)
                _BACKFILL_QUEUE.task_done()
                continue
            # ponytail: warm daily and active 1m data before the full-universe 5m archive.
            priority = min(_BACKFILL_PENDING, key=lambda candidate: ({"1d": 0, "1m": 1, "5m": 2}[candidate[1]], candidate[0]))
            if item != priority:
                _BACKFILL_QUEUE.put(item)
                stock, period = priority
            batch = [(stock, period)]
            for candidate in sorted(
                _BACKFILL_PENDING,
                key=lambda value: (_UNIVERSE_STATUS.get(value[0]) not in {"active", "exit_pending"}, value[0]),
            ):
                if candidate != (stock, period) and candidate[1] == period:
                    batch.append(candidate)
                    if len(batch) >= _HISTORY_DOWNLOAD_BATCH_SIZE:
                        break
            for candidate in batch:
                _BACKFILL_PENDING.discard(candidate)
            _BACKFILL_RUNNING = (period, len(batch))
        stocks = sorted({candidate[0] for candidate in batch})
        try:
            start_time, end_time = backfill_range(period)
            incremental = all((_BACKFILL_RESULTS.get(stock + ":" + period) or {}).get("status") == "success" for stock in stocks)
            logger.info(
                "QMT history backfill started: %s symbols=%s mode=%s range=%s..%s",
                period, len(stocks), "incremental" if incremental else "bounded", start_time, end_time,
            )
            if incremental:
                download_history_incremental(stocks, period)
            else:
                download_history_range(stocks, period, start_time, end_time)
            _announce_history(stocks, period, start_time, end_time, incremental)
            result = {"status": "success", "period": period, "trade_date": datetime.now(_QMT_TIMEZONE).strftime("%Y%m%d"), "mode": "incremental" if incremental else "bounded_incremental", "bars": None}
            logger.info("QMT history backfill complete: %s symbols=%s mode=%s", period, len(stocks), result["mode"])
            results = {stock + ":" + period: result for stock in stocks}
        except Exception as e:
            logger.exception("QMT history backfill failed: %s symbols=%s", period, len(stocks))
            results = {stock + ":" + period: {"status": "failed", "period": period, "error": str(e)} for stock in stocks}
        with _BACKFILL_LOCK:
            _BACKFILL_RESULTS.update(results)
            _BACKFILL_RUNNING = None
        _BACKFILL_QUEUE.task_done()


def start_backfill_worker(ctx):
    global _BACKFILL_CONTEXT, _BACKFILL_THREAD
    _BACKFILL_CONTEXT = ctx
    if _BACKFILL_THREAD and _BACKFILL_THREAD.is_alive():
        return
    _BACKFILL_STOP.clear()
    _BACKFILL_THREAD = Thread(target=backfill_worker, name="qmt-history-backfill", daemon=True)
    _BACKFILL_THREAD.start()


def account_status_payload():
    positions = []
    for position in safe_call(get_trade_detail_data, ACCOUNT_ID, "stock", "position", "qmt") or []:
        try:
            symbol = position.m_strInstrumentID + "." + position.m_strExchangeID
            positions.append({
                "StockCode": symbol,
                "Volume": position.m_nVolume,
                "OpenPrice": position.m_dOpenPrice,
                "FloatProfit": position.m_dFloatProfit,
                "LastPrice": position.m_dLastPrice,
                "MarketValue": position.m_dMarketValue,
                "CanUseVolume": position.m_nCanUseVolume,
                "YesterdayVolume": position.m_nYesterdayVolume,
            })
        except Exception:
            logger.exception("QMT position serialization failed")
    account_rows = safe_call(get_trade_detail_data, ACCOUNT_ID, "stock", "account", "qmt") or []
    account = account_rows[0] if account_rows else None
    if account is None:
        return None
    return {
        "account": ACCOUNT_ID,
        "account_type": "stock",
        "total_money": float(account.m_dBalance),
        "available_money": float(account.m_dAvailable),
        "positions": positions,
    }


# ============= BaseHandler =============

AUTH_EXEMPT = set()


def no_auth(cls):
    AUTH_EXEMPT.add(cls)
    return cls


class BaseHandler(RequestHandler):
    def prepare(self):
        if self.__class__ not in AUTH_EXEMPT:
            token = self.request.headers.get('X-Token')
            if token != TOKEN:
                raise HTTPError(401, "认证失败：token 无效或缺失")

    def set_default_headers(self):
        self.set_header("Content-Type", "application/json; charset=utf-8")

    def write_error(self, status_code, **kwargs):
        self.finish(json.dumps({
            "error": self._reason,
            "status_code": status_code
        }, ensure_ascii=False))

    def json_body(self):
        raw = self.request.body or b"{}"
        if not raw:
            return {}
        try:
            text = raw.decode("utf-8")
        except Exception:
            text = raw.decode("gbk", "ignore")
        text = text.strip()
        if not text:
            return {}

        try:
            data = json.loads(text)
        except Exception:
            fixed = text
            fixed = re.sub(r'([{,]\s*)([A-Za-z_][A-Za-z0-9_]*)\s*:', r'\1"\2":', fixed)
            if "'" in fixed and '"' not in fixed:
                fixed = fixed.replace("'", '"')
            try:
                data = json.loads(fixed)
            except Exception:
                args = {}
                for key in self.request.arguments:
                    values = self.get_arguments(key)
                    args[key] = values[-1] if values else ""
                if args:
                    return args
                logger.warning("JSON body parse failed: %s", text[:300])
                raise HTTPError(400, "请求体必须是合法JSON，例如 {\"stock_code\":\"600000.SH\"}")

        if data is None:
            return {}
        if not isinstance(data, dict):
            raise HTTPError(400, "请求体JSON必须是对象")
        return data

    def ctx(self):
        return self.application.ContextInfo

    def acc(self):
        return self.application.accountID


class MarketWebSocketHandler(WebSocketHandler):
    def initialize(self):
        self.subscriptions = {}
        self.universe = {}
        self.wants_history = False
        self.wants_factors = False
        self.wants_account = False
        self.push_interval_ms = 1000
        self.pusher = None
        self.account_pusher = None

    def check_origin(self, origin):
        return True

    def open(self):
        token = self.get_argument("token", None) or self.request.headers.get("X-Token")
        if token != TOKEN:
            self.close(code=4001, reason="认证失败：token 无效或缺失")
            return
        _WS_CLIENTS.add(self)
        self.pusher = PeriodicCallback(self.push_market_data, self.push_interval_ms)
        self.pusher.start()
        self.send_json({"type": "connected", "message": "QMT websocket connected"})

    def on_close(self):
        _WS_CLIENTS.discard(self)
        if self.pusher:
            self.pusher.stop()
            self.pusher = None
        if self.account_pusher:
            self.account_pusher.stop()
            self.account_pusher = None
        self.unsubscribe_all()

    def on_message(self, message):
        try:
            data = json.loads(message)
        except Exception:
            fixed = re.sub(r'([{,]\s*)([A-Za-z_][A-Za-z0-9_]*)\s*:', r'\1"\2":', message)
            if "'" in fixed and '"' not in fixed:
                fixed = fixed.replace("'", '"')
            try:
                data = json.loads(fixed)
            except Exception:
                self.send_json({"type": "error", "message": "message must be json object"})
                return

        action = data.get("action", "ping")
        if action == "ping":
            self.send_json({"type": "pong"})
        elif action in {"configure", "universe"}:
            self.handle_configure(data)
        elif action == "history_ack":
            self.handle_history_ack(data)
        elif action == "account_status":
            self.wants_account = True
            if not self.account_pusher:
                self.account_pusher = PeriodicCallback(self.push_account_status, 60000)
                self.account_pusher.start()
            self.push_account_status()
        elif action == "subscribe":
            self.handle_subscribe(data)
        elif action == "unsubscribe":
            self.handle_unsubscribe(data)
        elif action == "subscriptions":
            self.send_json({"type": "subscriptions", "subscriptions": self.subscriptions})
        else:
            self.send_json({"type": "error", "message": "unknown action: %s" % action})

    def handle_configure(self, data):
        rows = data.get("universe", data.get("symbols", []))
        if isinstance(rows, dict):
            rows = [{"symbol": symbol, "status": status} for symbol, status in rows.items()]
        self.universe = {
            str(row.get("symbol", "")).upper().strip(): str(row.get("status", "inactive")).lower().strip()
            for row in rows if isinstance(row, dict) and str(row.get("symbol", "")).strip()
        }
        self.wants_history = True
        self.wants_factors = True
        queued = set_universe_targets(
            [{"symbol": symbol, "status": status} for symbol, status in self.universe.items()],
            refresh=parse_bool(data.get("refresh_history")),
        )
        logger.info(
            "QMT universe configured: symbols=%s realtime=%s backfill_tasks=%s",
            len(self.universe), sum(status in {"active", "exit_pending"} for status in self.universe.values()), queued,
        )
        self.refresh_subscriptions()
        self.send_json({
            "type": "configured",
            "symbols": len(self.universe),
            "queued": queued,
            "realtime": sum(status in {"active", "exit_pending"} for status in self.universe.values()),
        })
        with _BACKFILL_LOCK:
            manifests = list(_HISTORY_MANIFESTS)
        for manifest in manifests:
            self.send_json(manifest)

    def handle_history_ack(self, data):
        manifest_id = str(data.get("id", ""))
        with _BACKFILL_LOCK:
            _HISTORY_MANIFESTS[:] = [item for item in _HISTORY_MANIFESTS if item.get("id") != manifest_id]
        self.send_json({"type": "history_acked", "id": manifest_id})

    def refresh_subscriptions(self):
        desired = {
            symbol for symbol, status in self.universe.items()
            if status in {"active", "exit_pending"}
        }
        current = {item.get("stock_code") for item in self.subscriptions.values()}
        for key, item in list(self.subscriptions.items()):
            if item.get("stock_code") not in desired:
                if item.get("sub_id") is not None:
                    safe_call(self.application.ContextInfo.unsubscribe_quote, int(item["sub_id"]))
                self.subscriptions.pop(key, None)
        current = {item.get("stock_code") for item in self.subscriptions.values()}
        for stock in sorted(desired - current):
            for period in ("1m", "5m"):
                sub_id = safe_call(self.application.ContextInfo.subscribe_quote, stock, period, "none")
                self.subscriptions[str(sub_id) if sub_id is not None else stock + ":" + period] = {
                    "stock_code": stock, "period": period, "qmt_period": period,
                    "dividend_type": "none", "fields": list(_QMT_FIELDS), "count": 1, "sub_id": sub_id,
                }

    def push_account_status(self):
        payload = account_status_payload()
        if payload is not None:
            self.send_json({"type": "account_status", "data": payload})

    def handle_subscribe(self, data):
        stock_list = parse_list(data.get("stock_code", data.get("stocks", data.get("stock", ""))))
        if not stock_list:
            self.send_json({"type": "error", "message": "need stock_code"})
            return

        period = str(data.get("period", "1m")).strip()
        qmt_period = "tick" if period in ("full_tick", "fulltick") else period
        dividend_type = data.get("dividend_type", "follow")
        fields = parse_list(data.get("fields", "open,high,low,close,volume,amount"))
        count = int(data.get("count", "1"))
        interval_ms = int(data.get("interval_ms", self.push_interval_ms))
        if interval_ms < 500:
            interval_ms = 500
        if interval_ms != self.push_interval_ms:
            self.push_interval_ms = interval_ms
            if self.pusher:
                self.pusher.stop()
                self.pusher = PeriodicCallback(self.push_market_data, self.push_interval_ms)
                self.pusher.start()

        results = []
        for stock in stock_list:
            sub_id = safe_call(self.application.ContextInfo.subscribe_quote, stock, qmt_period, dividend_type)
            self.subscriptions[str(sub_id) if sub_id is not None else stock] = {
                "stock_code": stock,
                "period": period,
                "qmt_period": qmt_period,
                "dividend_type": dividend_type,
                "fields": fields,
                "count": count,
                "sub_id": sub_id,
            }
            results.append({
                "stock_code": stock,
                "period": period,
                "sub_id": sub_id,
                "status": "success" if sub_id is not None else "failed",
            })

        self.send_json({"type": "subscribed", "results": results})
        self.push_market_data()

    def handle_unsubscribe(self, data):
        sub_ids = parse_list(data.get("sub_id", data.get("sub_ids", "")))
        stocks = set(parse_list(data.get("stock_code", data.get("stocks", ""))))

        removed = []
        for key, item in list(self.subscriptions.items()):
            sub_id = item.get("sub_id")
            stock_code = item.get("stock_code")
            if (sub_ids and str(sub_id) in sub_ids) or (stocks and stock_code in stocks):
                if sub_id is not None:
                    safe_call(self.application.ContextInfo.unsubscribe_quote, int(sub_id))
                removed.append(item)
                self.subscriptions.pop(key, None)

        self.send_json({"type": "unsubscribed", "removed": removed})

    def unsubscribe_all(self):
        for item in list(self.subscriptions.values()):
            sub_id = item.get("sub_id")
            if sub_id is not None:
                safe_call(self.application.ContextInfo.unsubscribe_quote, int(sub_id))
        self.subscriptions.clear()

    def push_market_data(self):
        if not self.subscriptions:
            return

        groups = {}
        for item in self.subscriptions.values():
            period = item.get("period", "1m")
            qmt_period = item.get("qmt_period", period)
            fields = tuple(item.get("fields") or [])
            dividend_type = item.get("dividend_type", "follow")
            count = int(item.get("count", 1))
            key = (period, qmt_period, fields, dividend_type, count)
            groups.setdefault(key, []).append(item.get("stock_code"))

        for key, stocks in groups.items():
            period, qmt_period, fields, dividend_type, count = key
            stocks = sorted(set(stocks))
            if qmt_period == "tick":
                data = safe_call(self.application.ContextInfo.get_full_tick, stocks)
                if data is None:
                    self.send_json({
                        "type": "error",
                        "period": period,
                        "message": "get_full_tick failed",
                        "stocks": stocks,
                    })
                    continue
                self.send_json({
                    "type": "tick",
                    "period": period,
                    "stocks": stocks,
                    "data": to_jsonable(data),
                })
                continue

            data = safe_call(
                self.application.ContextInfo.get_market_data_ex,
                list(fields),
                stocks,
                qmt_period,
                "",
                "",
                count,
                dividend_type,
            )
            if data is None:
                self.send_json({
                    "type": "error",
                    "period": period,
                    "message": "get_market_data_ex failed",
                    "stocks": stocks,
                })
                continue

            self.send_json({
                "type": "bar",
                "period": period,
                "stocks": stocks,
                "fields": list(fields),
                "count": count,
                "data": to_jsonable(data),
            })

    def send_json(self, data):
        if self.ws_connection is None:
            return
        try:
            future = self.write_message(json.dumps(data, ensure_ascii=False, default=str))
            if future is not None:
                future.add_done_callback(self._consume_write_result)
        except Exception as e:
            logger.debug("websocket write skipped: %s", e)

    @staticmethod
    def _consume_write_result(future):
        try:
            future.result()
        except Exception:
            pass


# ============= 1. ContextInfo 属性 =============
# ContextInfo.period - 获取当前周期
class ContextPeriodHandler(BaseHandler):
    def get(self):
        self.write(json.dumps({"period": self.ctx().period}, ensure_ascii=False))

# ContextInfo.barpos - 获取当前K线索引号
class ContextBarposHandler(BaseHandler):
    def get(self):
        self.write(json.dumps({"barpos": self.ctx().barpos}, ensure_ascii=False))

# ContextInfo.time_tick_size - 获取当前K线数目
class ContextTimeTickSizeHandler(BaseHandler):
    def get(self):
        self.write(json.dumps({"time_tick_size": self.ctx().time_tick_size}, ensure_ascii=False))

# ContextInfo.stockcode - 获取当前主图品种代码
class ContextStockCodeHandler(BaseHandler):
    def get(self):
        self.write(json.dumps({"stockcode": self.ctx().stockcode}, ensure_ascii=False))

# ContextInfo.dividend_type - 获取当前复权方式
class ContextDividendTypeHandler(BaseHandler):
    def get(self):
        self.write(json.dumps({"dividend_type": self.ctx().dividend_type}, ensure_ascii=False))

# ContextInfo.market - 获取当前主图市场
class ContextMarketHandler(BaseHandler):
    def get(self):
        self.write(json.dumps({"market": self.ctx().market}, ensure_ascii=False))

# ContextInfo.do_back_test - 是否开启回测模式
class ContextDoBackTestHandler(BaseHandler):
    def get(self):
        self.write(json.dumps({"do_back_test": self.ctx().do_back_test}, ensure_ascii=False))

# ContextInfo.benchmark - 获取回测基准
class ContextBenchmarkHandler(BaseHandler):
    def get(self):
        self.write(json.dumps({"benchmark": self.ctx().benchmark}, ensure_ascii=False))

# ContextInfo.capital - 获取回测初始资金
class ContextCapitalHandler(BaseHandler):
    def get(self):
        self.write(json.dumps({"capital": self.ctx().capital}, ensure_ascii=False))

# ContextInfo.get_universe() - 获取股票池中的股票
class ContextUniverseHandler(BaseHandler):
    def get(self):
        self.write(json.dumps({"universe": self.ctx().get_universe()}, ensure_ascii=False))


# ============= 2. 数据查询 (ContextInfo get_*) =============
# ContextInfo.get_stock_name() - 根据代码获取股票名称
class StockNameHandler(BaseHandler):
    def post(self):
        data = self.json_body()
        stockcode = data.get('stockcode', '')
        ret = safe_call(self.ctx().get_stock_name, stockcode)
        self.write(json.dumps({"stockcode": stockcode, "name": ret}, ensure_ascii=False))

# get_open_date() - 根据代码获取上市时间
class OpenDateHandler(BaseHandler):
    def post(self):
        data = self.json_body()
        stockcode = data.get('stockcode', '')
        ret = safe_call(get_open_date, stockcode)
        self.write(json.dumps({"stockcode": stockcode, "open_date": ret}, ensure_ascii=False))

# ContextInfo.get_last_volume() - 获取最新流通股本
class LastVolumeHandler(BaseHandler):
    def post(self):
        data = self.json_body()
        stockcode = data.get('stockcode', '')
        ret = safe_call(self.ctx().get_last_volume, stockcode)
        if ret is None:
            raise HTTPError(500, "获取流通股本失败")
        self.write(json.dumps({"stockcode": stockcode, "last_volume": ret}, ensure_ascii=False))

# ContextInfo.get_bar_timetag() - 获取K线时间戳
class BarTimetagHandler(BaseHandler):
    def post(self):
        data = self.json_body()
        index = int(data.get('index', -1))
        ret = safe_call(self.ctx().get_bar_timetag, index)
        self.write(json.dumps({"index": index, "timetag": ret}, ensure_ascii=False))

# ContextInfo.get_tick_timetag() - 获取最新分笔时间戳
class TickTimetagHandler(BaseHandler):
    def get(self):
        ret = safe_call(self.ctx().get_tick_timetag)
        self.write(json.dumps({"timetag": ret}, ensure_ascii=False))

# ContextInfo.get_sector() - 获取指数成份股
class SectorHandler(BaseHandler):
    def post(self):
        data = self.json_body()
        sector = data.get('sector', '')
        realtime = data.get('realtime', '0')
        if not sector:
            raise HTTPError(400, "need args sector")
        ret = safe_call(self.ctx().get_sector, sector, int(realtime) if realtime != '0' else 0)
        self.write(json.dumps({"sector": sector, "stocks": ret or []}, ensure_ascii=False))

# ContextInfo.get_industry() - 获取行业成份股
class IndustryHandler(BaseHandler):
    def post(self):
        data = self.json_body()
        industry = data.get('industry', '')
        if not industry:
            raise HTTPError(400, "need args industry")
        print(industry)
        ret = safe_call(self.ctx().get_industry, industry)
        self.write(json.dumps({"industry": industry, "stocks": ret or []}, ensure_ascii=False))

# ContextInfo.get_stock_list_in_sector() - 获取板块成份股
class StockListInSectorHandler(BaseHandler):
    def post(self):
        data = self.json_body()
        sectorname = data.get('sectorname', '')
        if not sectorname:
            raise HTTPError(400, "need args sectorname")
        ret = safe_call(self.ctx().get_stock_list_in_sector, sectorname)
        self.write(json.dumps({"sectorname": sectorname, "stocks": ret or []}, ensure_ascii=False))

# ContextInfo.get_weight_in_index() - 获取指数中权重
class WeightInIndexHandler(BaseHandler):
    def post(self):
        data = self.json_body()
        indexcode = data.get('indexcode', '')
        stockcode = data.get('stockcode', '')
        ret = safe_call(self.ctx().get_weight_in_index, indexcode, stockcode)
        self.write(json.dumps({"indexcode": indexcode, "stockcode": stockcode, "weight": ret}, ensure_ascii=False))

# ContextInfo.get_contract_multiplier() - 获取合约乘数
class ContractMultiplierHandler(BaseHandler):
    def post(self):
        data = self.json_body()
        contractcode = data.get('contractcode', '')
        ret = safe_call(self.ctx().get_contract_multiplier, contractcode)
        self.write(json.dumps({"contractcode": contractcode, "multiplier": ret}, ensure_ascii=False))

# ContextInfo.get_risk_free_rate() - 获取无风险利率
class RiskFreeRateHandler(BaseHandler):
    def post(self):
        data = self.json_body()
        index = int(data.get('index', '-1'))
        ret = safe_call(self.ctx().get_risk_free_rate, index)
        self.write(json.dumps({"index": index, "risk_free_rate": ret}, ensure_ascii=False))

# ContextInfo.get_date_location() - 获取日期对应的K线索引
class DateLocationHandler(BaseHandler):
    def post(self):
        data = self.json_body()
        strdate = data.get('strdate', '')
        ret = safe_call(self.ctx().get_date_location, strdate)
        self.write(json.dumps({"strdate": strdate, "location": ret}, ensure_ascii=False))

# 历史行情：同步按明确范围下载完成，再读取不填充、不订阅的本地数据。
class HistoryDataHandler(BaseHandler):
    def post(self):
        data = self.json_body()
        stock_code = data.get('stock_code', data.get('stocks', data.get('stock', '')))
        stock_list = parse_list(stock_code)
        if not stock_list:
            raise HTTPError(400, "need args stock_code")

        period = data.get('period', '1d')
        start_time = data.get('start_time', data.get('startTime', ''))
        end_time = data.get('end_time', data.get('endTime', ''))
        count = int(data.get('count', data.get('len', '-1')))
        dividend_type = data.get('dividend_type', data.get('dividendType', 'none'))
        fields = data.get('fields', data.get('field', 'open,high,low,close,volume'))
        fields_list = parse_list(fields)
        do_download = parse_bool(data.get('download', True), True)

        download_results = {}
        if do_download:
            if period not in BACKFILL_PERIOD_DAYS:
                raise HTTPError(400, "history download only supports 1d and 5m")
            start_time, end_time = backfill_range(period, start_time, end_time)
            try:
                download_history_range(stock_list, period, start_time, end_time)
                download_results = {stock: "success" for stock in stock_list}
            except Exception as e:
                logger.exception("QMT history download failed")
                raise HTTPError(502, "历史行情下载失败: %s" % e)

        try:
            ret = self.ctx().get_market_data_ex(
                fields_list, stock_list, period, start_time, end_time, count,
                dividend_type, False, False
            )
        except Exception as e:
            logger.exception("QMT history read failed")
            raise HTTPError(500, "获取历史行情失败: %s" % e)
        if ret is None:
            raise HTTPError(500, "获取历史行情失败")

        self.write(json.dumps({
            "data": to_jsonable(ret),
            "source": "get_market_data_ex",
            "download": download_results,
        }, ensure_ascii=False, default=str))


class BackfillTargetsHandler(BaseHandler):
    def put(self):
        self.update_targets()

    def post(self):
        self.update_targets()

    def update_targets(self):
        data = self.json_body()
        stocks = data.get('symbols', data.get('stock_code', data.get('stocks', '')))
        queued = set_backfill_targets(stocks, parse_bool(data.get('refresh', False)))
        self.set_status(202)
        self.write(json.dumps({
            "status": "accepted",
            "symbols": len(parse_list(stocks)),
            "queued": queued,
        }, ensure_ascii=False))


class BackfillStatusHandler(BaseHandler):
    def get(self):
        with _BACKFILL_LOCK:
            running = None
            if _BACKFILL_RUNNING:
                running = {"period": _BACKFILL_RUNNING[0], "symbols": _BACKFILL_RUNNING[1]}
            payload = {
                "targets": len(_UNIVERSE_STATUS),
                "pending": len(_BACKFILL_PENDING),
                "running": running,
                "results": dict(_BACKFILL_RESULTS),
            }
        self.write(json.dumps(payload, ensure_ascii=False))

# ContextInfo.get_market_data() - 获取行情数据(DataFrame)
class MarketDataHandler(BaseHandler):
    def post(self):
        data = self.json_body()
        fields = data.get('fields', '')
        stock_code = data.get('stock_code', '')
        start_time = data.get('start_time', '')
        end_time = data.get('end_time', '')
        period = data.get('period', '1d')
        dividend_type = data.get('dividend_type', 'none')
        count = int(data.get('count', '-1'))
        fields_list = [f.strip() for f in fields.split(',')] if fields else []
        stock_list = [s.strip() for s in stock_code.split(',')] if stock_code else []
        ret = safe_call(self.ctx().get_market_data, fields_list, stock_list, start_time, end_time, True, period, dividend_type, count)
        if ret is None:
            raise HTTPError(500, "获取行情数据失败")
        if hasattr(ret, 'to_dict'):
            ret = ret.to_dict()
        self.write(json.dumps({"data": ret}, ensure_ascii=False, default=str))

# ContextInfo.get_market_data_ex() - 获取扩展行情(Level2)
class MarketDataExHandler(BaseHandler):
    def post(self):
        data = self.json_body()
        fields = data.get('fields', '')
        stock_code = data.get('stock_code', '')
        period = data.get('period', 'follow')
        start_time = data.get('start_time', '')
        end_time = data.get('end_time', '')
        count = int(data.get('count', '-1'))
        dividend_type = data.get('dividend_type', 'follow')
        fields_list = [f.strip() for f in fields.split(',')] if fields else []
        stock_list = [s.strip() for s in stock_code.split(',')] if stock_code else []
        ret = safe_call(self.ctx().get_market_data_ex, fields_list, stock_list, period, start_time, end_time, count, dividend_type)
        if ret is None:
            raise HTTPError(500, "获取扩展行情失败")
        result = {}
        for k, v in ret.items():
            if hasattr(v, 'to_dict'):
                result[k] = v.to_dict()
            else:
                result[k] = str(v)
        self.write(json.dumps({"data": result}, ensure_ascii=False, default=str))

# ContextInfo.get_full_tick() - 获取分笔数据
class FullTickHandler(BaseHandler):
    def post(self):
        data = self.json_body()
        stocks = data.get('stocks', '')
        if not stocks:
            raise HTTPError(400, "need args stocks")
        code_list = [s.strip() for s in stocks.split(',')]
        ret = safe_call(self.ctx().get_full_tick, code_list)
        if not ret:
            raise HTTPError(500, "获取分笔行情失败")
        self.write(json.dumps(ret, ensure_ascii=False, default=str))

# ContextInfo.get_divid_factors() - 获取除权除息和复权因子
class DividFactorsHandler(BaseHandler):
    def post(self):
        data = self.json_body()
        stockcode = data.get('stockcode', '')
        ret = safe_call(self.ctx().get_divid_factors, stockcode)
        self.write(json.dumps({"stockcode": stockcode, "factors": ret or {}}, ensure_ascii=False))

# ContextInfo.get_main_contract() - 获取期货主力合约
class MainContractHandler(BaseHandler):
    def post(self):
        data = self.json_body()
        codemarket = data.get('codemarket', '')
        ret = safe_call(self.ctx().get_main_contract, codemarket)
        self.write(json.dumps({"codemarket": codemarket, "main_contract": ret}, ensure_ascii=False))

# timetag_to_datetime() - 毫秒时间戳转日期时间
class TimetagToDatetimeHandler(BaseHandler):
    def post(self):
        data = self.json_body()
        timetag = int(data.get('timetag', '0'))
        fmt = data.get('format', '%Y-%m-%d %H:%M:%S')
        ret = safe_call(timetag_to_datetime, timetag, fmt)
        self.write(json.dumps({"timetag": timetag, "datetime": ret}, ensure_ascii=False))

# ContextInfo.get_total_share() - 获取总股本
class TotalShareHandler(BaseHandler):
    def post(self):
        data = self.json_body()
        stockcode = data.get('stockcode', '')
        ret = safe_call(self.ctx().get_total_share, stockcode)
        self.write(json.dumps({"stockcode": stockcode, "total_share": ret}, ensure_ascii=False))

# ContextInfo.get_trading_dates() - 获取交易日列表
class TradingDatesHandler(BaseHandler):
    def post(self):
        data = self.json_body()
        stockcode = data.get('stockcode', '')
        start_date = data.get('start_date', '')
        end_date = data.get('end_date', '')
        count = data.get('count', '')
        period = data.get('period', '1d')
        count_int = int(count) if count else -1
        ret = safe_call(self.ctx().get_trading_dates, stockcode, start_date, end_date, count_int, period)
        self.write(json.dumps({"dates": ret or []}, ensure_ascii=False))

# ContextInfo.get_svol() - 获取内盘成交量
class SvolHandler(BaseHandler):
    def post(self):
        data = self.json_body()
        stockcode = data.get('stockcode', '')
        ret = safe_call(self.ctx().get_svol, stockcode)
        self.write(json.dumps({"stockcode": stockcode, "svol": ret}, ensure_ascii=False))

# ContextInfo.get_bvol() - 获取外盘成交量
class BvolHandler(BaseHandler):
    def post(self):
        data = self.json_body()
        stockcode = data.get('stockcode', '')
        ret = safe_call(self.ctx().get_bvol, stockcode)
        self.write(json.dumps({"stockcode": stockcode, "bvol": ret}, ensure_ascii=False))

# ContextInfo.get_longhubang() - 获取龙虎榜数据
class LonghubangHandler(BaseHandler):
    def post(self):
        data = self.json_body()
        stock_list = data.get('stock_list', '')
        startTime = data.get('startTime', '')
        endTime = data.get('endTime', '')
        slist = [s.strip() for s in stock_list.split(',')] if stock_list else []
        ret = safe_call(self.ctx().get_longhubang, slist, startTime, endTime)
        if hasattr(ret, 'to_dict'):
            ret = ret.to_dict()
        self.write(json.dumps({"data": ret} if ret else {"error": "获取龙虎榜数据失败"}, ensure_ascii=False, default=str))

# get_top10_share_holder() - 获取十大股东数据
class Top10ShareHolderHandler(BaseHandler):
    def post(self):
        data = self.json_body()
        stock_list = data.get('stock_list', '')
        data_name = data.get('data_name', 'holder')
        start_time = data.get('start_time', '')
        end_time = data.get('end_time', '')
        slist = [s.strip() for s in stock_list.split(',')] if stock_list else []
        ret = safe_call(get_top10_share_holder, slist, data_name, start_time, end_time)
        if hasattr(ret, 'to_dict'):
            ret = ret.to_dict()
        self.write(json.dumps({"data": ret} if ret else {"error": "获取十大股东数据失败"}, ensure_ascii=False, default=str))

# ContextInfo.get_option_detail_data() - 获取期权详细信息
class OptionDetailHandler(BaseHandler):
    def post(self):
        data = self.json_body()
        optioncode = data.get('optioncode', '')
        ret = safe_call(self.ctx().get_option_detail_data, optioncode)
        self.write(json.dumps({"optioncode": optioncode, "detail": ret or {}}, ensure_ascii=False))

# ContextInfo.get_turnover_rate() - 获取换手率
class TurnoverRateHandler(BaseHandler):
    def post(self):
        data = self.json_body()
        stock_list = data.get('stock_list', '')
        startTime = data.get('startTime', '')
        endTime = data.get('endTime', '')
        slist = [s.strip() for s in stock_list.split(',')] if stock_list else []
        ret = safe_call(self.ctx().get_turnover_rate, slist, startTime, endTime)
        if hasattr(ret, 'to_dict'):
            ret = ret.to_dict()
        self.write(json.dumps({"data": ret} if ret else {"error": "获取换手率失败"}, ensure_ascii=False, default=str))

# get_etf_info() - 获取ETF申赎清单及成分股
class EtfInfoHandler(BaseHandler):
    def post(self):
        data = self.json_body()
        stockcode = data.get('stockcode', '')
        ret = safe_call(get_etf_info, stockcode)
        self.write(json.dumps({"stockcode": stockcode, "info": ret or {}}, ensure_ascii=False, default=str))

# get_etf_iopv() - 获取ETF基金份额参考净值
class EtfIopvHandler(BaseHandler):
    def post(self):
        data = self.json_body()
        stockcode = data.get('stockcode', '')
        ret = safe_call(get_etf_iopv, stockcode)
        self.write(json.dumps({"stockcode": stockcode, "iopv": ret}, ensure_ascii=False))

# ContextInfo.get_instrumentdetail() - 获取合约详细信息
class InstrumentDetailHandler(BaseHandler):
    def post(self):
        data = self.json_body()
        stockcode = data.get('stockcode', '')
        ret = safe_call(self.ctx().get_instrumentdetail, stockcode)
        self.write(json.dumps({"stockcode": stockcode, "detail": ret or {}}, ensure_ascii=False, default=str))

# ContextInfo.get_contract_expire_date() - 获取期货合约到期日
class ContractExpireDateHandler(BaseHandler):
    def post(self):
        data = self.json_body()
        codemarket = data.get('codemarket', '')
        ret = safe_call(self.ctx().get_contract_expire_date, codemarket)
        self.write(json.dumps({"codemarket": codemarket, "expire_date": ret}, ensure_ascii=False))

# ContextInfo.get_option_undl_data() - 获取期权标的对应的期权品种列表
class OptionUndlDataHandler(BaseHandler):
    def post(self):
        data = self.json_body()
        undl_code_ref = data.get('undl_code_ref', '')
        ret = safe_call(self.ctx().get_option_undl_data, undl_code_ref)
        self.write(json.dumps({"data": ret or []}, ensure_ascii=False, default=str))

# ContextInfo.get_financial_data() - 获取财务数据
class FinancialDataHandler(BaseHandler):
    def post(self):
        data = self.json_body()
        tabname = data.get('tabname', '')
        colname = data.get('colname', '')
        market = data.get('market', '')
        code = data.get('code', '')
        report_type = data.get('report_type', 'report_time')
        barpos = int(data.get('barpos', '-1'))
        if tabname and colname and market and code:
            ret = safe_call(self.ctx().get_financial_data, tabname, colname, market, code, report_type, barpos)
        else:
            field_list = data.get('fieldList', '')
            stock_list = data.get('stockList', '')
            start_date = data.get('startDate', '')
            end_date = data.get('endDate', '')
            fields = [f.strip() for f in field_list.split(',')] if field_list else []
            stocks = [s.strip() for s in stock_list.split(',')] if stock_list else []
            rtype = data.get('report_type', 'announce_time')
            ret = safe_call(self.ctx().get_financial_data, fields, stocks, start_date, end_date, rtype)
        if hasattr(ret, 'to_dict'):
            ret = ret.to_dict()
        self.write(json.dumps({"data": ret} if ret is not None else {"error": "获取财务数据失败"}, ensure_ascii=False, default=str))

# ContextInfo.get_factor_data() - 获取多因子数据
class FactorDataHandler(BaseHandler):
    def post(self):
        data = self.json_body()
        field_list = data.get('fieldList', '')
        stock_list = data.get('stockList', '')
        stock_code = data.get('stockCode', '')
        start_date = data.get('startDate', '')
        end_date = data.get('endDate', '')
        fields = [f.strip() for f in field_list.split(',')] if field_list else []
        if stock_code:
            ret = safe_call(self.ctx().get_factor_data, fields, stock_code, start_date, end_date)
        else:
            stocks = [s.strip() for s in stock_list.split(',')] if stock_list else []
            ret = safe_call(self.ctx().get_factor_data, fields, stocks, start_date, end_date)
        if hasattr(ret, 'to_dict'):
            ret = ret.to_dict()
        self.write(json.dumps({"data": ret} if ret is not None else {"error": "获取因子数据失败"}, ensure_ascii=False, default=str))

# ContextInfo.get_his_st_data() - 获取历史ST数据
class HisStDataHandler(BaseHandler):
    def post(self):
        data = self.json_body()
        stockCode = data.get('stockCode', '')
        ret = safe_call(self.ctx().get_his_st_data, stockCode)
        self.write(json.dumps({"stockCode": stockCode, "data": ret or {}}, ensure_ascii=False))

# ContextInfo.get_his_index_data() - 获取历史指数数据
class HisIndexDataHandler(BaseHandler):
    def post(self):
        data = self.json_body()
        index = data.get('index', '')
        ret = safe_call(self.ctx().get_his_index_data, index)
        self.write(json.dumps({"index": index, "data": ret or {}}, ensure_ascii=False, default=str))

# ContextInfo.get_all_subscription() - 获取当前所有行情订阅信息
class AllSubscriptionHandler(BaseHandler):
    def get(self):
        ret = safe_call(self.ctx().get_all_subscription)
        self.write(json.dumps({"subscriptions": ret or {}}, ensure_ascii=False, default=str))

# ContextInfo.get_option_list() - 获取指定期权列表
class OptionListHandler(BaseHandler):
    def post(self):
        data = self.json_body()
        undl_code = data.get('undl_code', '')
        dedate = data.get('dedate', '')
        opttype = data.get('opttype', '')
        isavailable = data.get('isavailable', 'true').lower() == 'true'
        ret = safe_call(self.ctx().get_option_list, undl_code, dedate, opttype, isavailable)
        self.write(json.dumps({"option_list": ret or []}, ensure_ascii=False))

# ContextInfo.get_his_contract_list() - 获取过期合约列表
class HisContractListHandler(BaseHandler):
    def post(self):
        data = self.json_body()
        market = data.get('market', '')
        ret = safe_call(self.ctx().get_his_contract_list, market)
        self.write(json.dumps({"market": market, "contracts": ret or []}, ensure_ascii=False))

# ContextInfo.get_option_iv() - 获取期权实时隐含波动率
class OptionIvHandler(BaseHandler):
    def post(self):
        data = self.json_body()
        optioncode = data.get('optioncode', '')
        ret = safe_call(self.ctx().get_option_iv, optioncode)
        self.write(json.dumps({"optioncode": optioncode, "iv": ret}, ensure_ascii=False))

# ContextInfo.bsm_price() - BS模型计算欧式期权理论价格
class BsmPriceHandler(BaseHandler):
    def post(self):
        data = self.json_body()
        optionType = data.get('optionType', 'C')
        objectPrices = data.get('objectPrices', '')
        strikePrice = float(data.get('strikePrice', '0'))
        riskFree = float(data.get('riskFree', '0'))
        sigma = float(data.get('sigma', '0'))
        days = int(data.get('days', '0'))
        dividend = float(data.get('dividend', '0'))
        try:
            op = float(objectPrices)
        except ValueError:
            op = [float(x) for x in objectPrices.split(',')]
        ret = safe_call(self.ctx().bsm_price, optionType, op, strikePrice, riskFree, sigma, days, dividend)
        self.write(json.dumps({"price": ret}, ensure_ascii=False, default=str))

# ContextInfo.bsm_iv() - BS模型计算欧式期权隐含波动率
class BsmIvHandler(BaseHandler):
    def post(self):
        data = self.json_body()
        optionType = data.get('optionType', 'C')
        objectPrices = float(data.get('objectPrices', '0'))
        strikePrice = float(data.get('strikePrice', '0'))
        optionPrice = float(data.get('optionPrice', '0'))
        riskFree = float(data.get('riskFree', '0'))
        days = int(data.get('days', '0'))
        dividend = float(data.get('dividend', '0'))
        ret = safe_call(self.ctx().bsm_iv, optionType, objectPrices, strikePrice, optionPrice, riskFree, days, dividend)
        self.write(json.dumps({"iv": ret}, ensure_ascii=False))

# ContextInfo.get_local_data() - 从本地获取行情数据
class LocalDataHandler(BaseHandler):
    def post(self):
        data = self.json_body()
        stock_code = data.get('stock_code', '')
        start_time = data.get('start_time', '')
        end_time = data.get('end_time', '')
        period = data.get('period', '1d')
        divid_type = data.get('divid_type', 'none')
        count = int(data.get('count', '-1'))
        ret = safe_call(self.ctx().get_local_data, stock_code, start_time, end_time, period, divid_type, count)
        if ret is None:
            raise HTTPError(500, "获取本地行情失败")
        self.write(json.dumps({"data": ret}, ensure_ascii=False, default=str))

# ContextInfo.subscribe_quote() - 订阅行情数据
class SubscribeQuoteHandler(BaseHandler):
    def post(self):
        data = self.json_body()
        stock_code = data.get('stock_code', data.get('stocks', data.get('stock', '')))
        stock_list = parse_list(stock_code)
        if not stock_list:
            raise HTTPError(400, "need args stock_code")
        period = data.get('period', 'follow')
        dividend_type = data.get('dividend_type', 'follow')
        results = []
        for stock in stock_list:
            sub_id = safe_call(self.ctx().subscribe_quote, stock, period, dividend_type)
            results.append({
                "stock_code": stock,
                "status": "success" if sub_id is not None else "failed",
                "sub_id": sub_id,
            })
        self.write(json.dumps({
            "status": "success" if all(item["sub_id"] is not None for item in results) else "partial_failed",
            "results": results,
            "sub_id": results[0]["sub_id"] if len(results) == 1 else None,
        }, ensure_ascii=False))

# ContextInfo.unsubscribe_quote() - 反订阅行情数据
class UnsubscribeQuoteHandler(BaseHandler):
    def post(self):
        data = self.json_body()
        sub_id = int(data.get('sub_id', '0'))
        safe_call(self.ctx().unsubscribe_quote, sub_id)
        self.write(json.dumps({"status": "success", "sub_id": sub_id}, ensure_ascii=False))


# ============= 3. 判定函数 (is_*) =============
# ContextInfo.is_last_bar() - 判定是否为最后一根K线
class IsLastBarHandler(BaseHandler):
    def get(self):
        ret = safe_call(self.ctx().is_last_bar)
        self.write(json.dumps({"is_last_bar": ret}, ensure_ascii=False))

# ContextInfo.is_new_bar() - 判定是否为新的K线
class IsNewBarHandler(BaseHandler):
    def get(self):
        ret = safe_call(self.ctx().is_new_bar)
        self.write(json.dumps({"is_new_bar": ret}, ensure_ascii=False))

# ContextInfo.is_suspended_stock() - 判定股票是否停牌
class IsSuspendedStockHandler(BaseHandler):
    def post(self):
        data = self.json_body()
        stockcode = data.get('stockcode', '')
        ret = safe_call(self.ctx().is_suspended_stock, stockcode)
        self.write(json.dumps({"stockcode": stockcode, "is_suspended": ret}, ensure_ascii=False))

# is_sector_stock() - 判定股票是否在指定板块中
class IsSectorStockHandler(BaseHandler):
    def post(self):
        data = self.json_body()
        sectorname = data.get('sectorname', '')
        market = data.get('market', '')
        stockcode = data.get('stockcode', '')
        ret = safe_call(is_sector_stock, sectorname, market, stockcode)
        self.write(json.dumps({"sectorname": sectorname, "stockcode": stockcode, "is_in_sector": ret}, ensure_ascii=False))

# is_typed_stock() - 判定股票是否属于某个类别
class IsTypedStockHandler(BaseHandler):
    def post(self):
        data = self.json_body()
        stocktypenum = int(data.get('stocktypenum', '0'))
        market = data.get('market', '')
        stockcode = data.get('stockcode', '')
        ret = safe_call(is_typed_stock, stocktypenum, market, stockcode)
        self.write(json.dumps({"stocktypenum": stocktypenum, "stockcode": stockcode, "result": ret}, ensure_ascii=False))

# get_industry_name_of_stock() - 获取股票行业分类名称
class GetIndustryNameOfStockHandler(BaseHandler):
    def post(self):
        data = self.json_body()
        industryType = data.get('industryType', '')
        stockcode = data.get('stockcode', '')
        ret = safe_call(get_industry_name_of_stock, industryType, stockcode)
        self.write(json.dumps({"industryType": industryType, "stockcode": stockcode, "industry_name": ret}, ensure_ascii=False))


# ============= 4. 交易函数 =============
# passorder() - 综合交易下单(支持股票买卖等)
class PassorderHandler(BaseHandler):
    def post(self):
        try:
            data = self.json_body()
            opType = int(data['opType'])
            orderType = int(data.get('orderType', 1101))
            stock = data['stock']
            pr_type = int(data.get('prType', 11))
            price = float(data['price'])
            volume = int(data['volume'])
            quickTrade = int(data.get('quickTrade', 2))
            order_ref = passorder(opType, orderType, self.acc(), stock, pr_type, price, volume, 'qmt', quickTrade, self.ctx())
            self.write(json.dumps({
                "status": "success",
                "opType": opType,
                "stock": stock,
                "order_ref": str(order_ref) if order_ref else "unknown"
            }, ensure_ascii=False))
        except Exception as e:
            logger.exception("passorder下单异常")
            raise HTTPError(400, f"下单失败: {str(e)}")

# algo_passorder() - 算法交易下单
class AlgoPassorderHandler(BaseHandler):
    def post(self):
        try:
            data = self.json_body()
            order_ref = algo_passorder(
                int(data['opType']), int(data.get('orderType', 1101)),
                self.acc(), data['stock'], int(data.get('prType', -1)),
                float(data['price']), int(data['volume']),
                data.get('strategyName', ''), int(data.get('quickTrade', 2)),
                data.get('userOrderId', ''), data.get('userOrderParam', {}),
                self.ctx()
            )
            self.write(json.dumps({"status": "success", "order_ref": str(order_ref) if order_ref else "unknown"}, ensure_ascii=False))
        except Exception as e:
            logger.exception("algo_passorder异常")
            raise HTTPError(400, f"算法下单失败: {str(e)}")

# smart_algo_passorder() - 智能算法交易下单
class SmartAlgoPassorderHandler(BaseHandler):
    def post(self):
        try:
            data = self.json_body()
            order_ref = smart_algo_passorder(
                int(data['opType']), int(data.get('orderType', 1101)),
                self.acc(), data['stock'], int(data.get('prType', -1)),
                float(data['price']), int(data['volume']),
                data['smartAlgoType'], int(data.get('limitOverRate', 0)),
                int(data.get('minAmountPerOrder', 0)),
                data.get('startTime', ''), data.get('endTime', ''),
                self.ctx()
            )
            self.write(json.dumps({"status": "success", "order_ref": str(order_ref) if order_ref else "unknown"}, ensure_ascii=False))
        except Exception as e:
            logger.exception("smart_algo_passorder异常")
            raise HTTPError(400, f"智能算法下单失败: {str(e)}")

# order_lots() - 指定手数交易
class OrderLotsHandler(BaseHandler):
    def post(self):
        try:
            data = self.json_body()
            order_lots(data['stock'], int(data['lots']), data.get('style', 'LATEST'),
                       float(data.get('price', 0)), self.ctx(), data.get('accId', self.acc()))
            self.write(json.dumps({"status": "success", "action": "order_lots", "stock": data['stock']}, ensure_ascii=False))
        except Exception as e:
            logger.exception("order_lots异常")
            raise HTTPError(400, f"下单失败: {str(e)}")

# order_value() - 指定价值交易
class OrderValueHandler(BaseHandler):
    def post(self):
        try:
            data = self.json_body()
            order_value(data['stock'], float(data['value']), data.get('style', 'LATEST'),
                        float(data.get('price', 0)), self.ctx(), data.get('accId', self.acc()))
            self.write(json.dumps({"status": "success", "action": "order_value", "stock": data['stock']}, ensure_ascii=False))
        except Exception as e:
            logger.exception("order_value异常")
            raise HTTPError(400, f"下单失败: {str(e)}")

# order_percent() - 指定比例交易
class OrderPercentHandler(BaseHandler):
    def post(self):
        try:
            data = self.json_body()
            order_percent(data['stock'], float(data['percent']), data.get('style', 'LATEST'),
                          float(data.get('price', 0)), self.ctx(), data.get('accId', self.acc()))
            self.write(json.dumps({"status": "success", "action": "order_percent", "stock": data['stock']}, ensure_ascii=False))
        except Exception as e:
            logger.exception("order_percent异常")
            raise HTTPError(400, f"下单失败: {str(e)}")

# order_target_value() - 指定目标价值交易
class OrderTargetValueHandler(BaseHandler):
    def post(self):
        try:
            data = self.json_body()
            order_target_value(data['stock'], float(data['tar_value']), data.get('style', 'LATEST'),
                               float(data.get('price', 0)), self.ctx(), data.get('accId', self.acc()))
            self.write(json.dumps({"status": "success", "action": "order_target_value", "stock": data['stock']}, ensure_ascii=False))
        except Exception as e:
            logger.exception("order_target_value异常")
            raise HTTPError(400, f"下单失败: {str(e)}")

# order_target_percent() - 指定目标比例交易
class OrderTargetPercentHandler(BaseHandler):
    def post(self):
        try:
            data = self.json_body()
            order_target_percent(data['stock'], float(data['tar_percent']), data.get('style', 'LATEST'),
                                 float(data.get('price', 0)), self.ctx(), data.get('accId', self.acc()))
            self.write(json.dumps({"status": "success", "action": "order_target_percent", "stock": data['stock']}, ensure_ascii=False))
        except Exception as e:
            logger.exception("order_target_percent异常")
            raise HTTPError(400, f"下单失败: {str(e)}")

# order_shares() - 指定股数交易
class OrderSharesHandler(BaseHandler):
    def post(self):
        try:
            data = self.json_body()
            order_shares(data['stock'], int(data['shares']), data.get('style', 'LATEST'),
                         float(data.get('price', 0)), self.ctx(), data.get('accId', self.acc()))
            self.write(json.dumps({"status": "success", "action": "order_shares", "stock": data['stock']}, ensure_ascii=False))
        except Exception as e:
            logger.exception("order_shares异常")
            raise HTTPError(400, f"下单失败: {str(e)}")


# ============= 5. 期货交易 =============
# buy_open() - 期货买入开仓
class FuturesBuyOpenHandler(BaseHandler):
    def post(self):
        try:
            data = self.json_body()
            buy_open(data['stock'], int(data['amount']), data.get('style', 'LATEST'),
                     float(data.get('price', 0)), self.ctx(), data.get('accId', self.acc()))
            self.write(json.dumps({"status": "success", "action": "buy_open", "stock": data['stock']}, ensure_ascii=False))
        except Exception as e:
            logger.exception("buy_open异常")
            raise HTTPError(400, f"期货买入开仓失败: {str(e)}")

# buy_close_tdayfirst() - 期货买入平仓(平今优先)
class FuturesBuyCloseTdayFirstHandler(BaseHandler):
    def post(self):
        try:
            data = self.json_body()
            buy_close_tdayfirst(data['stock'], int(data['amount']), data.get('style', 'LATEST'),
                                float(data.get('price', 0)), self.ctx(), data.get('accId', self.acc()))
            self.write(json.dumps({"status": "success", "action": "buy_close_tdayfirst", "stock": data['stock']}, ensure_ascii=False))
        except Exception as e:
            logger.exception("buy_close_tdayfirst异常")
            raise HTTPError(400, f"期货买入平仓(平今)失败: {str(e)}")

# buy_close_ydayfirst() - 期货买入平仓(平昨优先)
class FuturesBuyCloseYdayFirstHandler(BaseHandler):
    def post(self):
        try:
            data = self.json_body()
            buy_close_ydayfirst(data['stock'], int(data['amount']), data.get('style', 'LATEST'),
                                float(data.get('price', 0)), self.ctx(), data.get('accId', self.acc()))
            self.write(json.dumps({"status": "success", "action": "buy_close_ydayfirst", "stock": data['stock']}, ensure_ascii=False))
        except Exception as e:
            logger.exception("buy_close_ydayfirst异常")
            raise HTTPError(400, f"期货买入平仓(平昨)失败: {str(e)}")

# sell_open() - 期货卖出开仓
class FuturesSellOpenHandler(BaseHandler):
    def post(self):
        try:
            data = self.json_body()
            sell_open(data['stock'], int(data['amount']), data.get('style', 'LATEST'),
                      float(data.get('price', 0)), self.ctx(), data.get('accId', self.acc()))
            self.write(json.dumps({"status": "success", "action": "sell_open", "stock": data['stock']}, ensure_ascii=False))
        except Exception as e:
            logger.exception("sell_open异常")
            raise HTTPError(400, f"期货卖出开仓失败: {str(e)}")

# sell_close_tdayfirst() - 期货卖出平仓(平今优先)
class FuturesSellCloseTdayFirstHandler(BaseHandler):
    def post(self):
        try:
            data = self.json_body()
            sell_close_tdayfirst(data['stock'], int(data['amount']), data.get('style', 'LATEST'),
                                 float(data.get('price', 0)), self.ctx(), data.get('accId', self.acc()))
            self.write(json.dumps({"status": "success", "action": "sell_close_tdayfirst", "stock": data['stock']}, ensure_ascii=False))
        except Exception as e:
            logger.exception("sell_close_tdayfirst异常")
            raise HTTPError(400, f"期货卖出平仓(平今)失败: {str(e)}")

# sell_close_ydayfirst() - 期货卖出平仓(平昨优先)
class FuturesSellCloseYdayFirstHandler(BaseHandler):
    def post(self):
        try:
            data = self.json_body()
            sell_close_ydayfirst(data['stock'], int(data['amount']), data.get('style', 'LATEST'),
                                 float(data.get('price', 0)), self.ctx(), data.get('accId', self.acc()))
            self.write(json.dumps({"status": "success", "action": "sell_close_ydayfirst", "stock": data['stock']}, ensure_ascii=False))
        except Exception as e:
            logger.exception("sell_close_ydayfirst异常")
            raise HTTPError(400, f"期货卖出平仓(平昨)失败: {str(e)}")


# ============= 6. 任务管理 =============
# cancel_task() - 撤销任务
class CancelTaskHandler(BaseHandler):
    def post(self):
        try:
            data = self.json_body()
            taskId = data['taskId']
            accountType = data.get('accountType', 'stock')
            ret = cancel_task(taskId, self.acc(), accountType, self.ctx())
            self.write(json.dumps({"status": "success" if ret else "failed", "taskId": taskId}, ensure_ascii=False))
        except Exception as e:
            logger.exception("cancel_task异常")
            raise HTTPError(400, f"撤销任务失败: {str(e)}")

# pause_task() - 暂停任务
class PauseTaskHandler(BaseHandler):
    def post(self):
        try:
            data = self.json_body()
            taskId = data['taskId']
            accountType = data.get('accountType', 'stock')
            ret = pause_task(taskId, self.acc(), accountType, self.ctx())
            self.write(json.dumps({"status": "success" if ret else "failed", "taskId": taskId}, ensure_ascii=False))
        except Exception as e:
            logger.exception("pause_task异常")
            raise HTTPError(400, f"暂停任务失败: {str(e)}")

# resume_task() - 继续任务
class ResumeTaskHandler(BaseHandler):
    def post(self):
        try:
            data = self.json_body()
            taskId = data['taskId']
            accountType = data.get('accountType', 'stock')
            ret = resume_task(taskId, self.acc(), accountType, self.ctx())
            self.write(json.dumps({"status": "success" if ret else "failed", "taskId": taskId}, ensure_ascii=False))
        except Exception as e:
            logger.exception("resume_task异常")
            raise HTTPError(400, f"继续任务失败: {str(e)}")

# do_order() - 实时触发前一根bar信号函数
class DoOrderHandler(BaseHandler):
    def post(self):
        try:
            do_order(self.ctx())
            self.write(json.dumps({"status": "success", "message": "信号已触发"}, ensure_ascii=False))
        except Exception as e:
            logger.exception("do_order异常")
            raise HTTPError(400, f"触发信号失败: {str(e)}")


# ============= 7. 账户/订单查询 =============
# get_trade_detail_data() - 获取交易明细(持仓/委托/成交/资金)
class TradeDetailDataHandler(BaseHandler):
    def post(self):
        data = self.json_body()
        account = data.get('account', 'stock')
        datatype = data.get('datatype', 'position')
        ret = safe_call(get_trade_detail_data, self.acc(), account, datatype, 'qmt')
        if ret is None:
            ret = []
        result = []
        for obj in ret:
            attrs = {}
            for attr in dir(obj):
                if not attr.startswith('_'):
                    try:
                        val = getattr(obj, attr)
                        if not callable(val):
                            attrs[attr] = str(val)
                    except Exception:
                        pass
            result.append(attrs)
        self.write(json.dumps({"data": result}, ensure_ascii=False))

# get_value_by_order_id() - 根据委托号获取委托/成交信息
class ValueByOrderIdHandler(BaseHandler):
    def post(self):
        data = self.json_body()
        orderId = data.get('orderId', '')
        accountType = data.get('accountType', 'stock')
        datatype = data.get('datatype', 'ORDER')
        ret = safe_call(get_value_by_order_id, orderId, self.acc(), accountType, datatype)
        attrs = {}
        if ret:
            for attr in dir(ret):
                if not attr.startswith('_'):
                    try:
                        val = getattr(ret, attr)
                        if not callable(val):
                            attrs[attr] = str(val)
                    except Exception:
                        pass
        self.write(json.dumps({"orderId": orderId, "data": attrs}, ensure_ascii=False))

# get_last_order_id() - 获取最新委托/成交的委托号
class LastOrderIdHandler(BaseHandler):
    def post(self):
        data = self.json_body()
        account = data.get('account', 'stock')
        datatype = data.get('datatype', 'ORDER')
        ret = safe_call(get_last_order_id, self.acc(), account, datatype, 'qmt')
        self.write(json.dumps({"last_order_id": ret}, ensure_ascii=False))

# can_cancel_order() - 查询委托是否可撤销
class CanCancelOrderHandler(BaseHandler):
    def post(self):
        data = self.json_body()
        orderId = data.get('orderId', '')
        accountType = data.get('accountType', 'stock')
        ret = safe_call(can_cancel_order, orderId, self.acc(), accountType)
        self.write(json.dumps({"orderId": orderId, "can_cancel": ret}, ensure_ascii=False))

# get_debt_contract() - 获取两融负债合约明细
class DebtContractHandler(BaseHandler):
    def post(self):
        data = self.json_body()
        accId = data.get('accId', self.acc())
        ret = safe_call(get_debt_contract, accId)
        result = []
        if ret:
            for obj in ret:
                attrs = {}
                for attr in dir(obj):
                    if not attr.startswith('_'):
                        try:
                            val = getattr(obj, attr)
                            if not callable(val):
                                attrs[attr] = str(val)
                        except Exception:
                            pass
                result.append(attrs)
        self.write(json.dumps({"data": result}, ensure_ascii=False))

# get_assure_contract() - 获取两融担保标的明细
class AssureContractHandler(BaseHandler):
    def post(self):
        data = self.json_body()
        accId = data.get('accId', self.acc())
        ret = safe_call(get_assure_contract, accId)
        result = []
        if ret:
            for obj in ret:
                attrs = {}
                for attr in dir(obj):
                    if not attr.startswith('_'):
                        try:
                            val = getattr(obj, attr)
                            if not callable(val):
                                attrs[attr] = str(val)
                        except Exception:
                            pass
                result.append(attrs)
        self.write(json.dumps({"data": result}, ensure_ascii=False))

# get_enable_short_contract() - 获取可融券明细
class EnableShortContractHandler(BaseHandler):
    def post(self):
        data = self.json_body()
        accId = data.get('accId', self.acc())
        ret = safe_call(get_enable_short_contract, accId)
        result = []
        if ret:
            for obj in ret:
                attrs = {}
                for attr in dir(obj):
                    if not attr.startswith('_'):
                        try:
                            val = getattr(obj, attr)
                            if not callable(val):
                                attrs[attr] = str(val)
                        except Exception:
                            pass
                result.append(attrs)
        self.write(json.dumps({"data": result}, ensure_ascii=False))

# get_ipo_data() - 获取当日新股新债信息
class IpoDataHandler(BaseHandler):
    def post(self):
        data = self.json_body()
        typ = data.get('type', '')
        ret = safe_call(get_ipo_data, typ)
        self.write(json.dumps({"data": ret or {}}, ensure_ascii=False, default=str))

# get_new_purchase_limit() - 获取新股申购额度
class NewPurchaseLimitHandler(BaseHandler):
    def post(self):
        data = self.json_body()
        accid = data.get('accid', self.acc())
        ret = safe_call(get_new_purchase_limit, accid)
        self.write(json.dumps({"data": ret or {}}, ensure_ascii=False, default=str))


# ============= 8. 引用函数 (ext_data) =============
# ext_data() - 获取扩展数据数值
class ExtDataHandler(BaseHandler):
    def post(self):
        data = self.json_body()
        extdataname = data.get('extdataname', '')
        stockcode = data.get('stockcode', '')
        deviation = int(data.get('deviation', '0'))
        ret = safe_call(ext_data, extdataname, stockcode, deviation, self.ctx())
        self.write(json.dumps({"extdataname": extdataname, "stockcode": stockcode, "value": ret}, ensure_ascii=False))

# ext_data_rank() - 获取扩展数据排名
class ExtDataRankHandler(BaseHandler):
    def post(self):
        data = self.json_body()
        extdataname = data.get('extdataname', '')
        stockcode = data.get('stockcode', '')
        deviation = int(data.get('deviation', '0'))
        ret = safe_call(ext_data_rank, extdataname, stockcode, deviation, self.ctx())
        self.write(json.dumps({"extdataname": extdataname, "stockcode": stockcode, "rank": ret}, ensure_ascii=False))

# get_factor_value() - 获取因子数据
class GetFactorValueHandler(BaseHandler):
    def post(self):
        data = self.json_body()
        factorname = data.get('factorname', '')
        stockcode = data.get('stockcode', '')
        deviation = int(data.get('deviation', '0'))
        ret = safe_call(get_factor_value, factorname, stockcode, deviation, self.ctx())
        self.write(json.dumps({"factorname": factorname, "stockcode": stockcode, "value": ret}, ensure_ascii=False))

# get_factor_rank() - 获取因子数据排名
class GetFactorRankHandler(BaseHandler):
    def post(self):
        data = self.json_body()
        factorname = data.get('factorname', '')
        stockcode = data.get('stockcode', '')
        deviation = int(data.get('deviation', '0'))
        ret = safe_call(get_factor_rank, factorname, stockcode, deviation, self.ctx())
        self.write(json.dumps({"factorname": factorname, "stockcode": stockcode, "rank": ret}, ensure_ascii=False))


# ============= 9. 原有 Handler（保持兼容） =============
# get_trade_detail_data('position') - 查询持仓列表(封装格式)
class HoldingHandler(BaseHandler):
    def post(self):
        data = self.json_body()
        account = data.get('account', 'stock')
        positions = safe_call(get_trade_detail_data, self.acc(), account, 'position') or []
        holding = {}
        for position in positions:
            stock = position.m_strInstrumentID + '.' + position.m_strExchangeID
            holding[stock] = {
                'StockCode': stock,
                'StockName': position.m_strInstrumentName,
                'Direction': position.m_nDirection,
                'Volume': position.m_nVolume,
                'OpenPrice': position.m_dOpenPrice,
                'FloatProfit': position.m_dFloatProfit,
                'MarketValue': position.m_dMarketValue,
                'StockHolder': position.m_strStockHolder,
                'FrozenVolume': position.m_nFrozenVolume,
                'CanUseVolume': position.m_nCanUseVolume,
                'OnRoadVolume': position.m_nOnRoadVolume,
                'YesterdayVolume': position.m_nYesterdayVolume,
                'LastPrice': position.m_dLastPrice,
                'ProfitRate': position.m_dProfitRate,
                'FutureTradeType': position.m_eFutureTradeType,
                'ExpireDate': position.m_strExpireDate
            }
        self.write(json.dumps(holding, ensure_ascii=False))

# get_trade_detail_data('account') - 查询总资产
class TotalMoneyHandler(BaseHandler):
    def post(self):
        data = self.json_body()
        account = data.get('account', 'stock')
        _data = safe_call(get_trade_detail_data, self.acc(), account, 'account')
        info = _data[0] if _data else None
        if not info:
            raise HTTPError(500, "资金数据获取失败")
        self.write(json.dumps({"total_money": round(info.m_dBalance, 2)}, ensure_ascii=False))

# get_trade_detail_data('account') - 查询可用资金
class AvailableMoneyHandler(BaseHandler):
    def post(self):
        data = self.json_body()
        account = data.get('account', 'stock')
        _data = safe_call(get_trade_detail_data, self.acc(), account, 'account')
        info = _data[0] if _data else None
        if not info:
            raise HTTPError(500, "资金数据获取失败")
        self.write(json.dumps({"available_money": round(info.m_dAvailable, 2)}, ensure_ascii=False))

# passorder(23) - 简化买入下单(封装passorder)
class BuyHandler(BaseHandler):
    def post(self):
        try:
            data = self.json_body()
            stock = data['stock']
            price = float(data['price'])
            volume = int(data['volume'])
            pr_type = data.get('prType', 11)
            order_ref = passorder(23, 1101, self.acc(), stock, pr_type, price, volume, 'qmt', 2, self.ctx())
            self.write(json.dumps({
                "status": "success", "action": "buy", "stock": stock,
                "order_ref": str(order_ref) if order_ref else "unknown"
            }, ensure_ascii=False))
        except Exception as e:
            logger.exception("买入下单异常")
            raise HTTPError(400, f"下单失败: {str(e)}")

# passorder(24) - 简化卖出下单(封装passorder)
class SellHandler(BaseHandler):
    def post(self):
        try:
            data = self.json_body()
            stock = data['stock']
            price = float(data['price'])
            volume = int(data['volume'])
            pr_type = data.get('prType', 11)
            order_ref = passorder(24, 1101, self.acc(), stock, pr_type, price, volume, 'qmt', 2, self.ctx())
            self.write(json.dumps({
                "status": "success", "action": "sell", "stock": stock,
                "order_ref": str(order_ref) if order_ref else "unknown"
            }, ensure_ascii=False))
        except Exception as e:
            logger.exception("卖出下单异常")
            raise HTTPError(400, f"下单失败: {str(e)}")

# get_trade_detail_data('order') - 查询委托状态列表
class OrderStatusHandler(BaseHandler):
    def post(self):
        data = self.json_body()
        account = data.get('account', 'stock')
        orders = safe_call(get_trade_detail_data, self.acc(), account, 'order', 'qmt') or []
        rets = []
        for order in orders:
            rets.append({
                "order_sys_id": order.m_strOrderSysID,
                "status": order.m_nOrderStatus,
                "volume_left": order.m_nVolumeTotal,
                "volume_traded": order.m_nVolumeTraded,
            })
        self.write(json.dumps({"orders": rets}, ensure_ascii=False))

# cancel() - 全部撤单
class CancelAllHandler(BaseHandler):
    def post(self):
        try:
            data = self.json_body()
            account = data.get('account', 'stock')
            orders = safe_call(get_trade_detail_data, self.acc(), account, 'order', 'qmt') or []
            canceled_list = []
            for order in orders:
                if can_cancel_order(order.m_strOrderSysID, self.acc(), account):
                    cancel(order.m_strOrderSysID, self.acc(), account, self.ctx())
                    canceled_list.append({
                        "order_sys_id": order.m_strOrderSysID,
                        "stock": order.m_strInstrumentID,
                        "volume_left": order.m_nVolumeTotal
                    })
            self.write(json.dumps({
                "status": "success",
                "message": f"已发出 {len(canceled_list)} 笔撤单请求",
                "canceled_orders": canceled_list
            }, ensure_ascii=False))
        except Exception as e:
            logger.exception("全部撤单异常")
            raise HTTPError(500, f"撤单失败: {str(e)}")


class CancelByRuleHandler(BaseHandler):
    def post(self):
        try:
            data = self.json_body()
            stock = data.get('stock')
            cancel_volume = int(data.get('volume', 0))
            account = data.get('account', 'stock')
            if not stock or cancel_volume <= 0:
                raise HTTPError(400, "参数错误：必须提供 stock 且 volume > 0")
            orders = safe_call(get_trade_detail_data, self.acc(), account, 'order', 'qmt') or []
            target_orders = []
            for order in orders:
                order_code = f"{order.m_strInstrumentID}.{order.m_strExchangeID}"
                if order.m_nVolumeTotal + order.m_nVolumeTraded == cancel_volume and order_code == stock and can_cancel_order(order.m_strOrderSysID, self.acc(), account):
                    target_orders.append(order)
            if not target_orders:
                self.write(json.dumps({"status": "failed", "message": "未找到符合条件的活跃订单"}, ensure_ascii=False))
                return
            canceled_ids = []
            for t_order in target_orders:
                cancel(t_order.m_strOrderSysID, self.acc(), account, self.ctx())
                canceled_ids.append(t_order.m_strOrderSysID)
            self.write(json.dumps({
                "status": "success",
                "message": f"匹配到 {len(target_orders)} 笔订单并发出撤单请求",
                "canceled_sys_ids": canceled_ids
            }, ensure_ascii=False))
        except Exception as e:
            logger.exception("规则撤单异常")
            raise HTTPError(500, f"撤单失败: {str(e)}")

# cancel() - 按股票+数量匹配规则撤单
# sys: Python版本信息
class PythonVersionHandler(BaseHandler):
    def get(self):
        import sys
        version_info = {
            "python_version": sys.version,
            "python_version_info": {
                "major": sys.version_info.major,
                "minor": sys.version_info.minor,
                "micro": sys.version_info.micro,
                "releaselevel": sys.version_info.releaselevel,
                "serial": sys.version_info.serial,
            }
        }
        self.write(json.dumps(version_info, ensure_ascii=False))

# sys: 关闭HTTP服务
class ShutdownHandler(BaseHandler):
    def post(self):
        logger.info("收到关闭请求，服务器即将停止并释放端口...")
        self.write(json.dumps({"status": "success", "message": "服务器正在关闭并释放端口..."}, ensure_ascii=False))
        self.finish()
        IOLoop.current().add_callback(lambda: _shutdown_server("api shutdown"))

# get_trade_detail_data('deal') - 查询成交明细
class DealHandler(BaseHandler):
    def post(self):
        data = self.json_body()
        account = data.get('account', 'stock')
        deals = safe_call(get_trade_detail_data, self.acc(), account, 'deal', 'qmt') or []
        rets = []
        for deal in deals:
            attrs = {}
            for attr in dir(deal):
                if not attr.startswith('_'):
                    try:
                        val = getattr(deal, attr)
                        if not callable(val):
                            attrs[attr] = str(val)
                    except Exception:
                        pass
            rets.append(attrs)
        self.write(json.dumps({"deals": rets}, ensure_ascii=False))


# ============= 路由注册 =============
def make_app():
    return Application([
        (r"/ws", MarketWebSocketHandler),

        # 原有兼容路由
        (r"/api/holding", HoldingHandler),
        (r"/api/money/total", TotalMoneyHandler),
        (r"/api/money/available", AvailableMoneyHandler),
        (r"/api/order/buy", BuyHandler),
        (r"/api/order/sell", SellHandler),
        (r"/api/order/status", OrderStatusHandler),
        (r"/api/order/cancel_all", CancelAllHandler),
        (r"/api/order/cancel_order", CancelByRuleHandler),
        (r"/api/order/deal", DealHandler),

        # ContextInfo 属性
        (r"/api/context/period", ContextPeriodHandler),
        (r"/api/context/barpos", ContextBarposHandler),
        (r"/api/context/time_tick_size", ContextTimeTickSizeHandler),
        (r"/api/context/stockcode", ContextStockCodeHandler),
        (r"/api/context/dividend_type", ContextDividendTypeHandler),
        (r"/api/context/market", ContextMarketHandler),
        (r"/api/context/do_back_test", ContextDoBackTestHandler),
        (r"/api/context/benchmark", ContextBenchmarkHandler),
        (r"/api/context/capital", ContextCapitalHandler),
        (r"/api/context/universe", ContextUniverseHandler),

        # 数据查询
        (r"/api/data/stock_name", StockNameHandler),
        (r"/api/data/open_date", OpenDateHandler),
        (r"/api/data/last_volume", LastVolumeHandler),
        (r"/api/data/bar_timetag", BarTimetagHandler),
        (r"/api/data/tick_timetag", TickTimetagHandler),
        (r"/api/data/sector", SectorHandler),
        (r"/api/data/industry", IndustryHandler),
        (r"/api/data/stock_list_in_sector", StockListInSectorHandler),
        (r"/api/data/weight_in_index", WeightInIndexHandler),
        (r"/api/data/contract_multiplier", ContractMultiplierHandler),
        (r"/api/data/risk_free_rate", RiskFreeRateHandler),
        (r"/api/data/date_location", DateLocationHandler),
        (r"/api/data/history_data", HistoryDataHandler),
        (r"/api/data/backfill/targets", BackfillTargetsHandler),
        (r"/api/data/backfill/status", BackfillStatusHandler),
        (r"/api/data/market_data", MarketDataHandler),
        (r"/api/data/market_data_ex", MarketDataExHandler),
        (r"/api/data/full_tick", FullTickHandler),
        (r"/api/data/divid_factors", DividFactorsHandler),
        (r"/api/data/main_contract", MainContractHandler),
        (r"/api/data/timetag_to_datetime", TimetagToDatetimeHandler),
        (r"/api/data/total_share", TotalShareHandler),
        (r"/api/data/trading_dates", TradingDatesHandler),
        (r"/api/data/svol", SvolHandler),
        (r"/api/data/bvol", BvolHandler),
        (r"/api/data/longhubang", LonghubangHandler),
        (r"/api/data/top10_share_holder", Top10ShareHolderHandler),
        (r"/api/data/option_detail", OptionDetailHandler),
        (r"/api/data/turnover_rate", TurnoverRateHandler),
        (r"/api/data/etf_info", EtfInfoHandler),
        (r"/api/data/etf_iopv", EtfIopvHandler),
        (r"/api/data/instrumentdetail", InstrumentDetailHandler),
        (r"/api/data/contract_expire_date", ContractExpireDateHandler),
        (r"/api/data/option_undl_data", OptionUndlDataHandler),
        (r"/api/data/financial_data", FinancialDataHandler),
        (r"/api/data/factor_data", FactorDataHandler),
        (r"/api/data/his_st_data", HisStDataHandler),
        (r"/api/data/his_index_data", HisIndexDataHandler),
        (r"/api/data/all_subscription", AllSubscriptionHandler),
        (r"/api/data/option_list", OptionListHandler),
        (r"/api/data/his_contract_list", HisContractListHandler),
        (r"/api/data/option_iv", OptionIvHandler),
        (r"/api/data/bsm_price", BsmPriceHandler),
        (r"/api/data/bsm_iv", BsmIvHandler),
        (r"/api/data/local_data", LocalDataHandler),

        # 订阅
        (r"/api/data/subscribe_quote", SubscribeQuoteHandler),
        (r"/api/data/unsubscribe_quote", UnsubscribeQuoteHandler),

        # 判定函数
        (r"/api/check/is_last_bar", IsLastBarHandler),
        (r"/api/check/is_new_bar", IsNewBarHandler),
        (r"/api/check/is_suspended_stock", IsSuspendedStockHandler),
        (r"/api/check/is_sector_stock", IsSectorStockHandler),
        (r"/api/check/is_typed_stock", IsTypedStockHandler),
        (r"/api/check/get_industry_name_of_stock", GetIndustryNameOfStockHandler),

        # 交易
        (r"/api/trade/passorder", PassorderHandler),
        (r"/api/trade/algo_passorder", AlgoPassorderHandler),
        (r"/api/trade/smart_algo_passorder", SmartAlgoPassorderHandler),
        (r"/api/trade/order_lots", OrderLotsHandler),
        (r"/api/trade/order_value", OrderValueHandler),
        (r"/api/trade/order_percent", OrderPercentHandler),
        (r"/api/trade/order_target_value", OrderTargetValueHandler),
        (r"/api/trade/order_target_percent", OrderTargetPercentHandler),
        (r"/api/trade/order_shares", OrderSharesHandler),

        # 期货交易
        (r"/api/trade/futures/buy_open", FuturesBuyOpenHandler),
        (r"/api/trade/futures/buy_close_tdayfirst", FuturesBuyCloseTdayFirstHandler),
        (r"/api/trade/futures/buy_close_ydayfirst", FuturesBuyCloseYdayFirstHandler),
        (r"/api/trade/futures/sell_open", FuturesSellOpenHandler),
        (r"/api/trade/futures/sell_close_tdayfirst", FuturesSellCloseTdayFirstHandler),
        (r"/api/trade/futures/sell_close_ydayfirst", FuturesSellCloseYdayFirstHandler),

        # 任务管理
        (r"/api/trade/cancel_task", CancelTaskHandler),
        (r"/api/trade/pause_task", PauseTaskHandler),
        (r"/api/trade/resume_task", ResumeTaskHandler),
        (r"/api/trade/do_order", DoOrderHandler),

        # 账户/订单查询
        (r"/api/trade/trade_detail_data", TradeDetailDataHandler),
        (r"/api/trade/value_by_order_id", ValueByOrderIdHandler),
        (r"/api/trade/last_order_id", LastOrderIdHandler),
        (r"/api/trade/can_cancel_order", CanCancelOrderHandler),
        (r"/api/trade/debt_contract", DebtContractHandler),
        (r"/api/trade/assure_contract", AssureContractHandler),
        (r"/api/trade/enable_short_contract", EnableShortContractHandler),
        (r"/api/trade/ipo_data", IpoDataHandler),
        (r"/api/trade/new_purchase_limit", NewPurchaseLimitHandler),

        # 引用函数
        (r"/api/ext/ext_data", ExtDataHandler),
        (r"/api/ext/ext_data_rank", ExtDataRankHandler),
        (r"/api/ext/get_factor_value", GetFactorValueHandler),
        (r"/api/ext/get_factor_rank", GetFactorRankHandler),

        # 系统
        (r"/api/sys/python_version", PythonVersionHandler),
        (r"/api/sys/shutdown", ShutdownHandler),

    ], debug=False, compress_response=True)


def _stop_server_on_ioloop(reason):
    global _SERVER
    logger.info("QMT HTTP Server shutdown requested: %s; releasing port 0.0.0.0:%s", reason, PORT)
    for client in list(_WS_CLIENTS):
        try:
            client.close()
        except Exception:
            pass
    _WS_CLIENTS.clear()
    if _SERVER is not None:
        _SERVER.stop()
        _SERVER = None
        logger.info("QMT HTTP Server stopped; released port 0.0.0.0:%s", PORT)
    IOLoop.current().stop()


def _shutdown_server(reason="stop"):
    global _STOPPING
    if _STOPPING:
        return
    _STOPPING = True
    _BACKFILL_STOP.set()
    try:
        if _IOLOOP is None:
            logger.info("QMT HTTP Server no active IOLoop; port 0.0.0.0:%s already released", PORT)
            return
        _IOLOOP.add_callback(_stop_server_on_ioloop, reason)
    except Exception as e:
        logger.exception("QMT HTTP Server release port failed: %s", e)


def init(ContextInfo):
    global _APP, _SERVER, _IOLOOP, _STOPPING
    loop = None
    try:
        if _SERVER is not None:
            _SERVER.stop()
            _SERVER = None
        if _IOLOOP is not None:
            _IOLOOP.close(all_fds=True)

        # QMT's ContextInfo and trading APIs belong to the model thread. Keep
        # Tornado on that thread too; a background IOLoop can be starved once
        # QMT enters its simulation loop and must not call ContextInfo.
        loop = IOLoop()
        loop.make_current()
        _IOLOOP = loop

        ContextInfo.accountID = ACCOUNT_ID
        app = make_app()
        app.ContextInfo = ContextInfo
        app.accountID = ContextInfo.accountID

        _APP = app
        _SERVER = HTTPServer(app)
        _SERVER.listen(PORT, address='0.0.0.0')
        start_backfill_worker(ContextInfo)
        _STOPPING = False
        logger.info(f"QMT HTTP Server 启动于 http://0.0.0.0:{PORT} (全部API已加载，模型线程运行)")
        loop.start()
        if not _STOPPING:
            logger.error("QMT HTTP IOLoop unexpectedly stopped")
    except Exception as e:
        logger.exception(f"server start failed: {e}")
    finally:
        if _SERVER is not None:
            _SERVER.stop()
            _SERVER = None
        if loop is not None:
            loop.close(all_fds=True)
        if _IOLOOP is loop:
            _IOLOOP = None


def stop(ContextInfo):
    _shutdown_server("stop")


def uninit():
    _shutdown_server("uninit")


def on_stop():
    _shutdown_server("on_stop")


def on_exit():
    _shutdown_server("on_exit")


def destroy():
    _shutdown_server("destroy")
