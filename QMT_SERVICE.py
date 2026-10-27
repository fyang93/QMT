# -*- coding: gbk -*-
import json
import locale
from math import isfinite
import re
from collections import OrderedDict, deque
from uuid import uuid4
from datetime import datetime, timedelta, timezone
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
_DOWNLOADS = OrderedDict()
_DOWNLOAD_JOBS = {}
_DOWNLOAD_CLIENT_LIMIT = 10
_DOWNLOAD_QUEUE = OrderedDict()
_DOWNLOAD_LOCK = Lock()
_DOWNLOAD_STOP = Event()
_DOWNLOAD_WAKE = Event()
_DOWNLOAD_THREAD = None
_DOWNLOAD_LIMIT = 50
_DOWNLOAD_COMPLETED_LIMIT = 200
_UNIVERSE_SUBSCRIBED = {}
_QMT_REALTIME_SYMBOL_LIMIT = 50
_CLIENT_LIMIT = 64
_UNIVERSE_SYMBOL_LIMIT = 10000
_MESSAGE_LIMIT = 1024 * 1024
_WRITE_LIMIT = 2 * 1024 * 1024
_BROADCAST_LIMIT = 256
_BROADCAST_QUEUE = deque()
_BROADCAST_LOCK = Lock()
_BROADCAST_SCHEDULED = False
_BRIDGE_ID = uuid4().hex
_STARTED_AT = datetime.now(timezone.utc).isoformat()
_UNIVERSE_CLIENTS = {}
_REALTIME_SUBSCRIPTIONS = {}
_REALTIME_RELEASE_AT = {}
_REALTIME_PUSHER = None
_ACCOUNT_PUSHER = None
_HEALTH_SAMPLER = None
_IOLOOP_LAST_TICK = None
_IOLOOP_LAG_SAMPLES = deque(maxlen=120)
_REALTIME_LAST_PUSH_AT = None
_REALTIME_PUSHED_SYMBOL_COUNT = 0
_REALTIME_LAST_5M = 0.0
_DISCONNECT_GRACE_SECONDS = 120
# Universe and sockets are IOLoop-confined; native callbacks and downloads use their own locks.


def safe_call(func, *args, **kwargs):
    try:
        return func(*args, **kwargs)
    except Exception as e:
        logger.error("QMT call failed: %s", e)
        return None


def parse_bool(value, default=False):
    if isinstance(value, bool):
        return value
    if isinstance(value, (str, int)) and str(value).lower() in ("1", "true", "yes", "y", "on"):
        return True
    if isinstance(value, (str, int)) and str(value).lower() in ("0", "false", "no", "n", "off"):
        return False
    raise HTTPError(400, "expected a boolean")


def parse_list(value):
    if isinstance(value, str):
        value = value.split(",")
    if not isinstance(value, (list, tuple)) or any(not isinstance(item, str) for item in value):
        raise HTTPError(400, "expected a comma-separated string or list of strings")
    return [item.strip() for item in value if item.strip()]


def number(value, field):
    try:
        if isinstance(value, bool) or not isinstance(value, (str, int, float)):
            raise ValueError()
        result = float(value)
        if not isfinite(result):
            raise ValueError()
    except (TypeError, ValueError, OverflowError):
        raise HTTPError(400, "%s must be a finite number" % field)
    if field == "price" and result < 0 and result != -1:
        raise HTTPError(400, "price must be non-negative or the native -1 sentinel")
    if field in ("volume", "amount") and result <= 0:
        raise HTTPError(400, "%s must be positive" % field)
    if field in ("sigma", "strikePrice", "optionPrice", "days", "minAmountPerOrder") and result < 0:
        raise HTTPError(400, "%s must be non-negative" % field)
    return result


def integer(value, field):
    result = number(value, field)
    if not result.is_integer():
        raise HTTPError(400, "%s must be an integer" % field)
    if field in ("count", "len") and result < -1:
        raise HTTPError(400, "%s must be -1 or non-negative" % field)
    if isinstance(value, int) or isinstance(value, str) and re.fullmatch(r"[+-]?[0-9]+", value.strip()):
        return int(value)
    return int(result)


def validate_client_id(value):
    if not isinstance(value, str) or not value.strip() or len(value) > 1024 or any(ord(char) < 32 for char in value):
        raise HTTPError(400, "client_id must be a non-empty string of at most 1024 characters")


def validate_body(data):
    lists = {"fields", "field", "fieldList", "stockList", "stock_list", "stocks", "stock_code", "symbols", "objectPrices"}
    text = {"stock", "stockcode", "stockCode", "period", "start_time", "end_time", "startTime", "endTime",
            "start_date", "end_date", "startDate", "endDate", "account", "accountType", "accId", "accid",
            "sector", "sectorname", "industry", "indexcode", "contractcode", "strdate", "dividend_type",
            "dividendType", "divid_type", "format", "codemarket", "optioncode", "undl_code_ref",
            "tabname", "colname", "market", "code", "report_type", "data_name", "undl_code", "dedate",
            "opttype", "optionType", "strategyName", "userOrderId", "smartAlgoType", "style",
            "datatype", "extdataname", "factorname", "industryType", "client_id"}
    for key, value in data.items():
        if key in lists and key != "objectPrices":
            parse_list(value)
        elif key in text and not isinstance(value, str):
            raise HTTPError(400, "%s must be a string" % key)
        elif key == "userOrderParam" and not isinstance(value, dict):
            raise HTTPError(400, "userOrderParam must be an object")
    pending = list(data.values())
    while pending:
        value = pending.pop()
        if isinstance(value, str):
            try:
                value.encode("utf-8")
            except UnicodeError:
                raise HTTPError(400, "JSON strings must be valid Unicode")
        elif isinstance(value, dict):
            pending.extend(value.values())
        elif isinstance(value, list):
            pending.extend(value)
        elif isinstance(value, float) and not isfinite(value):
            raise HTTPError(400, "JSON numbers must be finite")


def to_jsonable(value):
    if value is None:
        return None
    if hasattr(value, "to_dict"):
        return to_jsonable(value.to_dict())
    if isinstance(value, float) and not isfinite(value):
        return None
    if isinstance(value, dict):
        result = {}
        for k, v in value.items():
            result[str(k)] = to_jsonable(v)
        return result
    if isinstance(value, (list, tuple)):
        return [to_jsonable(v) for v in value]
    return value


def _queue_broadcast(payload, stream=None):
    global _BROADCAST_SCHEDULED
    try:
        payload = to_jsonable(payload)
        if len(json.dumps(payload, default=str, allow_nan=False).encode("utf-8")) > _MESSAGE_LIMIT:
            logger.warning("QMT broadcast exceeds message limit; reread broker facts")
            return
    except Exception:
        logger.exception("QMT broadcast serialization failed")
        return
    loop = _IOLOOP
    if loop is None:
        return
    with _BROADCAST_LOCK:
        if len(_BROADCAST_QUEUE) >= _BROADCAST_LIMIT:
            logger.warning("QMT broadcast queue full; clients must reread broker facts")
            return
        _BROADCAST_QUEUE.append((payload, stream))
        if _BROADCAST_SCHEDULED:
            return
        _BROADCAST_SCHEDULED = True
    try:
        loop.add_callback(_drain_broadcasts)
    except Exception:
        with _BROADCAST_LOCK:
            _BROADCAST_QUEUE.clear()
            _BROADCAST_SCHEDULED = False
        logger.exception("QMT broadcast scheduling failed")


def _drain_broadcasts():
    global _BROADCAST_SCHEDULED
    with _BROADCAST_LOCK:
        pending = list(_BROADCAST_QUEUE)
        _BROADCAST_QUEUE.clear()
        _BROADCAST_SCHEDULED = False
    for payload, stream in pending:
        _broadcast(payload, stream)


def _broadcast(payload, stream=None):
    for client in list(_WS_CLIENTS):
        if stream is None or stream in getattr(client, "streams", set()):
            safe_call(client.send_json, payload)


def parse_universe(rows):
    if not isinstance(rows, list) or len(rows) > _UNIVERSE_SYMBOL_LIMIT:
        raise ValueError("universe must be a list of at most %s {symbol, subscribed} objects" % _UNIVERSE_SYMBOL_LIMIT)
    desired = {}
    for row in rows:
        if not isinstance(row, dict) or set(row) != {"symbol", "subscribed"}:
            raise ValueError("each universe row must contain only symbol and subscribed")
        symbol = row["symbol"]
        if not isinstance(symbol, str) or not re.fullmatch(r"[A-Za-z0-9_]+\.[A-Za-z0-9_]+", symbol) or len(symbol) > 64 or type(row["subscribed"]) is not bool:
            raise ValueError("universe symbol must be a valid QMT code <=64 characters and subscribed must be boolean")
        symbol = symbol.upper()
        if symbol in desired:
            raise ValueError("universe contains duplicate symbol: %s" % symbol)
        desired[symbol] = row["subscribed"]
    return desired


def set_universe_targets(desired):
    _UNIVERSE_SUBSCRIBED.clear()
    _UNIVERSE_SUBSCRIBED.update(desired)


def merged_universe(clients):
    desired = {}
    for client in clients.values():
        for stock, subscribed in client["universe"].items():
            desired[stock] = desired.get(stock, False) or subscribed
    return desired


def set_client_universe(owner, universe, connection=None):
    if owner not in _UNIVERSE_CLIENTS and len(_UNIVERSE_CLIENTS) >= _CLIENT_LIMIT:
        raise ValueError("configured client limit reached, including disconnect grace")
    clients = dict(_UNIVERSE_CLIENTS)
    clients[owner] = {
        "universe": dict(universe), "connection": connection, "expires": None,
        "addr": connection.request.remote_ip if connection is not None else None,
        "expires_at": None,
    }
    desired = merged_universe(clients)
    if len(desired) > _UNIVERSE_SYMBOL_LIMIT:
        raise ValueError("combined universe exceeds symbol limit")
    occupied = {stock for stock, period in _REALTIME_SUBSCRIPTIONS} | {stock for stock, enabled in desired.items() if enabled}
    if len(occupied) > _QMT_REALTIME_SYMBOL_LIMIT:
        own = sum(universe.values())
        current = sum(_UNIVERSE_CLIENTS.get(owner, {}).get("universe", {}).values())
        raise ValueError("realtime limit=%s global_occupied=%s client_occupied=%s client_requested=%s combined_requested=%s (including grace)" % (
            _QMT_REALTIME_SYMBOL_LIMIT, len({stock for stock, period in _REALTIME_SUBSCRIPTIONS}), current, own, len(occupied)))
    previous = _UNIVERSE_CLIENTS.get(owner, {}).get("connection")
    _UNIVERSE_CLIENTS.clear()
    _UNIVERSE_CLIENTS.update(clients)
    set_universe_targets(desired)
    if previous is not None and previous is not connection:
        safe_call(previous.close, code=4000, reason="client_id reconnected")


def disconnect_universe_client(owner, connection):
    client = _UNIVERSE_CLIENTS.get(owner)
    if client and client["connection"] is connection:
        client["connection"] = None
        client["expires"] = monotonic() + _DISCONNECT_GRACE_SECONDS
        client["expires_at"] = (datetime.now(timezone.utc) + timedelta(seconds=_DISCONNECT_GRACE_SECONDS)).isoformat()


def expire_universe_clients():
    previous = {stock for stock, enabled in _UNIVERSE_SUBSCRIBED.items() if enabled}
    expired = [owner for owner, client in _UNIVERSE_CLIENTS.items() if client["expires"] is not None and client["expires"] <= monotonic()]
    for owner in expired:
        del _UNIVERSE_CLIENTS[owner]
        logger.info("QMT client grace expired: client_id=%s", owner)
    if expired:
        set_universe_targets(merged_universe(_UNIVERSE_CLIENTS))
        for key in _REALTIME_SUBSCRIPTIONS:
            if key[0] in previous and not _UNIVERSE_SUBSCRIBED.get(key[0], False):
                _REALTIME_RELEASE_AT[key] = monotonic()
        logger.info("QMT universe after expiry: symbols=%s realtime=%s clients=%s", len(_UNIVERSE_SUBSCRIBED), sum(_UNIVERSE_SUBSCRIBED.values()), len(_UNIVERSE_CLIENTS))


def refresh_global_subscriptions(ctx):
    desired = {(stock, period) for stock, enabled in _UNIVERSE_SUBSCRIBED.items() if enabled for period in ("1m", "5m")}
    for key, sub_id in list(_REALTIME_SUBSCRIPTIONS.items()):
        if key in desired:
            _REALTIME_RELEASE_AT.pop(key, None)
        else:
            if monotonic() < _REALTIME_RELEASE_AT.setdefault(key, monotonic() + _DISCONNECT_GRACE_SECONDS):
                continue
            # Keep a failed unsubscribe tracked so the next tick can retry it.
            try:
                ctx.unsubscribe_quote(int(sub_id))
            except Exception:
                logger.exception("QMT realtime unsubscribe failed: %s", key)
                continue
            del _REALTIME_SUBSCRIPTIONS[key]
            _REALTIME_RELEASE_AT.pop(key, None)
    for stock, period in sorted(desired - set(_REALTIME_SUBSCRIPTIONS)):
        sub_id = safe_call(ctx.subscribe_quote, stock, period, "none")
        if sub_id is not None:
            _REALTIME_SUBSCRIPTIONS[stock, period] = sub_id


def push_global_market_data():
    safe_call(_push_global_market_data)


def _push_global_market_data():
    global _REALTIME_LAST_5M, _REALTIME_LAST_PUSH_AT, _REALTIME_PUSHED_SYMBOL_COUNT
    pushed = set()
    try:
        expire_universe_clients()
        ctx = _APP.ContextInfo
        refresh_global_subscriptions(ctx)
    except Exception:
        logger.exception("QMT realtime subscription refresh failed")
        return
    for period in ("1m", "5m"):
        if period == "5m":
            if monotonic() - _REALTIME_LAST_5M < 5:
                continue
            _REALTIME_LAST_5M = monotonic()
        stocks = sorted(stock for stock, enabled in _UNIVERSE_SUBSCRIBED.items() if enabled and (stock, period) in _REALTIME_SUBSCRIPTIONS)
        if not stocks:
            continue
        data = safe_call(ctx.get_market_data_ex, list(_QMT_FIELDS), stocks, period, "", "", 2, "none", False, False)
        if not isinstance(data, dict):
            continue
        data = to_jsonable(data)
        for client in list(_WS_CLIENTS):
            if _UNIVERSE_CLIENTS.get(client.client_id, {}).get("connection") is not client:
                continue
            selected = {stock: bars for stock, bars in data.items() if client.universe.get(stock, False)}
            if selected:
                if safe_call(client.send_json, {"type": "bar", "period": period, "stocks": sorted(selected), "fields": list(_QMT_FIELDS), "count": 2, "data": selected}):
                    pushed.update(selected)
    if pushed:
        # These writes were queued, not acknowledged by the receiving clients.
        _REALTIME_LAST_PUSH_AT = datetime.now(timezone.utc).isoformat()
        _REALTIME_PUSHED_SYMBOL_COUNT = len(pushed)


def update_downloads():
    # Called under _DOWNLOAD_LOCK; job outcomes are shared across callers.
    for request in _DOWNLOADS.values():
        jobs = request["jobs"]
        previous_status = request["status"]
        if previous_status in ("success", "failed"):
            continue
        if all(job["status"] in ("success", "failed") for job in jobs):
            errors = [job["error"] for job in jobs if job["status"] == "failed"]
            request.update(status="failed" if errors else "success", error="; ".join(errors)[:2000] if errors else None)
        elif any(job["status"] != "queued" for job in jobs):
            request["status"] = "running"
        new_status = request["status"]
        if new_status != previous_status:
            request["updated_at"] = datetime.now(timezone.utc).isoformat()
        if new_status in ("success", "failed"):
            request["finished_at"] = request["updated_at"]
    completed = [key for key, value in _DOWNLOADS.items() if value["status"] in ("success", "failed")]
    for key in completed[:-_DOWNLOAD_COMPLETED_LIMIT]:
        del _DOWNLOADS[key]
    retained = {id(job) for request in _DOWNLOADS.values() for job in request["jobs"]}
    for key, job in list(_DOWNLOAD_JOBS.items()):
        if id(job) not in retained:
            del _DOWNLOAD_JOBS[key]
    for owner, queue in list(_DOWNLOAD_QUEUE.items()):
        queue = deque(key for key in queue if key in _DOWNLOADS and _DOWNLOADS[key]["status"] in ("queued", "running"))
        if queue:
            _DOWNLOAD_QUEUE[owner] = queue
        else:
            del _DOWNLOAD_QUEUE[owner]


def take_download():
    with _DOWNLOAD_LOCK:
        for owner, queue in list(_DOWNLOAD_QUEUE.items()):
            while queue and queue[0] not in _DOWNLOADS:
                queue.popleft()
            if not queue:
                del _DOWNLOAD_QUEUE[owner]
                continue
            request = _DOWNLOADS[queue[0]]
            job = next((job for job in request["jobs"] if job["status"] == "queued"), None)
            if job is None:
                queue.popleft()
                if queue:
                    _DOWNLOAD_QUEUE.move_to_end(owner)
                else:
                    del _DOWNLOAD_QUEUE[owner]
                continue
            _DOWNLOAD_QUEUE.move_to_end(owner)
            job["status"] = "running"
            update_downloads()
            return job
        return None


def run_download(job):
    try:
        symbol, period, start_time, end_time = job["key"]
        downloader = globals().get("download_history_data2")
        if downloader is None:
            raise RuntimeError("download_history_data2 is unavailable")
        result = downloader([symbol], period, start_time, end_time, incrementally=False)
        if result is False:
            raise RuntimeError("download_history_data2 returned False")
        status, error = "success", None
    except Exception as exc:
        logger.exception("QMT download failed")
        status, error = "failed", str(exc)[:2000] or type(exc).__name__
    with _DOWNLOAD_LOCK:
        job.update(status=status, error=error)
        update_downloads()


def download_worker():
    while not _DOWNLOAD_STOP.is_set():
        job = take_download()
        if job is None:
            _DOWNLOAD_WAKE.wait(0.5)
            _DOWNLOAD_WAKE.clear()
            continue
        run_download(job)


def start_download_worker():
    global _DOWNLOAD_THREAD
    with _DOWNLOAD_LOCK:
        if _DOWNLOAD_THREAD is not None and _DOWNLOAD_THREAD.is_alive():
            if _DOWNLOAD_STOP.is_set():
                raise RuntimeError("previous download is still stopping; restart after it finishes")
            return
        _DOWNLOAD_STOP.clear()
        _DOWNLOAD_THREAD = Thread(target=download_worker, name="qmt-download", daemon=True)
        _DOWNLOAD_THREAD.start()


def _position_unrealized_pnl(position):
    """Return mark-to-holding-cost P&L for a live stock position.

    QMT's m_dFloatProfit is its intraday holding P&L: it is marked to the
    previous close, not to m_dOpenPrice. Keep it separately as DailyHoldingPnl
    so callers never confuse a daily change with total unrealized P&L.
    """
    try:
        volume = float(position.m_nVolume)
        open_price = float(position.m_dOpenPrice)
        last_price = float(position.m_dLastPrice)
    except (AttributeError, TypeError, ValueError):
        return None
    if volume <= 0 or open_price <= 0 or last_price <= 0:
        return None
    return (last_price - open_price) * volume


def account_status_payload():
    try:
        position_rows = get_trade_detail_data(ACCOUNT_ID, "stock", "position", "qmt")
    except Exception:
        logger.exception("QMT position query failed")
        return {"account": ACCOUNT_ID, "account_type": "stock", "positions": [], "positions_complete": False}
    if position_rows is None:
        logger.error("QMT position query returned no result")
        return {"account": ACCOUNT_ID, "account_type": "stock", "positions": [], "positions_complete": False}

    positions = []
    positions_complete = True
    for position in position_rows:
        try:
            # QMT can retain zero-volume rows with stale P&L after a close.
            if float(position.m_nVolume) <= 0:
                continue
            symbol = position.m_strInstrumentID + "." + position.m_strExchangeID
            unrealized_pnl = _position_unrealized_pnl(position)
            daily_holding_pnl = position.m_dFloatProfit
            positions.append({
                "StockCode": symbol,
                "StockName": position.m_strInstrumentName,
                "Volume": position.m_nVolume,
                "OpenPrice": position.m_dOpenPrice,
                # Compatibility field: QMT's m_dFloatProfit is daily holding
                # P&L marked to the prior close, not total unrealized P&L.
                "FloatProfit": daily_holding_pnl,
                "DailyHoldingPnl": daily_holding_pnl,
                "UnrealizedPnl": unrealized_pnl,
                "PnlSources": {
                    "daily_holding_pnl": "qmt_m_dFloatProfit_marked_to_prior_close",
                    "unrealized_pnl": "last_price_minus_open_price",
                },
                "LastPrice": position.m_dLastPrice,
                "MarketValue": position.m_dMarketValue,
                "CanUseVolume": position.m_nCanUseVolume,
                "YesterdayVolume": position.m_nYesterdayVolume,
            })
        except Exception:
            positions_complete = False
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
        "positions_complete": positions_complete,
    }


def push_account_status():
    payload = safe_call(account_status_payload)
    if payload is not None:
        _broadcast({"type": "account_status", "data": payload}, "account")


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
        error = self._reason if status_code < 500 else "internal bridge error"
        exc = kwargs.get("exc_info", (None, None, None))[1]
        if status_code < 500 and isinstance(exc, HTTPError) and exc.log_message:
            error = exc.log_message % exc.args if exc.args else exc.log_message
        self.finish(json.dumps({"error": error, "status_code": status_code}, ensure_ascii=False))

    def json_body(self):
        raw = self.request.body or b"{}"
        if len(raw) > _MESSAGE_LIMIT:
            raise HTTPError(400, "JSON body exceeds size limit")
        try:
            data = json.loads(raw.decode("utf-8"))
        except (ValueError, UnicodeError, RecursionError):
            raise HTTPError(400, "body must be valid UTF-8 JSON")
        if not isinstance(data, dict):
            raise HTTPError(400, "JSON body must be an object")
        validate_body(data)
        return data

    def ctx(self):
        return self.application.ContextInfo

    def acc(self):
        return self.application.accountID


class MarketWebSocketHandler(WebSocketHandler):
    def initialize(self):
        self.universe = {}
        self.client_id = "ws:%s" % id(self)
        self.configured = False
        self.streams = set()
        self.write_buffer_bytes = 0

    def check_origin(self, origin):
        return True

    def open(self):
        token = self.get_argument("token", None) or self.request.headers.get("X-Token")
        if token != TOKEN:
            self.close(code=4001, reason="认证失败：token 无效或缺失")
            return
        if len(_WS_CLIENTS) >= _CLIENT_LIMIT:
            self.close(code=4002, reason="client limit reached")
            return
        _WS_CLIENTS.add(self)
        logger.info("QMT websocket opened: addr=%s client_id=%s symbols=0 global_symbols=%s global_realtime=%s", self.request.remote_ip, self.client_id, len(_UNIVERSE_SUBSCRIBED), sum(_UNIVERSE_SUBSCRIBED.values()))
        self.send_json({"type": "connected", "bridge_id": _BRIDGE_ID, "message": "QMT websocket connected"})

    def on_close(self):
        _WS_CLIENTS.discard(self)
        disconnect_universe_client(self.client_id, self)
        logger.info("QMT websocket closed: addr=%s client_id=%s symbols=%s realtime=%s global_symbols=%s global_realtime=%s grace_seconds=%s", self.request.remote_ip, self.client_id, len(self.universe), sum(self.universe.values()), len(_UNIVERSE_SUBSCRIBED), sum(_UNIVERSE_SUBSCRIBED.values()), _DISCONNECT_GRACE_SECONDS)

    def on_message(self, message):
        try:
            if len(message) > _MESSAGE_LIMIT:
                raise ValueError("message exceeds size limit")
            data = json.loads(message)
            if not isinstance(data, dict) or not isinstance(data.get("action", "ping"), str):
                raise ValueError("message must be a JSON object with a string action")
            action = data.get("action", "ping")
            if action == "ping":
                self.send_json({"type": "pong"})
            elif action in ("configure", "universe"):
                self.handle_configure(data)
            elif action == "account_status":
                payload = account_status_payload()
                if payload is None:
                    self.send_json({"type": "error", "message": "account status unavailable"})
                else:
                    self.send_json({"type": "account_status", "data": payload})
            elif action == "subscribe":
                self.handle_subscribe(data)
            elif action == "unsubscribe":
                self.handle_unsubscribe(data)
            elif action == "subscriptions":
                self.send_json({"type": "subscriptions", "subscriptions": [
                    {"stock_code": stock, "period": period, "sub_id": sub_id}
                    for (stock, period), sub_id in _REALTIME_SUBSCRIPTIONS.items() if self.universe.get(stock, False)
                ]})
            else:
                raise ValueError("unknown action: %s" % action)
        except Exception as exc:
            self.send_json({"type": "error", "message": str(exc)})

    def handle_configure(self, data):
        try:
            universe = parse_universe(data.get("universe"))
            client_id = data.get("client_id", self.client_id)
            if not isinstance(client_id, str) or not client_id.strip() or len(client_id) > 1024 or any(ord(char) < 32 for char in client_id):
                raise ValueError("client_id must be a non-empty string of at most 1024 characters")
            if self.configured and client_id != self.client_id:
                raise ValueError("client_id cannot change on a configured connection")
            streams = data.get("streams", [])
            if not isinstance(streams, list) or any(not isinstance(item, str) or item not in ("account", "order", "deal") for item in streams):
                raise ValueError("streams must be a list containing account, order and/or deal")
            set_client_universe(client_id, universe, self)
            streams = set(streams)
        except ValueError as exc:
            self.send_json({"type": "error", "message": str(exc)})
            return
        self.client_id = client_id
        self.universe = universe
        self.configured = True
        self.streams = streams
        logger.info(
            "QMT universe configured: symbols=%s realtime=%s addr=%s client_id=%s global_symbols=%s global_realtime=%s clients=%s",
            len(self.universe), sum(self.universe.values()), self.request.remote_ip, self.client_id,
            len(_UNIVERSE_SUBSCRIBED), sum(_UNIVERSE_SUBSCRIBED.values()), len(_UNIVERSE_CLIENTS),
        )
        self.refresh_subscriptions()
        self.send_json({
            "type": "configured",
            "symbols": len(self.universe),
            "streams": sorted(self.streams),
            "realtime": sum(self.universe.values()),
            "bridge_id": _BRIDGE_ID,
            "global_symbols": len(_UNIVERSE_SUBSCRIBED),
            "global_realtime": sum(_UNIVERSE_SUBSCRIBED.values()),
        })

    def refresh_subscriptions(self):
        refresh_global_subscriptions(self.application.ContextInfo)

    def handle_subscribe(self, data):
        # Legacy bar subscriptions share the configured universe, never a second QMT subscription.
        stocks = parse_list(data.get("stock_code", data.get("stocks", data.get("stock", ""))))
        if not stocks or data.get("period", "1m") not in ("1m", "5m"):
            self.send_json({"type": "error", "message": "use configure universe for 1m/5m bars; tick data uses HTTP full_tick"})
            return
        universe = dict(self.universe)
        universe.update({stock.upper(): True for stock in stocks})
        self.handle_configure({"client_id": self.client_id, "universe": [
            {"symbol": stock, "subscribed": subscribed} for stock, subscribed in universe.items()
        ]})

    def handle_unsubscribe(self, data):
        stocks = set(parse_list(data.get("stock_code", data.get("stocks", ""))))
        sub_ids = set(parse_list(data.get("sub_id", data.get("sub_ids", ""))))
        stocks.update(stock for (stock, period), sub_id in _REALTIME_SUBSCRIPTIONS.items() if str(sub_id) in sub_ids)
        self.handle_configure({"client_id": self.client_id, "universe": [
            {"symbol": stock, "subscribed": enabled and stock not in stocks} for stock, enabled in self.universe.items()
        ]})

    def send_json(self, data):
        if self.ws_connection is None:
            return False
        try:
            payload = json.dumps(to_jsonable(data), ensure_ascii=False, default=str, allow_nan=False)
            size = len(payload.encode("utf-8"))
            if self.write_buffer_bytes + size > _WRITE_LIMIT:
                self.close(code=4003, reason="slow client write buffer limit")
                return False
            self.write_buffer_bytes += size
            try:
                future = self.write_message(payload)
            except Exception:
                self.write_buffer_bytes -= size
                raise
            if future is not None:
                future.add_done_callback(lambda completed: self._consume_write_result(completed, size, self))
            else:
                self.write_buffer_bytes -= size
            return True
        except Exception as e:
            logger.debug("websocket write skipped: %s", e)
            return False

    @staticmethod
    def _consume_write_result(future, size=0, client=None):
        try:
            future.result()
        except Exception:
            pass
        finally:
            if client is not None:
                client.write_buffer_bytes = max(0, client.write_buffer_bytes - size)


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
        index = integer(data.get('index', -1), 'index')
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
        index = integer(data.get('index', '-1'), 'index')
        ret = safe_call(self.ctx().get_risk_free_rate, index)
        self.write(json.dumps({"index": index, "risk_free_rate": ret}, ensure_ascii=False))

# ContextInfo.get_date_location() - 获取日期对应的K线索引
class DateLocationHandler(BaseHandler):
    def post(self):
        data = self.json_body()
        strdate = data.get('strdate', '')
        ret = safe_call(self.ctx().get_date_location, strdate)
        self.write(json.dumps({"strdate": strdate, "location": ret}, ensure_ascii=False))

def validate_history_page(stocks, period, start_time, end_time, count):
    # Bound model-thread local reads too; clients page archives rather than monopolize the IOLoop.
    if len(stocks) > (20 if period == "1d" else 5):
        raise HTTPError(400, "history page exceeds symbol limit (20 daily / 5 intraday)")
    if count == 0 or count < -1 or count > (366 if period == "1d" else 2000):
        raise HTTPError(400, "history count exceeds page limit")
    if count == -1 and (not start_time or not end_time):
        raise HTTPError(400, "unbounded history requires explicit start_time and end_time")
    if start_time and end_time:
        try:
            start = datetime.strptime(str(start_time), "%Y%m%d" if len(str(start_time)) == 8 else "%Y%m%d%H%M%S")
            end = datetime.strptime(str(end_time), "%Y%m%d" if len(str(end_time)) == 8 else "%Y%m%d%H%M%S")
        except ValueError:
            raise HTTPError(400, "history timestamps must be YYYYMMDD or YYYYMMDDhhmmss")
        if end < start or end - start > timedelta(days=366 if period == "1d" else 7):
            raise HTTPError(400, "history page exceeds time range (366 daily / 7 intraday days)")


# 盘中只读有界本地分页；补数由全局后台队列负责。
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
        count = integer(data.get('count', data.get('len', '-1')), 'count')
        dividend_type = data.get('dividend_type', data.get('dividendType', 'none'))
        fields = data.get('fields', data.get('field', 'open,high,low,close,volume'))
        fields_list = parse_list(fields)
        if parse_bool(data.get("download", False)):
            raise HTTPError(400, "history_data is read-only; use POST /api/data/download")
        validate_history_page(stock_list, period, start_time, end_time, count)

        try:
            ret = self.ctx().get_market_data_ex(
                fields_list, stock_list, period, start_time, end_time, count,
                dividend_type, False, False
            )
        except HTTPError:
            raise
        except Exception as e:
            logger.exception("QMT history read failed")
            raise HTTPError(500, "获取历史行情失败: %s" % e)
        if ret is None:
            raise HTTPError(500, "获取历史行情失败")

        self.write(json.dumps({
            "data": to_jsonable(ret),
            "source": "get_market_data_ex",
            "download": {},
        }, ensure_ascii=False, default=str))


def validate_download(data):
    if set(data) != {"client_id", "symbols", "period", "start_time", "end_time"}:
        raise HTTPError(400, "download requires only client_id, symbols, period, start_time and end_time")
    client_id = data["client_id"]
    validate_client_id(client_id)
    symbols = data["symbols"]
    if not isinstance(symbols, list) or not symbols:
        raise HTTPError(400, "symbols must be a non-empty list")
    if any(not isinstance(symbol, str) or not re.fullmatch(r"[A-Za-z0-9_]+\.[A-Za-z0-9_]+", symbol) or len(symbol) > 64 for symbol in symbols):
        raise HTTPError(400, "symbols must be QMT codes such as 600000.SH")
    period = data["period"]
    if period not in ("1d", "5m", "1m"):
        raise HTTPError(400, "period must be 1d, 5m or 1m")
    start_time, end_time = data["start_time"], data["end_time"]
    try:
        if any(not isinstance(value, str) or not re.fullmatch(r"[0-9]{14}", value) for value in (start_time, end_time)):
            raise ValueError()
        start = datetime.strptime(start_time, "%Y%m%d%H%M%S")
        end = datetime.strptime(end_time, "%Y%m%d%H%M%S")
    except ValueError:
        raise HTTPError(400, "start_time and end_time must be valid 14-digit YYYYMMDDHHMMSS timestamps")
    if start > end:
        raise HTTPError(400, "start_time must not exceed end_time")
    if end > datetime.now(_QMT_TIMEZONE).replace(tzinfo=None):
        raise HTTPError(400, "end_time must not be in the future (Beijing time)")
    return client_id, sorted(set(symbol.upper() for symbol in symbols)), period, start_time, end_time


def download_payload(request_id, request):
    payload = {key: request[key] for key in (
        "client_id", "symbols", "period", "start_time", "end_time", "status", "error",
        "created_at", "updated_at", "finished_at")}
    payload.update(id=request_id, progress={
        "total": len(request["jobs"]),
        "completed": sum(job["status"] in ("success", "failed") for job in request["jobs"]),
        "failed": sum(job["status"] == "failed" for job in request["jobs"]),
    })
    return payload


class DownloadHandler(BaseHandler):
    def post(self):
        client_id, symbols, period, start_time, end_time = validate_download(self.json_body())
        with _DOWNLOAD_LOCK:
            for request_id, request in _DOWNLOADS.items():
                if (request["client_id"] == client_id and request["status"] != "failed"
                        and request["symbols"] == symbols and request["period"] == period
                        and request["start_time"] == start_time and request["end_time"] == end_time):
                    payload = download_payload(request_id, request)
                    break
            else:
                active = [value for value in _DOWNLOADS.values() if value["status"] in ("queued", "running")]
                if len(active) >= _DOWNLOAD_LIMIT:
                    raise HTTPError(429, "global download queue limit reached (%s)" % _DOWNLOAD_LIMIT)
                if sum(value["client_id"] == client_id for value in active) >= _DOWNLOAD_CLIENT_LIMIT:
                    raise HTTPError(429, "client download queue limit reached (%s): %s" % (_DOWNLOAD_CLIENT_LIMIT, client_id))
                jobs = []
                for symbol in symbols:
                    key = (symbol, period, start_time, end_time)
                    job = _DOWNLOAD_JOBS.get(key)
                    if job is None or job["status"] == "failed":
                        job = {"key": key, "status": "queued", "error": None}
                        _DOWNLOAD_JOBS[key] = job
                    jobs.append(job)
                request_id = uuid4().hex
                request = {"client_id": client_id, "status": "queued", "symbols": symbols,
                           "period": period, "start_time": start_time, "end_time": end_time,
                           "error": None, "jobs": jobs,
                           "created_at": datetime.now(timezone.utc).isoformat(),
                           "updated_at": datetime.now(timezone.utc).isoformat(), "finished_at": None}
                _DOWNLOADS[request_id] = request
                _DOWNLOAD_QUEUE.setdefault(client_id, deque()).append(request_id)
                update_downloads()
                payload = download_payload(request_id, request)
        _DOWNLOAD_WAKE.set()
        self.set_status(202)
        self.write(json.dumps(payload, ensure_ascii=False))

    def get(self):
        request_id = self.get_argument("id", "")
        client_id = self.get_argument("client_id", "")
        if bool(request_id) == bool(client_id):
            raise HTTPError(400, "provide exactly one of id or client_id")
        with _DOWNLOAD_LOCK:
            if client_id:
                validate_client_id(client_id)
                payload = {"client_id": client_id, "requests": [
                    dict(id=key, client_id=value["client_id"], period=value["period"],
                         start_time=value["start_time"], end_time=value["end_time"],
                         status=value["status"], error=value["error"],
                         created_at=value["created_at"], updated_at=value["updated_at"],
                         finished_at=value["finished_at"], symbol_count=len(value["symbols"]),
                         progress=download_payload(key, value)["progress"])
                    for key, value in _DOWNLOADS.items() if value["client_id"] == client_id]}
            else:
                if not re.fullmatch(r"[0-9a-f]{32}", request_id):
                    raise HTTPError(400, "id must be the download id returned by POST /api/data/download")
                request = _DOWNLOADS.get(request_id)
                if request is None:
                    raise HTTPError(404, "download not found (expired or bridge restarted)")
                payload = download_payload(request_id, request)
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
        count = integer(data.get('count', '-1'), 'count')
        fields_list = parse_list(fields)
        stock_list = parse_list(stock_code)
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
        count = integer(data.get('count', '-1'), 'count')
        dividend_type = data.get('dividend_type', 'follow')
        fields_list = parse_list(fields)
        stock_list = parse_list(stock_code)
        ret = safe_call(self.ctx().get_market_data_ex, fields_list, stock_list, period, start_time, end_time, count, dividend_type, False, False)
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
        code_list = parse_list(stocks)
        ret = safe_call(self.ctx().get_full_tick, code_list)
        if not ret:
            raise HTTPError(500, "获取分笔行情失败")
        self.write(json.dumps(ret, ensure_ascii=False, default=str))

# ContextInfo.get_divid_factors() - 获取除权除息和复权因子
class DividFactorsHandler(BaseHandler):
    def post(self):
        data = self.json_body()
        stockcode = data.get('stockcode', '')
        if not stockcode:
            raise HTTPError(400, "need args stockcode")
        ret = safe_call(self.ctx().get_divid_factors, stockcode)
        # Never turn a failed QMT call into a successful no-event response.
        if ret is None:
            raise HTTPError(500, "get_divid_factors failed")
        self.write(json.dumps({"stockcode": stockcode, "factors": to_jsonable(ret), "fetched": True}, ensure_ascii=False))

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
        timetag = integer(data.get('timetag', '0'), 'timetag')
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
        count_int = integer(count, 'count') if count != '' else -1
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
        slist = parse_list(stock_list)
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
        slist = parse_list(stock_list)
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
        slist = parse_list(stock_list)
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
        barpos = integer(data.get('barpos', '-1'), 'barpos')
        if tabname and colname and market and code:
            ret = safe_call(self.ctx().get_financial_data, tabname, colname, market, code, report_type, barpos)
        else:
            field_list = data.get('fieldList', '')
            stock_list = data.get('stockList', '')
            start_date = data.get('startDate', '')
            end_date = data.get('endDate', '')
            fields = parse_list(field_list)
            stocks = parse_list(stock_list)
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
        fields = parse_list(field_list)
        if stock_code:
            ret = safe_call(self.ctx().get_factor_data, fields, stock_code, start_date, end_date)
        else:
            stocks = parse_list(stock_list)
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
        isavailable = parse_bool(data.get('isavailable', True))
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
        strikePrice = number(data.get('strikePrice', '0'), 'strikePrice')
        riskFree = number(data.get('riskFree', '0'), 'riskFree')
        sigma = number(data.get('sigma', '0'), 'sigma')
        days = integer(data.get('days', '0'), 'days')
        dividend = number(data.get('dividend', '0'), 'dividend')
        if isinstance(objectPrices, list) or isinstance(objectPrices, str) and ',' in objectPrices:
            values = objectPrices if isinstance(objectPrices, list) else objectPrices.split(',')
            op = [number(value, 'objectPrices') for value in values]
        else:
            op = number(objectPrices, 'objectPrices')
        ret = safe_call(self.ctx().bsm_price, optionType, op, strikePrice, riskFree, sigma, days, dividend)
        self.write(json.dumps({"price": ret}, ensure_ascii=False, default=str))

# ContextInfo.bsm_iv() - BS模型计算欧式期权隐含波动率
class BsmIvHandler(BaseHandler):
    def post(self):
        data = self.json_body()
        optionType = data.get('optionType', 'C')
        objectPrices = number(data.get('objectPrices', '0'), 'objectPrices')
        strikePrice = number(data.get('strikePrice', '0'), 'strikePrice')
        optionPrice = number(data.get('optionPrice', '0'), 'optionPrice')
        riskFree = number(data.get('riskFree', '0'), 'riskFree')
        days = integer(data.get('days', '0'), 'days')
        dividend = number(data.get('dividend', '0'), 'dividend')
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
        count = integer(data.get('count', '-1'), 'count')
        ret = safe_call(self.ctx().get_local_data, stock_code, start_time, end_time, period, divid_type, count)
        if ret is None:
            raise HTTPError(500, "获取本地行情失败")
        self.write(json.dumps({"data": ret}, ensure_ascii=False, default=str))

# ContextInfo.subscribe_quote() - 订阅行情数据
class SubscribeQuoteHandler(BaseHandler):
    def post(self):
        raise HTTPError(409, "quotes are globally owned; use websocket configure universe")


class UnsubscribeQuoteHandler(BaseHandler):
    def post(self):
        raise HTTPError(409, "quotes are globally owned; use websocket configure universe")


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
        stocktypenum = integer(data.get('stocktypenum', '0'), 'stocktypenum')
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
            opType = integer(data['opType'], 'opType')
            orderType = integer(data.get('orderType', 1101), 'orderType')
            stock = data['stock']
            pr_type = integer(data.get('prType', 11), 'prType')
            price = number(data['price'], 'price')
            volume = number(data['volume'], 'volume')
            quickTrade = integer(data.get('quickTrade', 2), 'quickTrade')
            user_order_id = str(data.get('userOrderId', '')).strip()
            order_ref = passorder(opType, orderType, self.acc(), stock, pr_type, price, volume, 'qmt', quickTrade, user_order_id, self.ctx())
            self.write(json.dumps({
                "status": "success",
                "opType": opType,
                "stock": stock,
                "order_ref": str(order_ref) if order_ref else "unknown"
            }, ensure_ascii=False))
        except HTTPError:
            raise
        except Exception as e:
            logger.exception("passorder下单异常")
            raise HTTPError(400, f"下单失败: {str(e)}")

# algo_passorder() - 算法交易下单
class AlgoPassorderHandler(BaseHandler):
    def post(self):
        try:
            data = self.json_body()
            order_ref = algo_passorder(
                integer(data['opType'], 'opType'), integer(data.get('orderType', 1101), 'orderType'),
                self.acc(), data['stock'], integer(data.get('prType', -1), 'prType'),
                number(data['price'], 'price'), number(data['volume'], 'volume'),
                data.get('strategyName', ''), integer(data.get('quickTrade', 2), 'quickTrade'),
                data.get('userOrderId', ''), data.get('userOrderParam', {}),
                self.ctx()
            )
            self.write(json.dumps({"status": "success", "order_ref": str(order_ref) if order_ref else "unknown"}, ensure_ascii=False))
        except HTTPError:
            raise
        except Exception as e:
            logger.exception("algo_passorder异常")
            raise HTTPError(400, f"算法下单失败: {str(e)}")

# smart_algo_passorder() - 智能算法交易下单
class SmartAlgoPassorderHandler(BaseHandler):
    def post(self):
        try:
            data = self.json_body()
            order_ref = smart_algo_passorder(
                integer(data['opType'], 'opType'), integer(data.get('orderType', 1101), 'orderType'),
                self.acc(), data['stock'], integer(data.get('prType', -1), 'prType'),
                number(data['price'], 'price'), number(data['volume'], 'volume'),
                data['smartAlgoType'], integer(data.get('limitOverRate', 0), 'limitOverRate'),
                integer(data.get('minAmountPerOrder', 0), 'minAmountPerOrder'),
                data.get('startTime', ''), data.get('endTime', ''),
                self.ctx()
            )
            self.write(json.dumps({"status": "success", "order_ref": str(order_ref) if order_ref else "unknown"}, ensure_ascii=False))
        except HTTPError:
            raise
        except Exception as e:
            logger.exception("smart_algo_passorder异常")
            raise HTTPError(400, f"智能算法下单失败: {str(e)}")

# order_lots() - 指定手数交易
class OrderLotsHandler(BaseHandler):
    def post(self):
        try:
            data = self.json_body()
            order_lots(data['stock'], integer(data['lots'], 'lots'), data.get('style', 'LATEST'),
                       number(data.get('price', 0), 'price'), self.ctx(), data.get('accId', self.acc()))
            self.write(json.dumps({"status": "success", "action": "order_lots", "stock": data['stock']}, ensure_ascii=False))
        except HTTPError:
            raise
        except Exception as e:
            logger.exception("order_lots异常")
            raise HTTPError(400, f"下单失败: {str(e)}")

# order_value() - 指定价值交易
class OrderValueHandler(BaseHandler):
    def post(self):
        try:
            data = self.json_body()
            order_value(data['stock'], number(data['value'], 'value'), data.get('style', 'LATEST'),
                        number(data.get('price', 0), 'price'), self.ctx(), data.get('accId', self.acc()))
            self.write(json.dumps({"status": "success", "action": "order_value", "stock": data['stock']}, ensure_ascii=False))
        except HTTPError:
            raise
        except Exception as e:
            logger.exception("order_value异常")
            raise HTTPError(400, f"下单失败: {str(e)}")

# order_percent() - 指定比例交易
class OrderPercentHandler(BaseHandler):
    def post(self):
        try:
            data = self.json_body()
            order_percent(data['stock'], number(data['percent'], 'percent'), data.get('style', 'LATEST'),
                          number(data.get('price', 0), 'price'), self.ctx(), data.get('accId', self.acc()))
            self.write(json.dumps({"status": "success", "action": "order_percent", "stock": data['stock']}, ensure_ascii=False))
        except HTTPError:
            raise
        except Exception as e:
            logger.exception("order_percent异常")
            raise HTTPError(400, f"下单失败: {str(e)}")

# order_target_value() - 指定目标价值交易
class OrderTargetValueHandler(BaseHandler):
    def post(self):
        try:
            data = self.json_body()
            order_target_value(data['stock'], number(data['tar_value'], 'tar_value'), data.get('style', 'LATEST'),
                               number(data.get('price', 0), 'price'), self.ctx(), data.get('accId', self.acc()))
            self.write(json.dumps({"status": "success", "action": "order_target_value", "stock": data['stock']}, ensure_ascii=False))
        except HTTPError:
            raise
        except Exception as e:
            logger.exception("order_target_value异常")
            raise HTTPError(400, f"下单失败: {str(e)}")

# order_target_percent() - 指定目标比例交易
class OrderTargetPercentHandler(BaseHandler):
    def post(self):
        try:
            data = self.json_body()
            order_target_percent(data['stock'], number(data['tar_percent'], 'tar_percent'), data.get('style', 'LATEST'),
                                 number(data.get('price', 0), 'price'), self.ctx(), data.get('accId', self.acc()))
            self.write(json.dumps({"status": "success", "action": "order_target_percent", "stock": data['stock']}, ensure_ascii=False))
        except HTTPError:
            raise
        except Exception as e:
            logger.exception("order_target_percent异常")
            raise HTTPError(400, f"下单失败: {str(e)}")

# order_shares() - 指定股数交易
class OrderSharesHandler(BaseHandler):
    def post(self):
        try:
            data = self.json_body()
            order_shares(data['stock'], integer(data['shares'], 'shares'), data.get('style', 'LATEST'),
                         number(data.get('price', 0), 'price'), self.ctx(), data.get('accId', self.acc()))
            self.write(json.dumps({"status": "success", "action": "order_shares", "stock": data['stock']}, ensure_ascii=False))
        except HTTPError:
            raise
        except Exception as e:
            logger.exception("order_shares异常")
            raise HTTPError(400, f"下单失败: {str(e)}")


# ============= 5. 期货交易 =============
# buy_open() - 期货买入开仓
class FuturesBuyOpenHandler(BaseHandler):
    def post(self):
        try:
            data = self.json_body()
            buy_open(data['stock'], integer(data['amount'], 'amount'), data.get('style', 'LATEST'),
                     number(data.get('price', 0), 'price'), self.ctx(), data.get('accId', self.acc()))
            self.write(json.dumps({"status": "success", "action": "buy_open", "stock": data['stock']}, ensure_ascii=False))
        except HTTPError:
            raise
        except Exception as e:
            logger.exception("buy_open异常")
            raise HTTPError(400, f"期货买入开仓失败: {str(e)}")

# buy_close_tdayfirst() - 期货买入平仓(平今优先)
class FuturesBuyCloseTdayFirstHandler(BaseHandler):
    def post(self):
        try:
            data = self.json_body()
            buy_close_tdayfirst(data['stock'], integer(data['amount'], 'amount'), data.get('style', 'LATEST'),
                                number(data.get('price', 0), 'price'), self.ctx(), data.get('accId', self.acc()))
            self.write(json.dumps({"status": "success", "action": "buy_close_tdayfirst", "stock": data['stock']}, ensure_ascii=False))
        except HTTPError:
            raise
        except Exception as e:
            logger.exception("buy_close_tdayfirst异常")
            raise HTTPError(400, f"期货买入平仓(平今)失败: {str(e)}")

# buy_close_ydayfirst() - 期货买入平仓(平昨优先)
class FuturesBuyCloseYdayFirstHandler(BaseHandler):
    def post(self):
        try:
            data = self.json_body()
            buy_close_ydayfirst(data['stock'], integer(data['amount'], 'amount'), data.get('style', 'LATEST'),
                                number(data.get('price', 0), 'price'), self.ctx(), data.get('accId', self.acc()))
            self.write(json.dumps({"status": "success", "action": "buy_close_ydayfirst", "stock": data['stock']}, ensure_ascii=False))
        except HTTPError:
            raise
        except Exception as e:
            logger.exception("buy_close_ydayfirst异常")
            raise HTTPError(400, f"期货买入平仓(平昨)失败: {str(e)}")

# sell_open() - 期货卖出开仓
class FuturesSellOpenHandler(BaseHandler):
    def post(self):
        try:
            data = self.json_body()
            sell_open(data['stock'], integer(data['amount'], 'amount'), data.get('style', 'LATEST'),
                      number(data.get('price', 0), 'price'), self.ctx(), data.get('accId', self.acc()))
            self.write(json.dumps({"status": "success", "action": "sell_open", "stock": data['stock']}, ensure_ascii=False))
        except HTTPError:
            raise
        except Exception as e:
            logger.exception("sell_open异常")
            raise HTTPError(400, f"期货卖出开仓失败: {str(e)}")

# sell_close_tdayfirst() - 期货卖出平仓(平今优先)
class FuturesSellCloseTdayFirstHandler(BaseHandler):
    def post(self):
        try:
            data = self.json_body()
            sell_close_tdayfirst(data['stock'], integer(data['amount'], 'amount'), data.get('style', 'LATEST'),
                                 number(data.get('price', 0), 'price'), self.ctx(), data.get('accId', self.acc()))
            self.write(json.dumps({"status": "success", "action": "sell_close_tdayfirst", "stock": data['stock']}, ensure_ascii=False))
        except HTTPError:
            raise
        except Exception as e:
            logger.exception("sell_close_tdayfirst异常")
            raise HTTPError(400, f"期货卖出平仓(平今)失败: {str(e)}")

# sell_close_ydayfirst() - 期货卖出平仓(平昨优先)
class FuturesSellCloseYdayFirstHandler(BaseHandler):
    def post(self):
        try:
            data = self.json_body()
            sell_close_ydayfirst(data['stock'], integer(data['amount'], 'amount'), data.get('style', 'LATEST'),
                                 number(data.get('price', 0), 'price'), self.ctx(), data.get('accId', self.acc()))
            self.write(json.dumps({"status": "success", "action": "sell_close_ydayfirst", "stock": data['stock']}, ensure_ascii=False))
        except HTTPError:
            raise
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
        except HTTPError:
            raise
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
        except HTTPError:
            raise
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
        except HTTPError:
            raise
        except Exception as e:
            logger.exception("resume_task异常")
            raise HTTPError(400, f"继续任务失败: {str(e)}")

# do_order() - 实时触发前一根bar信号函数
class DoOrderHandler(BaseHandler):
    def post(self):
        try:
            do_order(self.ctx())
            self.write(json.dumps({"status": "success", "message": "信号已触发"}, ensure_ascii=False))
        except HTTPError:
            raise
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


class CancelOrderHandler(BaseHandler):
    def post(self):
        data = self.json_body()
        order_id = str(data.get('orderId', '')).strip()
        account_type = data.get('accountType', 'stock')
        if not order_id:
            raise HTTPError(400, "必须提供 orderId")
        try:
            if not can_cancel_order(order_id, self.acc(), account_type):
                raise HTTPError(409, "委托当前不可撤销")
            if not cancel(order_id, self.acc(), account_type, self.ctx()):
                raise HTTPError(409, "QMT 未接受撤单请求")
            self.write(json.dumps({"status": "success", "order_id": order_id}, ensure_ascii=False))
        except HTTPError:
            raise
        except Exception as e:
            logger.exception("按委托号撤单异常")
            raise HTTPError(500, f"撤单失败: {str(e)}")

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
        deviation = integer(data.get('deviation', '0'), 'deviation')
        ret = safe_call(ext_data, extdataname, stockcode, deviation, self.ctx())
        self.write(json.dumps({"extdataname": extdataname, "stockcode": stockcode, "value": ret}, ensure_ascii=False))

# ext_data_rank() - 获取扩展数据排名
class ExtDataRankHandler(BaseHandler):
    def post(self):
        data = self.json_body()
        extdataname = data.get('extdataname', '')
        stockcode = data.get('stockcode', '')
        deviation = integer(data.get('deviation', '0'), 'deviation')
        ret = safe_call(ext_data_rank, extdataname, stockcode, deviation, self.ctx())
        self.write(json.dumps({"extdataname": extdataname, "stockcode": stockcode, "rank": ret}, ensure_ascii=False))

# get_factor_value() - 获取因子数据
class GetFactorValueHandler(BaseHandler):
    def post(self):
        data = self.json_body()
        factorname = data.get('factorname', '')
        stockcode = data.get('stockcode', '')
        deviation = integer(data.get('deviation', '0'), 'deviation')
        ret = safe_call(get_factor_value, factorname, stockcode, deviation, self.ctx())
        self.write(json.dumps({"factorname": factorname, "stockcode": stockcode, "value": ret}, ensure_ascii=False))

# get_factor_rank() - 获取因子数据排名
class GetFactorRankHandler(BaseHandler):
    def post(self):
        data = self.json_body()
        factorname = data.get('factorname', '')
        stockcode = data.get('stockcode', '')
        deviation = integer(data.get('deviation', '0'), 'deviation')
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
            price = number(data['price'], 'price')
            volume = number(data['volume'], 'volume')
            pr_type = data.get('prType', 11)
            order_ref = passorder(23, 1101, self.acc(), stock, pr_type, price, volume, 'qmt', 2, self.ctx())
            self.write(json.dumps({
                "status": "success", "action": "buy", "stock": stock,
                "order_ref": str(order_ref) if order_ref else "unknown"
            }, ensure_ascii=False))
        except HTTPError:
            raise
        except Exception as e:
            logger.exception("买入下单异常")
            raise HTTPError(400, f"下单失败: {str(e)}")

# passorder(24) - 简化卖出下单(封装passorder)
class SellHandler(BaseHandler):
    def post(self):
        try:
            data = self.json_body()
            stock = data['stock']
            price = number(data['price'], 'price')
            volume = number(data['volume'], 'volume')
            pr_type = data.get('prType', 11)
            order_ref = passorder(24, 1101, self.acc(), stock, pr_type, price, volume, 'qmt', 2, self.ctx())
            self.write(json.dumps({
                "status": "success", "action": "sell", "stock": stock,
                "order_ref": str(order_ref) if order_ref else "unknown"
            }, ensure_ascii=False))
        except HTTPError:
            raise
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
        except HTTPError:
            raise
        except Exception as e:
            logger.exception("全部撤单异常")
            raise HTTPError(500, f"撤单失败: {str(e)}")


class CancelByRuleHandler(BaseHandler):
    def post(self):
        try:
            data = self.json_body()
            stock = data.get('stock')
            cancel_volume = number(data.get('volume', 0), 'volume')
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
        except HTTPError:
            raise
        except Exception as e:
            logger.exception("规则撤单异常")
            raise HTTPError(500, f"撤单失败: {str(e)}")

def sample_ioloop_lag():
    global _IOLOOP_LAST_TICK
    now = monotonic()
    if _IOLOOP_LAST_TICK is not None:
        _IOLOOP_LAG_SAMPLES.append((now, (now - _IOLOOP_LAST_TICK - 1.0) * 1000))
    _IOLOOP_LAST_TICK = now
    while _IOLOOP_LAG_SAMPLES and _IOLOOP_LAG_SAMPLES[0][0] < now - 60:
        _IOLOOP_LAG_SAMPLES.popleft()


class HealthHandler(BaseHandler):
    def get(self):
        now = monotonic()
        clients = {owner: {
            "client_id": owner, "addr": state["addr"],
            "state": "grace" if state["expires"] is not None else "connected",
            "expires_at": state["expires_at"], "symbols": len(state["universe"]),
            "realtime": sum(state["universe"].values()),
            "subscription_count": sum(state["universe"].get(stock, False) for stock, period in _REALTIME_SUBSCRIPTIONS),
            "streams": sorted(state["connection"].streams) if state["connection"] is not None else [],
            "queued": 0, "running": 0,
        } for owner, state in _UNIVERSE_CLIENTS.items()}
        for client in _WS_CLIENTS:
            clients.setdefault(client.client_id, {
                "client_id": client.client_id, "addr": client.request.remote_ip,
                "state": "connected", "expires_at": None, "symbols": 0, "realtime": 0,
                "subscription_count": 0, "streams": sorted(client.streams), "queued": 0, "running": 0,
            })
        downloads = {"queued": 0, "running": 0}
        with _DOWNLOAD_LOCK:
            for request in _DOWNLOADS.values():
                owner = request["client_id"]
                client = clients.setdefault(owner, {
                    "client_id": owner, "addr": None, "state": "disconnected", "expires_at": None,
                    "symbols": 0, "realtime": 0, "subscription_count": 0, "streams": [],
                    "queued": 0, "running": 0,
                })
                if request["status"] in downloads:
                    downloads[request["status"]] += 1
                    client[request["status"]] += 1
        self.write(json.dumps({
            "bridge_id": _BRIDGE_ID, "started_at": _STARTED_AT,
            "ioloop_lag_ms": {
                "latest": _IOLOOP_LAG_SAMPLES[-1][1] if _IOLOOP_LAG_SAMPLES else None,
                "max_60s": max((lag for sampled_at, lag in _IOLOOP_LAG_SAMPLES if sampled_at >= now - 60), default=None),
            },
            "realtime": {
                "last_push_at": _REALTIME_LAST_PUSH_AT, "pushed_symbol_count": _REALTIME_PUSHED_SYMBOL_COUNT,
                "symbol_count": sum(_UNIVERSE_SUBSCRIBED.values()),
                "subscription_count": len(_REALTIME_SUBSCRIPTIONS),
            },
            "universe_size": len(_UNIVERSE_SUBSCRIBED),
            "downloads": downloads, "clients": sorted(clients.values(), key=lambda client: client["client_id"]),
        }, ensure_ascii=False))


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
        (r"/api/data/download", DownloadHandler),
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
        (r"/api/trade/cancel_order", CancelOrderHandler),
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
        (r"/api/sys/health", HealthHandler),
        (r"/api/sys/python_version", PythonVersionHandler),
        (r"/api/sys/shutdown", ShutdownHandler),

    ], debug=False, compress_response=True, websocket_ping_interval=30, websocket_ping_timeout=30, websocket_max_message_size=_MESSAGE_LIMIT)


def _stop_server_on_ioloop(reason):
    global _SERVER
    for callback in (_REALTIME_PUSHER, _ACCOUNT_PUSHER, _HEALTH_SAMPLER):
        if callback is not None:
            callback.stop()
    if _APP is not None:
        for sub_id in list(_REALTIME_SUBSCRIPTIONS.values()):
            safe_call(_APP.ContextInfo.unsubscribe_quote, int(sub_id))
        _REALTIME_SUBSCRIPTIONS.clear()
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
    _DOWNLOAD_STOP.set()
    _DOWNLOAD_WAKE.set()
    try:
        if _IOLOOP is None:
            logger.info("QMT HTTP Server no active IOLoop; port 0.0.0.0:%s already released", PORT)
            return
        _IOLOOP.add_callback(_stop_server_on_ioloop, reason)
    except Exception as e:
        logger.exception("QMT HTTP Server release port failed: %s", e)


def init(ContextInfo):
    global _APP, _SERVER, _IOLOOP, _STOPPING, _REALTIME_PUSHER, _ACCOUNT_PUSHER
    global _HEALTH_SAMPLER, _IOLOOP_LAST_TICK
    loop = None
    try:
        for callback in (_REALTIME_PUSHER, _ACCOUNT_PUSHER, _HEALTH_SAMPLER):
            if callback is not None:
                callback.stop()
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
        _SERVER = HTTPServer(app, max_body_size=_MESSAGE_LIMIT, max_buffer_size=_WRITE_LIMIT)
        _SERVER.listen(PORT, address='0.0.0.0')
        start_download_worker()
        _IOLOOP_LAG_SAMPLES.clear()
        _HEALTH_SAMPLER = PeriodicCallback(sample_ioloop_lag, 1000)
        _IOLOOP_LAST_TICK = monotonic()
        _HEALTH_SAMPLER.start()
        _REALTIME_PUSHER = PeriodicCallback(push_global_market_data, 1000)
        _REALTIME_PUSHER.start()
        _ACCOUNT_PUSHER = PeriodicCallback(push_account_status, 60000)
        _ACCOUNT_PUSHER.start()
        _STOPPING = False
        logger.info(f"QMT HTTP Server 启动于 http://0.0.0.0:{PORT} (全部API已加载，模型线程运行)")
        loop.start()
        if not _STOPPING:
            logger.error("QMT HTTP IOLoop unexpectedly stopped")
    except Exception as e:
        logger.exception(f"server start failed: {e}")
    finally:
        for callback in (_REALTIME_PUSHER, _ACCOUNT_PUSHER, _HEALTH_SAMPLER):
            if callback is not None:
                callback.stop()
        _HEALTH_SAMPLER = None
        if _SERVER is not None:
            _SERVER.stop()
            _SERVER = None
        if loop is not None:
            loop.close(all_fds=True)
        if _IOLOOP is loop:
            _IOLOOP = None


def on_order_stock(order):
    """QMT 原生自动回调：委托状态变动通知"""
    if order is None:
        return
    try:
        data = to_jsonable(order)
        if not isinstance(data, dict):
            attrs = {}
            for attr in dir(order):
                if not attr.startswith('_'):
                    try:
                        val = getattr(order, attr)
                        if not callable(val):
                            attrs[attr] = to_jsonable(val)
                    except Exception:
                        pass
            data = attrs
        sys_id = getattr(order, "m_strOrderSysID", "") or data.get("m_strOrderSysID", "")
        status = getattr(order, "m_nOrderStatus", "") or data.get("m_nOrderStatus", "")
        logger.info("QMT on_order_stock received: sys_id=%s status=%s", sys_id, status)
        _queue_broadcast({"type": "order_update", "data": data}, "order")
    except Exception as e:
        logger.exception("QMT on_order_stock processing failed: %s", e)


def on_order_deal(deal):
    """QMT 原生自动回调：真实撮合成交回报"""
    if deal is None:
        return
    try:
        data = to_jsonable(deal)
        if not isinstance(data, dict):
            attrs = {}
            for attr in dir(deal):
                if not attr.startswith('_'):
                    try:
                        val = getattr(deal, attr)
                        if not callable(val):
                            attrs[attr] = to_jsonable(val)
                    except Exception:
                        pass
            data = attrs
        trade_id = getattr(deal, "m_strTradeID", "") or data.get("m_strTradeID", "")
        order_id = getattr(deal, "m_strOrderSysID", "") or data.get("m_strOrderSysID", "")
        price = getattr(deal, "m_dPrice", "") or data.get("m_dPrice", "")
        volume = getattr(deal, "m_nVolume", "") or data.get("m_nVolume", "")
        logger.info("QMT on_order_deal received: trade_id=%s order_id=%s price=%s volume=%s", trade_id, order_id, price, volume)
        _queue_broadcast({"type": "deal_update", "data": data}, "deal")
    except Exception as e:
        logger.exception("QMT on_order_deal processing failed: %s", e)


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
