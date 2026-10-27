# -*- coding: gbk -*-
import json
import locale
import os
import re
from collections import deque
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

# 明确时间范围可修复本地缓存缺口；不要用空 start_time 触发“从最后一条开始”的隐式增量下载。
BACKFILL_PERIOD_DAYS = {"1d": 180, "5m": 180, "1m": 10}
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
_BACKFILL_PENDING = set()
_BACKFILL_RESULTS = {}
_BACKFILL_RUNNING = set()
_BACKFILL_REQUESTS = {}
_BACKFILL_REQUEST_QUEUE = deque()
_BACKFILL_REQUEST_PAIRS = set()
_BACKFILL_LOCK = Lock()
_BACKFILL_REQUEST_LIMIT = 50
_BACKFILL_STOP = Event()
_BACKFILL_THREAD = None
_BACKFILL_NEXT_INCREMENTAL = 0.0
_BACKFILL_LAST_INCREMENTAL_DATE = ""
_UNIVERSE_SUBSCRIBED = {}
_QMT_REALTIME_SYMBOL_LIMIT = 50
_HISTORY_DOWNLOAD_BATCH_SIZE = 300
_HISTORY_MANIFESTS = []
_HISTORY_SEQUENCE = 0
_HISTORY_CLIENT_ACKS = {}
_HISTORY_MANIFEST_LIMIT = 128
_BRIDGE_ID = uuid4().hex
_STARTED_AT = datetime.now(timezone.utc).isoformat()
_UNIVERSE_CLIENTS = {}
_REALTIME_SUBSCRIPTIONS = {}
_REALTIME_RELEASE_AT = {}
_REALTIME_PUSHER = None
_ACCOUNT_PUSHER = None
_HEALTH_SAMPLER = None
_IOLOOP_LAST_TICK = None
_IOLOOP_LAG_SAMPLES = deque()
_REALTIME_LAST_PUSH_AT = None
_REALTIME_PUSHED_SYMBOL_COUNT = 0
_REALTIME_LAST_5M = 0.0
_DISCONNECT_GRACE_SECONDS = 120
# Some QMT editors omit __file__; the compiled model filename is a fallback at startup.
_SCRIPT_PATH = globals().get("__file__")
_BACKFILL_STATE_PATH = os.path.join(os.path.dirname(os.path.abspath(_SCRIPT_PATH)), "QMT_SERVICE_backfill.json") if _SCRIPT_PATH else None


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
    today = now.strftime("%Y%m%d")
    session_close = today + "150000"
    if start_time and len(str(start_time)) == 8:
        start_time = str(start_time) + "000000"
    if end_time and len(str(end_time)) == 8:
        end_time = str(end_time) + "235959"
    return (
        start_time or (now - timedelta(days=days)).strftime("%Y%m%d%H%M%S"),
        min(end_time or session_close, now.strftime("%Y%m%d%H%M%S"), session_close),
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


def trading_hours(now=None):
    now = (now or datetime.now(_QMT_TIMEZONE)).astimezone(_QMT_TIMEZONE)
    return now.weekday() < 5 and "0915" <= now.strftime("%H%M") <= "1505"


def load_backfill_results():
    if not _BACKFILL_STATE_PATH:
        logger.warning("QMT script filename unavailable; backfill persistence disabled")
        return
    try:
        with open(_BACKFILL_STATE_PATH, "r", encoding="utf-8") as stream:
            results = json.load(stream)
        if not isinstance(results, dict) or any(
            not isinstance(key, str) or not isinstance(value, dict)
            or value.get("status") != "success"
            or not re.fullmatch(r"\d{14}", str(value.get("start_time", "")))
            or not re.fullmatch(r"\d{14}", str(value.get("end_time", "")))
            for key, value in results.items()
        ):
            raise ValueError("invalid backfill state")
        _BACKFILL_RESULTS.update(results)
        logger.info("QMT backfill state loaded: results=%s", len(results))
    except FileNotFoundError:
        pass
    except (OSError, ValueError):
        logger.exception("QMT backfill state unavailable; bounded repair required")


def save_backfill_results():
    if not _BACKFILL_STATE_PATH:
        return
    # Called with _BACKFILL_LOCK held; atomic replacement also preserves the old file on failure.
    temporary = _BACKFILL_STATE_PATH + ".tmp"
    try:
        with open(temporary, "w", encoding="utf-8") as stream:
            json.dump({key: value for key, value in _BACKFILL_RESULTS.items() if value.get("status") == "success"}, stream)
        os.replace(temporary, _BACKFILL_STATE_PATH)
    except OSError:
        logger.exception("QMT backfill state save failed")


def _queue_broadcast(payload, *, history=False, factors=False):
    loop = _IOLOOP
    if loop is None:
        return
    loop.add_callback(_broadcast, payload, history, factors)


def _broadcast(payload, history=False, factors=False):
    for client in list(_WS_CLIENTS):
        if history:
            manifest = history_for_client(payload, client)
            if manifest is not None:
                client.send_json(manifest)
            continue
        if factors and not getattr(client, "wants_factors", False):
            continue
        client.send_json(payload)


def history_for_client(manifest, client):
    if not getattr(client, "wants_history", False):
        return None
    if _UNIVERSE_CLIENTS.get(client.client_id, {}).get("connection") is not client:
        return None
    if manifest["id"] in _HISTORY_CLIENT_ACKS.get(client.client_id, set()):
        return None
    stocks = [stock for stock in manifest["symbols"] if stock in client.universe]
    return dict(manifest, symbols=stocks) if stocks else None


def _announce_history(stocks, period, start_time, end_time, incremental):
    global _HISTORY_SEQUENCE
    # Factor synchronization uses the existing HTTP endpoint on the model thread,
    # not redundant background ContextInfo calls or a large IOLoop callback.
    stocks = sorted(set(stocks))
    with _BACKFILL_LOCK:
        matching = [item for item in _HISTORY_MANIFESTS if item["period"] == period and item["symbols"] == stocks]
        for item in matching:
            start_time = min(start_time, item["start_time"])
            end_time = max(end_time, item["end_time"])
            incremental = incremental and item["incremental"]
        _HISTORY_MANIFESTS[:] = [item for item in _HISTORY_MANIFESTS if item not in matching]
        # A long-lived bridge must not grow a merged repair beyond its retained window.
        retained_start = datetime.strptime(end_time, "%Y%m%d%H%M%S") - timedelta(days=BACKFILL_PERIOD_DAYS[period])
        start_time = max(start_time, retained_start.strftime("%Y%m%d%H%M%S"))
        _HISTORY_SEQUENCE += 1
        manifest = {
            "type": "history_ready",
            "bridge_id": _BRIDGE_ID,
            "id": str(_HISTORY_SEQUENCE),
            "period": period,
            "symbols": stocks,
            "start_time": start_time,
            "end_time": end_time,
            "incremental": incremental,
            "count": -1,
        }
        _HISTORY_MANIFESTS.append(manifest)
        if len(_HISTORY_MANIFESTS) > _HISTORY_MANIFEST_LIMIT:
            # shortcut: keep bounded hints, reconstruct evicted full windows from download state on configure.
            logger.warning("QMT history manifest limit reached; evicted hints will be rebuilt on configure")
            del _HISTORY_MANIFESTS[:-_HISTORY_MANIFEST_LIMIT]
        retained = {item["id"] for item in _HISTORY_MANIFESTS}
        for acked in _HISTORY_CLIENT_ACKS.values():
            acked.intersection_update(retained)
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


def parse_universe(rows):
    if not isinstance(rows, list):
        raise ValueError("universe must be a list of {symbol, subscribed} objects")
    desired = {}
    for row in rows:
        if not isinstance(row, dict) or set(row) != {"symbol", "subscribed"}:
            raise ValueError("each universe row must contain only symbol and subscribed")
        symbol = row["symbol"]
        if not isinstance(symbol, str) or not symbol.strip() or type(row["subscribed"]) is not bool:
            raise ValueError("universe symbol must be non-empty and subscribed must be boolean")
        desired[symbol.upper().strip()] = row["subscribed"]
    if sum(desired.values()) > _QMT_REALTIME_SYMBOL_LIMIT:
        raise ValueError("universe exceeds the 50-symbol realtime subscription limit")
    return desired


def set_universe_targets(desired, refresh=False):
    queued = 0
    reused = {}
    with _BACKFILL_LOCK:
        previous = dict(_UNIVERSE_SUBSCRIBED)
        _UNIVERSE_SUBSCRIBED.clear()
        _UNIVERSE_SUBSCRIBED.update(desired)
        added = set(desired) if refresh else set(desired) - set(previous)
        promoted = {stock for stock, subscribed in desired.items() if subscribed and not previous.get(stock, False)}
        for stock in sorted(added | promoted, key=lambda symbol: (not desired[symbol], symbol)):
            periods = (_BACKGROUND_BACKFILL_PERIODS if stock in added else ()) + (("1m",) if desired[stock] else ())
            for period in periods:
                if refresh:
                    _BACKFILL_RESULTS.pop(stock + ":" + period, None)
                item = (stock, period)
                if not refresh and backfill_result_current(stock, period):
                    reused.setdefault(period, []).append(stock)
                    continue
                if item not in _BACKFILL_PENDING and item not in _BACKFILL_RUNNING:
                    _BACKFILL_PENDING.add(item)
                    queued += 1
        if refresh:
            save_backfill_results()
    for period, stocks in reused.items():
        start_time, end_time = backfill_range(period)
        _announce_history(stocks, period, start_time, end_time, False)
    return queued


def has_backfill_window(stock, period):
    result = _BACKFILL_RESULTS.get(stock + ":" + period) or {}
    start_time = backfill_range(period)[0]
    return result.get("status") == "success" and str(result.get("start_time", "99999999999999")) <= start_time <= str(result.get("end_time", ""))


def backfill_result_current(stock, period):
    now = datetime.now(_QMT_TIMEZONE)
    completed = now
    if now.weekday() >= 5 or now.strftime("%H%M") < "1510":
        completed -= timedelta(days=1)
        while completed.weekday() >= 5:
            completed -= timedelta(days=1)
    result = _BACKFILL_RESULTS.get(stock + ":" + period) or {}
    return has_backfill_window(stock, period) and str(result.get("end_time", "")) >= completed.strftime("%Y%m%d") + "150000"


def merged_universe(clients):
    desired = {}
    for client in clients.values():
        for stock, subscribed in client["universe"].items():
            desired[stock] = desired.get(stock, False) or subscribed
    return desired


def set_client_universe(owner, universe, connection=None, refresh=False):
    clients = dict(_UNIVERSE_CLIENTS)
    clients[owner] = {
        "universe": dict(universe), "connection": connection, "expires": None,
        "addr": connection.request.remote_ip if connection is not None else None,
        "expires_at": None,
    }
    desired = merged_universe(clients)
    occupied = {stock for stock, period in _REALTIME_SUBSCRIPTIONS} | {stock for stock, enabled in desired.items() if enabled}
    if len(occupied) > _QMT_REALTIME_SYMBOL_LIMIT:
        raise ValueError("combined universe exceeds the 50-symbol realtime subscription limit (including unsubscribe/disconnect grace)")
    previous_state = _UNIVERSE_CLIENTS.get(owner, {})
    previous = previous_state.get("connection")
    newly_needed = set(universe) - set(previous_state.get("universe", {}))
    existing = set(_UNIVERSE_SUBSCRIBED)
    _UNIVERSE_CLIENTS.clear()
    _UNIVERSE_CLIENTS.update(clients)
    if previous is not None and previous is not connection:
        previous.close(code=4000, reason="client_id reconnected")
    queued = set_universe_targets(desired, refresh)
    # An expanded client scope needs a new manifest ID even if it acked an earlier
    # filtered view of the same archive. Reuse local data, not another download.
    if not refresh:
        with _BACKFILL_LOCK:
            acked = _HISTORY_CLIENT_ACKS.get(owner, set())
            expanded = {stock for item in _HISTORY_MANIFESTS if item["id"] in acked for stock in item["symbols"]} & newly_needed & existing
        for period in _BACKGROUND_BACKFILL_PERIODS + ("1m",):
            stocks = sorted(stock for stock in expanded if (period != "1m" or universe[stock]) and has_backfill_window(stock, period))
            if stocks:
                start_time, end_time = backfill_range(period)
                _announce_history(stocks, period, start_time, end_time, False)
    return queued


def ensure_history_manifests(universe):
    # Download completion and client persistence are independent. A restart or
    # bounded manifest eviction must still expose the whole local retained window.
    for period in _BACKGROUND_BACKFILL_PERIODS + ("1m",):
        start_time, end_time = backfill_range(period)
        with _BACKFILL_LOCK:
            covered = {stock for item in _HISTORY_MANIFESTS if item["period"] == period and not item["incremental"] and item["start_time"] <= start_time for stock in item["symbols"]}
            missing = sorted(stock for stock, enabled in universe.items() if (period != "1m" or enabled) and stock not in covered and has_backfill_window(stock, period))
        for offset in range(0, len(missing), _HISTORY_DOWNLOAD_BATCH_SIZE):
            _announce_history(missing[offset:offset + _HISTORY_DOWNLOAD_BATCH_SIZE], period, start_time, end_time, False)


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
        with _BACKFILL_LOCK:
            _HISTORY_CLIENT_ACKS.pop(owner, None)
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
    global _REALTIME_LAST_5M, _REALTIME_LAST_PUSH_AT, _REALTIME_PUSHED_SYMBOL_COUNT
    pushed = set()
    expire_universe_clients()
    ctx = _APP.ContextInfo
    refresh_global_subscriptions(ctx)
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
                if client.send_json({"type": "bar", "period": period, "stocks": sorted(selected), "fields": list(_QMT_FIELDS), "count": 2, "data": selected}):
                    pushed.update(selected)
    if pushed:
        # These writes were queued, not acknowledged by the receiving clients.
        _REALTIME_LAST_PUSH_AT = datetime.now(timezone.utc).isoformat()
        _REALTIME_PUSHED_SYMBOL_COUNT = len(pushed)


def set_backfill_targets(stocks, refresh=False, owner="http"):
    return set_client_universe(owner, {stock.upper(): False for stock in parse_list(stocks)}, refresh=refresh)


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
            for stock in sorted(_UNIVERSE_SUBSCRIBED)
            for period in _BACKGROUND_BACKFILL_PERIODS + (("1m",) if _UNIVERSE_SUBSCRIBED[stock] else ())
            if not backfill_result_current(stock, period)
        ]
        if not missing:
            _BACKFILL_LAST_INCREMENTAL_DATE = today
            return
        _BACKFILL_PENDING.update(missing)
        _BACKFILL_NEXT_INCREMENTAL = monotonic() + INCREMENTAL_INTERVAL_SECONDS
        queued = len(_BACKFILL_PENDING)
    if queued:
        logger.info("QMT intraday incremental backfill queued: tasks=%s", queued)


def take_backfill_batch():
    with _BACKFILL_LOCK:
        _BACKFILL_PENDING.intersection_update({
            item for item in _BACKFILL_PENDING
            if item[0] in _UNIVERSE_SUBSCRIBED and (item[1] != "1m" or _UNIVERSE_SUBSCRIBED[item[0]])
        })
        intraday = trading_hours()
        eligible = [item for item in _BACKFILL_PENDING - _BACKFILL_RUNNING - _BACKFILL_REQUEST_PAIRS if not intraday or item[1] == "1m"]
        if not eligible:
            return []
        eligible.sort(key=lambda item: ({"1d": 0, "1m": 1, "5m": 2}[item[1]], not _UNIVERSE_SUBSCRIBED[item[0]], item[0]))
        period = eligible[0][1]
        incremental = has_backfill_window(eligible[0][0], period)
        batch = [item for item in eligible if item[1] == period and has_backfill_window(item[0], period) == incremental][:(10 if intraday else _HISTORY_DOWNLOAD_BATCH_SIZE)]
        _BACKFILL_PENDING.difference_update(batch)
        _BACKFILL_RUNNING.update(batch)
        return batch


def take_backfill_request():
    with _BACKFILL_LOCK:
        while _BACKFILL_REQUEST_QUEUE:
            request_id = _BACKFILL_REQUEST_QUEUE.popleft()
            request = _BACKFILL_REQUESTS.get(request_id)
            if request and request["status"] == "queued":
                _BACKFILL_RUNNING.update((symbol, request["period"]) for symbol in request["symbols"])
                request["status"] = "running"
                for result in request["results"].values():
                    result["status"] = "running"
                return request_id, request
    return None


def run_backfill_request(request_id, request):
    try:
        download_history_range(request["symbols"], request["period"], request["start_time"], request["end_time"])
        with _BACKFILL_LOCK:
            for result in request["results"].values():
                result.update(status="success", error=None)
            request["status"] = "success"
    except Exception as exc:
        logger.exception("QMT requested backfill failed: id=%s", request_id)
        with _BACKFILL_LOCK:
            for result in request["results"].values():
                result.update(status="failed", error=str(exc))
            request["status"] = "failed"
    finally:
        with _BACKFILL_LOCK:
            pairs = {(symbol, request["period"]) for symbol in request["symbols"]}
            _BACKFILL_RUNNING.difference_update(pairs)
            _BACKFILL_REQUEST_PAIRS.difference_update(pairs)


def backfill_worker():
    while not _BACKFILL_STOP.is_set():
        schedule_incremental_backfill()
        request = take_backfill_request()
        if request is not None:
            run_backfill_request(*request)
            continue
        batch = take_backfill_batch()
        if not batch:
            _BACKFILL_STOP.wait(0.5)
            continue
        period = batch[0][1]
        stocks = sorted({item[0] for item in batch})
        try:
            start_time, end_time = backfill_range(period)
            with _BACKFILL_LOCK:
                previous = [_BACKFILL_RESULTS.get(stock + ":" + period) or {} for stock in stocks]
            incremental = all(has_backfill_window(stock, period) for stock in stocks)
            if incremental:
                # Explicit overlap repairs partial bars; never ask QMT to infer an empty-start download.
                earliest = min(datetime.strptime(result["end_time"], "%Y%m%d%H%M%S") for result in previous)
                start_time = max(start_time, (earliest - timedelta(days=7)).strftime("%Y%m%d%H%M%S"))
            logger.info(
                "QMT history backfill started: %s symbols=%s mode=%s range=%s..%s",
                period, len(stocks), "incremental" if incremental else "bounded", start_time, end_time,
            )
            # Recheck after selecting a batch: an opening boundary must defer archive downloads.
            if trading_hours() and period != "1m":
                with _BACKFILL_LOCK:
                    _BACKFILL_PENDING.update(batch)
                    _BACKFILL_RUNNING.difference_update(batch)
                continue
            download_history_range(stocks, period, start_time, end_time)
            _announce_history(stocks, period, start_time, end_time, incremental)
            result = {"status": "success", "period": period, "trade_date": datetime.now(_QMT_TIMEZONE).strftime("%Y%m%d"), "start_time": start_time, "end_time": end_time, "mode": "incremental" if incremental else "bounded_incremental", "bars": None}
            logger.info("QMT history backfill complete: %s symbols=%s mode=%s", period, len(stocks), result["mode"])
            results = {stock + ":" + period: dict(result, **completed_download_range(old, start_time, end_time)) for stock, old in zip(stocks, previous)}
        except Exception as e:
            logger.exception("QMT history backfill failed: %s symbols=%s", period, len(stocks))
            with _BACKFILL_LOCK:
                results = {
                    stock + ":" + period: dict(_BACKFILL_RESULTS[stock + ":" + period], last_error=str(e))
                    if (_BACKFILL_RESULTS.get(stock + ":" + period) or {}).get("status") == "success"
                    else {"status": "failed", "period": period, "error": str(e)}
                    for stock in stocks
                }
        with _BACKFILL_LOCK:
            _BACKFILL_RESULTS.update(results)
            _BACKFILL_RUNNING.difference_update(batch)
            save_backfill_results()


def start_backfill_worker():
    global _BACKFILL_THREAD, _BACKFILL_STATE_PATH
    if _BACKFILL_THREAD and _BACKFILL_THREAD.is_alive():
        return
    if _BACKFILL_STATE_PATH is None:
        filename = start_backfill_worker.__code__.co_filename
        if filename and not filename.startswith("<"):
            _BACKFILL_STATE_PATH = os.path.join(os.path.dirname(os.path.abspath(filename)), "QMT_SERVICE_backfill.json")
    logger.info("QMT backfill state path: %s", _BACKFILL_STATE_PATH)
    load_backfill_results()
    _BACKFILL_STOP.clear()
    _BACKFILL_THREAD = Thread(target=backfill_worker, name="qmt-history-backfill", daemon=True)
    _BACKFILL_THREAD.start()


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
    payload = account_status_payload()
    if payload is not None:
        _broadcast({"type": "account_status", "data": payload})


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
        self.universe = {}
        self.client_id = "ws:%s" % id(self)
        self.wants_history = False
        self.wants_factors = False

    def check_origin(self, origin):
        return True

    def open(self):
        token = self.get_argument("token", None) or self.request.headers.get("X-Token")
        if token != TOKEN:
            self.close(code=4001, reason="认证失败：token 无效或缺失")
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

        if not isinstance(data, dict):
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
            push_account_status()
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
            self.send_json({"type": "error", "message": "unknown action: %s" % action})

    def handle_configure(self, data):
        try:
            universe = parse_universe(data.get("universe"))
            client_id = data.get("client_id", self.client_id)
            if not isinstance(client_id, str) or not client_id.strip() or len(client_id) > 1024 or any(ord(char) < 32 for char in client_id):
                raise ValueError("client_id must be a non-empty string of at most 1024 characters")
            if self.wants_history and client_id != self.client_id:
                raise ValueError("client_id cannot change on a configured connection")
            queued = set_client_universe(client_id, universe, self, refresh=parse_bool(data.get("refresh_history")))
        except ValueError as exc:
            self.send_json({"type": "error", "message": str(exc)})
            return
        self.client_id = client_id
        self.universe = universe
        self.wants_history = True
        self.wants_factors = True
        ensure_history_manifests(self.universe)
        logger.info(
            "QMT universe configured: symbols=%s realtime=%s backfill_tasks=%s addr=%s client_id=%s global_symbols=%s global_realtime=%s clients=%s",
            len(self.universe), sum(self.universe.values()), queued, self.request.remote_ip, self.client_id,
            len(_UNIVERSE_SUBSCRIBED), sum(_UNIVERSE_SUBSCRIBED.values()), len(_UNIVERSE_CLIENTS),
        )
        self.refresh_subscriptions()
        self.send_json({
            "type": "configured",
            "symbols": len(self.universe),
            "queued": queued,
            "realtime": sum(self.universe.values()),
            "bridge_id": _BRIDGE_ID,
            "global_symbols": len(_UNIVERSE_SUBSCRIBED),
            "global_realtime": sum(_UNIVERSE_SUBSCRIBED.values()),
        })
        with _BACKFILL_LOCK:
            manifests = list(_HISTORY_MANIFESTS)
        for manifest in manifests:
            selected = history_for_client(manifest, self)
            if selected is not None:
                self.send_json(selected)

    def handle_history_ack(self, data):
        manifest_id = str(data.get("id", ""))
        if data.get("bridge_id", _BRIDGE_ID) != _BRIDGE_ID:
            self.send_json({"type": "error", "message": "history_ack bridge_id mismatch"})
            return
        with _BACKFILL_LOCK:
            if any(item["id"] == manifest_id for item in _HISTORY_MANIFESTS):
                _HISTORY_CLIENT_ACKS.setdefault(self.client_id, set()).add(manifest_id)
        self.send_json({"type": "history_acked", "bridge_id": _BRIDGE_ID, "id": manifest_id})

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
            future = self.write_message(json.dumps(data, ensure_ascii=False, default=str))
            if future is not None:
                future.add_done_callback(self._consume_write_result)
            return True
        except Exception as e:
            logger.debug("websocket write skipped: %s", e)
            return False

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

def completed_download_range(previous, start_time, end_time):
    if previous.get("status") == "success" and previous.get("start_time", "99999999999999") <= end_time and start_time <= previous.get("end_time", ""):
        start_time = min(start_time, previous["start_time"])
        end_time = max(end_time, previous["end_time"])
    return {"start_time": start_time, "end_time": end_time}


def download_history_page(stocks, period, start_time, end_time):
    # HTTP repairs share the worker's pair reservations and persisted range cache.
    with _BACKFILL_LOCK:
        needed = [stock for stock in stocks if not (
            (_BACKFILL_RESULTS.get(stock + ":" + period) or {}).get("status") == "success"
            and (_BACKFILL_RESULTS[stock + ":" + period].get("start_time") or "99999999999999") <= start_time
            and _BACKFILL_RESULTS[stock + ":" + period]["end_time"] >= end_time
        )]
        batch = {(stock, period) for stock in needed}
        if batch & (_BACKFILL_RUNNING | _BACKFILL_REQUEST_PAIRS):
            raise HTTPError(503, "history download already queued or running; retry local reads later")
        _BACKFILL_RUNNING.update(batch)
        _BACKFILL_PENDING.difference_update(batch)
        previous = [_BACKFILL_RESULTS.get(stock + ":" + period) or {} for stock in needed]
        # Join an older cached range through the intervening gap, never claim
        # continuous coverage by merely taking the min/max of disjoint downloads.
        download_start = min([start_time] + [item["end_time"] for item in previous if item.get("status") == "success"])
        download_end = max([end_time] + [item["start_time"] for item in previous if item.get("status") == "success"])
    if not needed:
        return
    failed = False
    try:
        download_history_range(needed, period, download_start, download_end)
        with _BACKFILL_LOCK:
            for stock in needed:
                previous = _BACKFILL_RESULTS.get(stock + ":" + period) or {}
                _BACKFILL_RESULTS[stock + ":" + period] = dict(status="success", **completed_download_range(previous, download_start, download_end))
            save_backfill_results()
        _announce_history(needed, period, start_time, end_time, False)
    except Exception:
        failed = True
        raise
    finally:
        with _BACKFILL_LOCK:
            _BACKFILL_RUNNING.difference_update(batch)
            # Release and retry atomically: a worker must never inherit our reservation
            # between requeue and cleanup. Partial HTTP repairs still owe the archive.
            _BACKFILL_PENDING.update(item for item in batch if item[0] in _UNIVERSE_SUBSCRIBED and (failed or not backfill_result_current(*item)))


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
        count = int(data.get('count', data.get('len', '-1')))
        dividend_type = data.get('dividend_type', data.get('dividendType', 'none'))
        fields = data.get('fields', data.get('field', 'open,high,low,close,volume'))
        fields_list = parse_list(fields)
        do_download = parse_bool(data.get('download', True), True)
        if do_download and trading_hours():
            self.set_header("Retry-After", "60")
            raise HTTPError(503, "history downloads are deferred during 09:15-15:05 Beijing weekdays; use download=false for local reads")
        validate_history_page(stock_list, period, start_time, end_time, count)

        download_results = {}
        if do_download:
            if period not in BACKFILL_PERIOD_DAYS:
                raise HTTPError(400, "history download only supports 1d and 5m")
            start_time, end_time = backfill_range(period, start_time, end_time)
            if start_time > end_time:
                raise HTTPError(400, "history download range has no elapsed bars")
            try:
                download_history_page(stock_list, period, start_time, end_time)
                download_results = {stock: "success" for stock in stock_list}
            except HTTPError:
                raise
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
        queued = set_backfill_targets(stocks, parse_bool(data.get('refresh', False)), owner="http:" + self.request.remote_ip)
        logger.info("QMT HTTP backfill configured: addr=%s symbols=%s global_symbols=%s global_realtime=%s queued=%s", self.request.remote_ip, len(parse_list(stocks)), len(_UNIVERSE_SUBSCRIBED), sum(_UNIVERSE_SUBSCRIBED.values()), queued)
        self.set_status(202)
        self.write(json.dumps({
            "status": "accepted",
            "symbols": len(parse_list(stocks)),
            "queued": queued,
        }, ensure_ascii=False))


def validate_backfill_request(data):
    symbols = data.get("symbols")
    if not isinstance(symbols, list) or not symbols:
        raise ValueError("symbols must be a non-empty list of CN stock codes")
    stock_pattern = r"(?:(?:60|68)[0-9]{4}\.SH|(?:00|30)[0-9]{4}\.SZ|(?:[48][0-9]{5}|92[0-9]{4})\.BJ)"
    if any(not isinstance(symbol, str) or not re.fullmatch(stock_pattern, symbol.upper()) for symbol in symbols):
        raise ValueError("symbols must be CN stock codes such as 600000.SH")
    symbols = sorted(set(symbol.upper() for symbol in symbols))
    period = data.get("period")
    if period not in ("1m", "5m"):
        raise ValueError("period must be 1m or 5m")
    start_time, end_time = data.get("start_time"), data.get("end_time")
    try:
        if not isinstance(start_time, str) or not re.fullmatch(r"[0-9]{14}", start_time):
            raise ValueError
        if not isinstance(end_time, str) or not re.fullmatch(r"[0-9]{14}", end_time):
            raise ValueError
        start = datetime.strptime(start_time, "%Y%m%d%H%M%S")
        end = datetime.strptime(end_time, "%Y%m%d%H%M%S")
    except ValueError:
        raise ValueError("start_time and end_time must be valid YYYYMMDDHHMMSS timestamps")
    now = datetime.now(_QMT_TIMEZONE)
    today = now.strftime("%Y%m%d")
    if start.strftime("%Y%m%d") != today or end.strftime("%Y%m%d") != today or end < start:
        raise ValueError("start_time and end_time must be an ordered range on Beijing today")
    if end > now.replace(tzinfo=None):
        raise ValueError("end_time must not be in the future")
    return symbols, period, start_time, end_time


def backfill_request_payload(request_id, request):
    return {"id": request_id, "status": request["status"], "symbols": request["symbols"],
            "period": request["period"], "start_time": request["start_time"],
            "end_time": request["end_time"], "results": request["results"]}


class BackfillRequestHandler(BaseHandler):
    def post(self):
        try:
            symbols, period, start_time, end_time = validate_backfill_request(self.json_body())
        except ValueError as exc:
            raise HTTPError(400, str(exc))
        with _BACKFILL_LOCK:
            completed = [request_id for request_id, request in _BACKFILL_REQUESTS.items()
                         if request["status"] in ("success", "failed")]
            for request_id in completed[:-200]:
                del _BACKFILL_REQUESTS[request_id]
            for request_id, request in _BACKFILL_REQUESTS.items():
                if (request["status"] in ("queued", "running") and request["symbols"] == symbols
                        and request["period"] == period and request["start_time"] == start_time
                        and request["end_time"] == end_time):
                    self.set_status(202)
                    self.write(json.dumps(backfill_request_payload(request_id, request), ensure_ascii=False))
                    return
            pairs = {(symbol, period) for symbol in symbols}
            if pairs & (_BACKFILL_RUNNING | _BACKFILL_REQUEST_PAIRS):
                raise HTTPError(409, "symbol/period already queued or running for another request; retry after completion")
            active = sum(request["status"] in ("queued", "running") for request in _BACKFILL_REQUESTS.values())
            if active >= _BACKFILL_REQUEST_LIMIT:
                raise HTTPError(429, "backfill request queue is full")
            request_id = uuid4().hex
            request = {"status": "queued", "symbols": symbols, "period": period,
                       "start_time": start_time, "end_time": end_time,
                       "results": {symbol: {"status": "queued", "error": None} for symbol in symbols}}
            _BACKFILL_REQUESTS[request_id] = request
            _BACKFILL_REQUEST_QUEUE.append(request_id)
            _BACKFILL_REQUEST_PAIRS.update(pairs)
            payload = json.dumps(backfill_request_payload(request_id, request), ensure_ascii=False)
        self.set_status(202)
        self.write(payload)

    def get(self):
        request_id = self.get_argument("id", "")
        with _BACKFILL_LOCK:
            request = _BACKFILL_REQUESTS.get(request_id)
            if request is None:
                raise HTTPError(404, "backfill request not found")
            payload = json.dumps(backfill_request_payload(request_id, request), ensure_ascii=False)
        self.write(payload)


class BackfillStatusHandler(BaseHandler):
    def get(self):
        with _BACKFILL_LOCK:
            running = None
            if _BACKFILL_RUNNING:
                running = {"period": next(iter(_BACKFILL_RUNNING))[1], "symbols": len(_BACKFILL_RUNNING)}
            payload = {
                "targets": len(_UNIVERSE_SUBSCRIBED),
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
            user_order_id = str(data.get('userOrderId', '')).strip()
            order_ref = passorder(opType, orderType, self.acc(), stock, pr_type, price, volume, 'qmt', quickTrade, user_order_id, self.ctx())
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
        clients = [
            {"client_id": client.client_id, "addr": client.request.remote_ip,
             "state": "connected", "expires_at": None}
            for client in _WS_CLIENTS
        ]
        clients.extend(
            {"client_id": owner, "addr": client["addr"], "state": "grace",
             "expires_at": client["expires_at"]}
            for owner, client in _UNIVERSE_CLIENTS.items() if client["expires"] is not None
        )
        # Avoid waiting on the backfill lock, which may be held during disk I/O.
        backfill = {"pending": len(_BACKFILL_PENDING), "running": len(_BACKFILL_RUNNING)}
        universe_size = len(_UNIVERSE_SUBSCRIBED)
        self.write(json.dumps({
            "bridge_id": _BRIDGE_ID,
            "started_at": _STARTED_AT,
            "ioloop_lag_ms": {
                "latest": _IOLOOP_LAG_SAMPLES[-1][1] if _IOLOOP_LAG_SAMPLES else None,
                "max_60s": max((lag for sampled_at, lag in _IOLOOP_LAG_SAMPLES if sampled_at >= now - 60), default=None),
            },
            "realtime": {
                "last_push_at": _REALTIME_LAST_PUSH_AT,
                "pushed_symbol_count": _REALTIME_PUSHED_SYMBOL_COUNT,
                "symbol_count": sum(_UNIVERSE_SUBSCRIBED.values()),
                "subscription_count": len(_REALTIME_SUBSCRIPTIONS),
            },
            "universe_size": universe_size,
            "backfill": backfill,
            "clients": sorted(clients, key=lambda client: (client["client_id"], client["addr"])),
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
        (r"/api/data/backfill/request", BackfillRequestHandler),
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

    ], debug=False, compress_response=True, websocket_ping_interval=30, websocket_ping_timeout=30)


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
    _BACKFILL_STOP.set()
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
        _SERVER = HTTPServer(app)
        _SERVER.listen(PORT, address='0.0.0.0')
        start_backfill_worker()
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
        _queue_broadcast({"type": "order_update", "data": data})
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
        _queue_broadcast({"type": "deal_update", "data": data})
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
