# QMT market-data bridge

`QMT_SERVICE.py` runs inside the QMT Windows strategy runtime. The main AlphaForge process connects to its HTTP/WebSocket service. This directory is a Git submodule; pushing its commit updates source control only, never the Windows QMT process.

## Deploy a bridge revision on Windows

Bridge revisions that change the QMT data protocol must be switched as a coordinated pair with all project callers. There is no transparent mixed-version window: old callers with the new bridge lose manifest/automatic backfill and `history_data download=true` now returns 400; new callers with an old bridge get 404 for the download endpoint. Stop affected writers, deploy the bridge and update/reload callers together, then verify. Roll back the caller and bridge together as well.

1. Stop the existing QMT bridge strategy and back up its current `QMT_SERVICE.py`.
2. On the main project host, check out the approved revision and copy its `QMT_SERVICE.py` to the operator-approved transfer location:
   `git -C market/qmt show <commit>:QMT_SERVICE.py > QMT_SERVICE.py`
3. Transfer that file through the existing trusted operator channel. In the QMT Windows strategy editor, replace the current bridge script with this file, restore the operator's existing `ACCOUNT_ID`/`TOKEN` (source control deliberately retains placeholders), save it, then restart the QMT model. Preserve the editor's GBK encoding convention. Do not expose the QMT HTTP port to the public internet.
4. Verify the Windows bridge log shows the service is listening, then start/reload each project's QMT integration. Confirm configure/open/close logs identify the expected host, project path and process, and that per-client and global universe sizes are correct. Update all clients using the shared endpoint; its global realtime ceiling includes disconnected clients and retiring quotes during their grace period.
5. On failure, stop the new strategy and restore the saved script in the QMT editor; main-side `market/qmt` checkout does not restore a Windows deployment.

Download request state is memory-only. Restarting the bridge loses queued and completed request IDs; GET then returns 404 and the caller should POST the request again. Native data already downloaded remains in QMT's local data store.

WebSocket configure selects account, order and deal event streams with `streams: ["account", "order", "deal"]`; the default is no account/order/deal push. An explicit `action: "account_status"` requests a one-shot status response only on that connection. Download `client_id` separates scheduling and query lists, not authorization: any holder of the shared token can query another client's ID. Tenant authorization requires a separately deployed token/service boundary and is not implemented here. Legacy WebSocket bar subscribe/unsubscribe actions now use the same global 1m/5m universe; custom tick/field/count subscriptions are not independent subscriptions (tick reads remain available over HTTP). Raw HTTP subscribe/unsubscribe endpoints return a conflict directing callers to WebSocket configure, so they cannot duplicate or cancel another client's managed quotes.

The bridge preserves account, order, factor, realtime-market and other existing HTTP APIs except the retired history-backfill protocol.

No Windows path, remote host or deployment API is configured in this repository, so transfer, QMT editor replacement and runtime verification remain operator actions.
