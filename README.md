# QMT market-data bridge

`QMT_SERVICE.py` runs inside the QMT Windows strategy runtime. The main AlphaForge process connects to its HTTP/WebSocket service. This directory is a Git submodule; pushing its commit updates source control only, never the Windows QMT process.

## Deploy a bridge revision on Windows

1. Stop the existing QMT bridge strategy and back up its current `QMT_SERVICE.py`.
2. On the main project host, check out the approved revision and copy its `QMT_SERVICE.py` to the operator-approved transfer location:
   `git -C market/qmt show <commit>:QMT_SERVICE.py > QMT_SERVICE.py`
3. Transfer that file through the existing trusted operator channel. In the QMT Windows strategy editor, replace the current bridge script with this file, restore the operator's existing `ACCOUNT_ID`/`TOKEN` (source control deliberately retains placeholders), save it, then restart the QMT model. Preserve the editor's GBK encoding convention. Do not expose the QMT HTTP port to the public internet.
4. Verify the Windows bridge log shows the service is listening and a writable backfill-state path, then start/reload each project's QMT integration. Confirm configure/open/close logs identify the expected host, project path and process, and that both per-client and global universe sizes are correct. Update all clients using the shared endpoint; the bridge's global realtime ceiling includes disconnected clients and retiring quotes during their grace period.
5. On failure, stop the new strategy and restore the saved script in the QMT editor; main-side `market/qmt` checkout does not restore a Windows deployment.

Keep `QMT_SERVICE_backfill.json` beside the deployed script across model restarts; do not replace it with a file from another QMT data directory. It records completed native downloads, not client database completion. Missing/corrupt/unwritable state is logged and may require a fresh bounded download. The first installation has no state file: full-market archives wait outside Beijing weekday 09:15–15:05; realtime subscriptions remain available during that wait.

All authenticated projects share account and order/deal broadcasts: only give the token to mutually trusted clients. Legacy WebSocket bar subscribe/unsubscribe actions now use the same global 1m/5m universe; custom tick/field/count subscriptions are not independent subscriptions (tick reads remain available over HTTP). Raw HTTP subscribe/unsubscribe endpoints return a conflict directing callers to WebSocket configure, so they cannot duplicate or cancel another client's managed quotes.

Adjustment-factor synchronization cannot depend on a one-shot history broadcast: a disconnected client must be able to fetch factors independently. Deploy the HTTP failure/no-event distinction before relying on empty factor responses; older bridges return the same empty object for both cases.

No Windows path, remote host or deployment API is configured in this repository, so transfer, QMT editor replacement and runtime verification remain operator actions.
