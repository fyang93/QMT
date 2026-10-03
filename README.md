# QMT market-data bridge

`QMT_SERVICE.py` runs inside the QMT Windows strategy runtime. The main AlphaForge process connects to its HTTP/WebSocket service. This directory is a Git submodule; pushing its commit updates source control only, never the Windows QMT process.

## Deploy a bridge revision on Windows

1. Stop the existing QMT bridge strategy and back up its current `QMT_SERVICE.py`.
2. On the main project host, check out the approved revision and copy its `QMT_SERVICE.py` to the operator-approved transfer location:
   `git -C market/qmt show <commit>:QMT_SERVICE.py > QMT_SERVICE.py`
3. Transfer that file through the existing trusted operator channel. In the QMT Windows strategy editor, replace the current bridge script with this file, save it, then start the strategy. Do not expose the QMT HTTP port to the public internet.
4. Verify the Windows bridge log shows the service is listening, then start/reload the main QMT integration. Confirm the WebSocket `configured` response has the expected `symbols` and `realtime` counts and the main process logs completed subscriptions/backfill. This schema cutover's wire format is `{"symbol":"...","subscribed":true|false}`; both endpoints must use the same revision before sending the new format.
5. On failure, stop the new strategy and restore the saved script in the QMT editor; main-side `market/qmt` checkout does not restore a Windows deployment.

Adjustment-factor synchronization cannot depend on a one-shot history broadcast: a disconnected client must be able to fetch factors independently. Deploy the HTTP failure/no-event distinction before relying on empty factor responses; older bridges return the same empty object for both cases.

No Windows path, remote host or deployment API is configured in this repository, so transfer, QMT editor replacement and runtime verification remain operator actions.
