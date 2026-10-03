# QMT bridge

`QMT_SERVICE.py` runs inside the QMT strategy runtime; the main project connects to its WebSocket and HTTP service. Remote QMT deployment is operator-managed ([root deployment boundary](../../ARCHITECTURE.md)). This directory is a Git submodule on its configured `master` branch. After review, push the QMT change to that branch, then in the main repo run `git submodule update --remote market/qmt` and commit the updated gitlink. The pinned source revision still must be operator-deployed to QMT; neither step updates a running installation.

## Windows deployment

1. On the Windows machine running QMT, back up the existing strategy script and preserve the configured QMT account settings. `QMT_SERVICE.py` declares a GBK coding cookie, while the checked-in file is UTF-8; save/convert the deployed script to GBK so the QMT Python loader can decode it.
2. Copy `QMT_SERVICE.py` into the QMT strategy/script editor, select the intended account, and run it as a strategy. The service binds `0.0.0.0:10086`; allow inbound access only from the trusted main-project host using the Windows/network firewall. Do not expose the port publicly.
3. Set the script's `ACCOUNT_ID` and `TOKEN` to match the QMT account and the main project's `[qmt.accounts.*]` endpoint (`host`, `port`, `token`; see `config.toml.example`). Keep the token private. The QMT strategy account is not inferred from the endpoint name.
4. From a trusted main-project host, configure the QMT market-data endpoint and start its integration. Confirm the WebSocket reports `configured` with the expected complete universe and realtime count, and verify history/realtime data from the main project. Local configuration references are in `market/services/integrations/qmt.py`; service bind and start behavior are in `QMT_SERVICE.py`.
5. For a code change, stop automated trading first, protect exposure and reconcile accepted orders, then have the operator update/restart the QMT strategy and read back its behavior. Tests do not establish remote deployment success; see [deployment boundaries](../../ARCHITECTURE.md).

QMT's bundled Python environment may need Tornado, which this service imports. `QMT安装python第三方库.md` documents the QMT `bin.x64\Lib\site-packages` location, recommends backing up `DLLs` and `Lib` before library changes, and warns against upgrading bundled libraries. Check compatibility with the installed QMT runtime before adding dependencies; do not upgrade bundled pandas/numpy as a shortcut.

The WebSocket `configure` universe is a list of `{ "symbol": "...", "subscribed": true|false }` rows. Every symbol remains in historical-data scope; only `subscribed: true` enables realtime quotes. The bridge rejects malformed rows and refuses more than 50 distinct realtime symbols rather than silently dropping symbols. The cap is this bridge's safety limit, not a claim about QMT's provider maximum.

Run the offline bridge contract checks from the repository root:

```sh
uv run python -m unittest market.qmt.test_universe_protocol
```
