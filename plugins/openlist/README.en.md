# OpenList Plugin (openlist)

This plugin provides a unified driver registration/discovery mechanism for an OpenList server and a standardized directory listing API with pagination, caching, retries, and graceful degradation.

## Features

- Any storage driver: no driver allowlist
- Driver registration & discovery
  - Manual registry via `drivers`
  - Auto discovery via OpenList admin API (when available)
- Standardized directory listing output: `content/total/readme/header/write/provider`
- Pagination: `page` and `per_page (1~100)`
- Caching
  - TTL cache for identical requests
  - Degradation fallback to the most recent successful result within `stale_ttl_seconds`
- Retry: exponential backoff for network errors and 429/5xx
- Task hook: refresh OpenList paths after a successful save task

## Install

- Put [openlist.py](file:///root/xm/cloud-auto-save/plugins/openlist.py) into `plugins/` (already included in this repo).
- Configure `plugins.openlist` in `quark_config.json` and restart.

## Global Configuration

Example `quark_config.json` (aligned with the minimal `alist.py` style: only `url/token/storage_id/root_dir`):

```json
{
  "plugins": {
    "openlist": {
      "url": "http://openlist:5244",
      "token": "",
      "storage_id": "1",
      "root_dir": "/"
    }
  }
}
```

Key fields:

- `url`: OpenList base URL
- `token`: OpenList token (required depending on your OpenList setup)
- `storage_id`: storage identifier, supports:
  - numeric ID: calls `GET /api/admin/storage/get?id=...` to resolve `mount_path`
  - explicit mapping: `/mount_path:/root` (e.g. `/mnt1:/`)
  - mount path only: `/mnt1` (then set `root_dir` explicitly)
- `root_dir`: prefix root for task `savepath` mapping; can be omitted when `storage_id` is `/mount:/root`

Optional advanced keys (not auto-inserted): `timeout_seconds/max_retries/backoff_seconds/cache_ttl_seconds/cache_max_entries/stale_ttl_seconds`.

## Driver Registration & Discovery

### Auto discovery

If supported and permitted, the plugin attempts:

- `GET /api/admin/storage/list`

It uses `id` as the driver key and stores `mount_path`.

## Standardized Directory Listing API

```python
from plugins.openlist import Openlist

p = Openlist(url="http://openlist:5244", token="")
resp = p.list_dir("/mnt1", page=1, per_page=30, refresh=False)
if resp["code"] == 200:
    for item in resp["data"]["content"]:
        print(item.get("name"), item.get("is_dir"))
```

Behavior:

- Tries `POST /api/fs/listGet` first, falls back to `POST /api/fs/list`
- If `refresh=False`, returns `cached=true` on cache hit
- If the request fails but a recent successful stale entry exists, returns `degraded=true`

## Task-level Config (Refresh after Save)

Optionally add `addition.openlist` to override the default mapping:

```json
{
  "taskname": "Example",
  "shareurl": "https://pan.xxx/s/abc",
  "savepath": "/Movies/Action",
  "addition": {
    "openlist": {
      "enable": true,
      "driver": "1",
      "mount_path": "/mnt1",
      "root_dir": "/Movies",
      "password": "",
      "refresh": true
    }
  }
}
```

After a successful save, the plugin maps (task override first, then plugin `storage_id/root_dir`):

- `savepath` relative to `root_dir`
- joins it with `mount_path`
- calls `refresh_dir()` for that directory

## Tests

```bash
python -m unittest discover -s tests -p "test_*py"
```

This includes unit tests and a local mock integration test for the OpenList API.
