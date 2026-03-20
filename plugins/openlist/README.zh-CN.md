# OpenList 插件（openlist）

本插件面向 OpenList 服务提供统一的“驱动器注册与发现”机制，并实现标准化的目录列表获取接口，支持分页、缓存、异常重试与降级处理。

## 功能

- 任意存储驱动器：不做驱动器白名单限制
- 驱动器注册与发现
  - 手动注册：通过 `drivers` 字段维护驱动器信息
  - 自动发现：尝试调用 OpenList 管理接口获取挂载点并自动写入内存映射
- 标准化目录列表：统一输出 `content/total/readme/header/write/provider`
- 分页：`page` 与 `per_page(1~100)`
- 缓存策略
  - TTL 缓存：同参数请求命中缓存直接返回
  - 降级：请求失败时在 `stale_ttl_seconds` 内回落到最近一次成功结果
- 重试策略：对网络错误与 429/5xx 进行指数退避重试
- 任务钩子：转存成功后可按映射规则触发 OpenList 目录刷新

## 安装

- 将 [openlist.py](file:///root/xm/cloud-auto-save/plugins/openlist.py) 放入 `plugins/` 目录（仓库内已包含）。
- 通过 `quark_config.json` 的 `plugins.openlist` 填写配置后重启服务。

## 全局配置

`quark_config.json` 示例（与 `alist.py` 风格一致，仅需 `url/token/storage_id/root_dir`）：

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

字段说明：

- `url`: OpenList 服务地址（不带尾部 `/` 也可）
- `token`: OpenList Token（按你的 OpenList 配置决定是否必填）
- `storage_id`: 存储标识，支持三种写法
  - 数字 ID：会调用 `GET /api/admin/storage/get?id=...` 获取挂载路径
  - 直接指定挂载：`/挂载路径:/根目录`（例如 `/mnt1:/`）
  - 直接指定挂载路径：`/mnt1`（此时 `root_dir` 建议显式填写）
- `root_dir`: 任务 `savepath` 的前缀根（用于转存后刷新路径映射）；若 `storage_id` 使用 `/挂载:/根` 格式则可留空

可选高级参数（不写也能用）：`timeout_seconds/max_retries/backoff_seconds/cache_ttl_seconds/cache_max_entries/stale_ttl_seconds`。

## 驱动器注册与发现

### 自动发现

若 OpenList 支持并允许访问管理接口，插件会尝试调用：

- `GET /api/admin/storage/list`

成功后以 `id` 作为 driver key，记录 `mount_path`。

## 标准化目录列表接口

在代码中可直接调用：

```python
from plugins.openlist import Openlist

p = Openlist(url="http://openlist:5244", token="")
resp = p.list_dir("/mnt1", page=1, per_page=30, refresh=False)
if resp["code"] == 200:
    for item in resp["data"]["content"]:
        print(item.get("name"), item.get("is_dir"))
```

接口行为：

- 优先请求 `POST /api/fs/listGet`，失败后兼容回落到 `POST /api/fs/list`
- `refresh=False` 时命中 TTL 缓存会返回 `cached=true`
- 请求失败且存在陈旧缓存时返回 `degraded=true`

## 任务级配置与转存后刷新

任务中可选添加 `addition.openlist`，用于覆盖默认映射并刷新：

```json
{
  "taskname": "示例任务",
  "shareurl": "https://pan.xxx/s/abc",
  "savepath": "/电影/动作",
  "addition": {
    "openlist": {
      "enable": true,
      "driver": "1",
      "mount_path": "/mnt1",
      "root_dir": "/电影",
      "password": "",
      "refresh": true
    }
  }
}
```

当任务转存成功后，插件会按（任务级覆盖优先、否则使用插件全局的 `storage_id/root_dir`）：

- `savepath` 去掉 `root_dir` 前缀得到相对路径
- 拼接到 `mount_path`，并对该目录执行 `refresh_dir()`

## 测试

运行所有测试：

```bash
python -m unittest discover -s tests -p "test_*py"
```

其中包含 openlist 的单元测试与基于本地 mock OpenList 的集成测试。
