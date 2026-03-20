# 性能基准 / Performance Benchmark

本报告基于本地 Mock OpenList 服务，测量 `openlist` 插件“目录列表”链路的性能。
This report measures the directory listing path of the `openlist` plugin against a local mock OpenList server.

## 方法 / Method

- Tool: [scripts/bench_openlist.py](file:///root/xm/cloud-auto-save/scripts/bench_openlist.py)
- Endpoint: `POST /api/fs/listGet` (local mock, no network latency)
- Workload:
  - Uncached: `refresh=true`, varying paths
  - Cached: `refresh=false`, same path (TTL cache hit)

Command:

```bash
python scripts/bench_openlist.py --n 300 --per-page 30 --delay-ms 0
```

## 结果（本地 Mock）/ Results (local mock)

- `n=300`, `per_page=30`, `delay_ms=0`
- Uncached latency (ms): mean `2.211`, p50 `2.133`, p95 `2.996`, min `1.621`, max `3.798`
- Cached latency (ms): mean `0.00425`, p50 `0.00319`, p95 `0.00375`, min `0.00303`, max `0.189`

## 说明 / Notes

- 这些数据反映的是本机回环 + Python/HTTP 栈开销，不代表真实 WAN/LAN 条件下的 OpenList 性能。
- 如需真实评估，请将插件指向你的 OpenList 实例重新运行基准测试，并设置代表性的 `--delay-ms` 或使用真实网络链路。
