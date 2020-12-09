#### 性能报告

机器参数

- 执行稽核程序的机器参数为：
  1. cpu: 8核 Intel(R) Core(TM) i5-8265U CPU @ 1.60GHz 1.80GHz
  2. men: 16GB
  3. net：内网，不统计
  4. disc： 不统计
- presto服务器参数：
  - cpu：4核
  - mem：15G
  - net：不统计
  - disc：不统计

---

presto 内存配置 

**jvm.config：**

```shell
-Xmx10G
```

**config.properties：**

```shell
query.max-memory=8GB
query.max-memory-per-node=5GB
query.max-total-memory-per-node=6GB
```

---

#### 测试报告

| 数据量 | 任务耗时（presto) |  稽核耗时（presto)  | 任务耗时（java） | 稽核耗时（java) |
| :----: | :---------------: | :-----------------: | :--------------: | :-------------: |
|  10万  |     **7~9s**      |      **7~9s**       |     **5~6s**     |    **5~6s**     |
|  50万  |    **33~36s**     |     **31~34s**      |    **34~37s**    |   **19~22s**    |
| 100万  |    **1m 31s**     |     **1m 28s**      | **1m 7s~1m 15s** |  **41s ~ 43s**  |
| 300万  |     **≈ 5m**      | **4m 10s~ 4m 50s**  |       - -        |       - -       |
| 500万  | **7m 20s~8m 5s**  | **6m 50s ~ 7m 30s** |       - -        |       - -       |
| 1000万 |    **16m 44s**    |     **16m 18s**     |       - -        |       - -       |
