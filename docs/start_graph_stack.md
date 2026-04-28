# 图驱动一键启动脚本

`scripts/start_graph_stack.py` 用于从网络图生成运行 YAML，并一次启动 free5GC、UERANSIM 和 ns-3。脚本不做降级：场景必须提供 `topology.graph_file` 或 `topology.graph_snapshot_id`，并且必须配置 `writer.graph_db_url`。

## 用法

如果场景里的 `writer.graph_db_url` 指向 `localhost:5433`，先确认本机端口已经连到 PostgreSQL。例如当前示例场景需要先开隧道：

```bash
ssh -L 5433:localhost:5432 xiezhengyi@172.19.160.1 -N
```

另一个终端里确认端口：

```bash
ss -ltnp | rg ':5433\b'
```

从图快照场景启动：

```bash
python3 scripts/start_graph_stack.py scenarios/graph_snapshot_real_smoke.yaml
```

从最新 4-UPF 场景启动，并持续更新同一张 PostgreSQL 网络图：

```bash
python3 scripts/start_graph_stack.py scenarios/new_scenatios.yaml \
  --run-id new-scenatios-4upf-live-001
```

显式指定图快照和数据库：

```bash
python3 scripts/start_graph_stack.py scenarios/graph_snapshot_real_smoke.yaml \
  --graph-snapshot-id graph-89c679b8-3aed-4c2c-9594-ccdab6bd4499 \
  --graph-db-url "postgresql://postgres:123456@localhost:5433/multiagents_db"
```

显式指定拓扑图文件：

```bash
python3 scripts/start_graph_stack.py scenarios/policy_graph_multi_gnb.yaml \
  --graph-file scenarios/graphs/policy_graph_multi_gnb.yaml \
  --graph-db-url "postgresql://postgres:123456@localhost:5433/multiagents_db"
```

可选地指定运行 ID：

```bash
python3 scripts/start_graph_stack.py scenarios/graph_snapshot_real_smoke.yaml --run-id graph-run-001
```

## 产物

脚本启动后会打印 `run_id`、转换后的 YAML、manifest 和日志目录。主要产物位于：

- `artifacts/runs/<run-id>/generated/scenario-from-graph.yaml`：网络图合并后的场景 YAML。
- `artifacts/runs/<run-id>/generated/config/`：注入 free5GC / UERANSIM 的生成配置。
- `artifacts/runs/<run-id>/generated/ns3/tick-snapshots.jsonl`：ns-3 按场景 `tick_ms` 输出的 tick。
- `artifacts/runs/<run-id>/generated/ns3/real-ue-flows.jsonl`：真实 UE UDP 发生器每个 ns-3 tick 的发送记录。
- `artifacts/runs/<run-id>/generated/config/uerouting.yaml`：ULCL 路由和 `pfdDataForApp`，用于 free5GC application PFD 数据。
- `artifacts/runs/<run-id>/state/writer.db`：本地 SQLite 状态。
- `artifacts/runs/<run-id>/archive/`：tick 归档。

## 真实用户面

`scenarios/new_scenatios.yaml` 已开启：

```yaml
bridge:
  enable_inline_harness: true
```

开启后，运行 manifest 会在 RAN 容器启动后执行 `bridge-setup`，为每条 gNB→UPF backhaul 链路创建 tap/bridge/veth，并把这些 tap 注入 ns-3。`ns3-run` 同时带 `--external-traffic-only`，因此 ns-3 不再启动内部 `UdpClient/UdpServer` 用户面；真实 UDP 包由 `real-ue-flows` 从 UERANSIM UE 容器的 `uesimtun*` 发出。

`real-ue-flows` 读取 `generated/ns3-flow-profiles.tsv`，每条 flow 使用独立 UDP 五元组：UL 源端口为 `15000 + flow_index`，UL 目的端口为 `5000 + flow_index`，目的地址默认为 `8.8.8.8`；DL 使用反向五元组，从有该 UE PDU IP `upfgtp` 路由的 UPF 容器发往 UE。多 PDU session UE 会按 `session_ref` 顺序选择对应的 `uesimtunN`，不再把所有 flow 固定到第一个 `uesimtun`。包大小来自 `packet_size_bytes`。发生器只以 `generated/ns3/ns3-clock.json` 为节拍源，并按 clock 中每条 flow 的 `allocated_bandwidth_ul_mbps` / `allocated_bandwidth_dl_mbps` 计算本 tick 的 UL/DL 包数，因此真实用户面和 ns-3 指标使用同一套 slice 分配结果。`scenarios/new_scenatios.yaml` 当前使用 `tick_ms: 100`。

`writer-follow-ns3` 会同步读取 `real-ue-flows.jsonl`，在写 SQLite/PostgreSQL 网络图前用真实 UE PDU IP、真实 `uesimtunN` 和 DL UPF 容器覆盖 ns-3 snapshot 中的五元组。这样网络图中的 flow 五元组对应真实 free5GC 用户面，而不是 ns-3 内部 EPC 地址。

ns-3 external mode 不再使用内部 `UdpClient/UdpServer` 的 FlowMonitor 结果；它按 flow profile 的 offered load、已分配上下行带宽、SLA loss/jitter/latency 和 bridge delay 输出容量模型指标。因此 `throughput_ul_mbps`、`throughput_dl_mbps`、`loss_rate`、`delay_ms`、`jitter_ms` 不再是全理想值。真实发包明细以 `real-ue-flows.jsonl` 为准。

## Slice 级逻辑隔离

`scenarios/new_scenatios.yaml` 已开启：

```yaml
ns3:
  slice_isolation: true
```

开启后，每个 `slices[]` 条目必须声明 `resource.capacity_*`、`resource.guaranteed_*` 和 `resource.priority`。渲染器会生成 `generated/ns3/slice-resources.tsv`，ns-3 每个 tick 按 `(gNB, slice)` 资源池分配带宽。slice 节点会持续写回 `capacity`、`load` 和 `telemetry`，flow 节点会持续写回 `allocation`。

这里的隔离是混合逻辑隔离：ns-3 负责统一时钟、资源分配、指标和真实流量限速；free5GC/UERANSIM 只表达 S-NSSAI、DNN、UPF 选择、QoSFlow GBR/MBR 和 PDU Session AMBR，不提供 RAN scheduler 级硬隔离。

## App Data

`bootstrap-app-data` 会把 `generated/config/uerouting.yaml` 中的 `pfdDataForApp` upsert 到 MongoDB `free5gc.applicationData.pfds` 集合。检查方式：

```bash
docker exec mongodb mongo --quiet --eval \
  'db=db.getSiblingDB("free5gc"); print(db.getCollection("applicationData.pfds").count()); db.getCollection("applicationData.pfds").find().limit(3).forEach(printjson)'
```

free5GC WebUI 的注册 UE 页面来自 AMF OAM，不等同于 UDR 的 application PFD 数据。如果 WebUI 页面没有单独展示 `applicationData.pfds`，以 MongoDB/UDR 中的集合为准。

## 日志

每个命令的 stdout/stderr 都写到：

```text
artifacts/runs/<run-id>/logs/
```

常用日志：

- `compose-up-core.log`：free5GC core 启动。
- `bootstrap-subscribers.log`：UE 订阅者注入。
- `bootstrap-app-data.log`：application PFD 数据注入 MongoDB。
- `compose-up-ran.log`：UERANSIM gNB / UE 启动。
- `writer-follow-free5gc.log`：free5GC 语义事件采集。
- `writer-follow-ueransim.log`：UERANSIM 语义事件采集。
- `real-ue-flows.log`：真实 UE UDP 发生器输出；每行包含方向、tick、flow、UE PDU 地址或 UPF 容器、接口、目标地址、包大小和包数。
- `writer-follow-ns3.log`：ns-3 tick 写入 SQLite 和 PostgreSQL 图。
- `ns3-build.log`、`ns3-run.log`：ns-3 编译和运行。
- `compose-down.log`：退出清理。

也可以直接查看 Docker 日志：

```bash
docker compose -p <compose_project_name> -f <compose_file> logs -f
```

`compose_project_name` 和 `compose_file` 可在 `artifacts/runs/<run-id>/run-manifest.json` 中查看。

## 图写回

脚本会让 `writer-follow-ns3` 带 `--ensure-graph-schema` 和 `--live-graph-snapshot-id` 运行，并使用场景中的 `writer.graph_db_url` 写 PostgreSQL 图。每个 ns-3 tick 更新同一个 `network_graph_snapshot.snapshot_id`，节点、边和指标按 key 更新，不为每个 tick 新建图快照条目。默认 live 图 ID 是 `live-<scenario_id>`，也可以通过 `--live-graph-snapshot-id` 指定。

检查最近写回结果：

```sql
SELECT
  graph_summary->>'tick_index' AS tick_index,
  graph_summary->>'write_mode' AS write_mode,
  graph_summary->>'delta_node_count' AS delta_node_count,
  graph_summary->>'delta_edge_count' AS delta_edge_count,
  graph_summary->>'delta_metric_count' AS delta_metric_count
FROM network_graph_snapshot
WHERE snapshot_id = 'live-<scenario_id>';
```

## 中断行为

按 `Ctrl+C` 时，脚本会终止已启动的后台 writer / ns-3 进程，并执行 `docker compose down` 结束 free5GC 和 UERANSIM 容器。

如果复用同一个 `--run-id`，脚本启动前会终止同 run-id 的残留 writer / ns-3 / 真实流发生器进程，并清空该 run 目录下旧的 `tick-snapshots.jsonl`、`ns3-clock.json`、`real-ue-flows.jsonl`，避免旧进程和旧 JSONL 干扰本次运行。
