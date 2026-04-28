# ns3-free5gc-integration

**编排与快照工具链**，用于将 [free5GC](https://free5gc.org/) 核心网、[UERANSIM](https://github.com/aligungr/UERANSIM) 无线接入网仿真器和 [ns-3 NR](https://5g-lena.cttc.es/) 数字孪生（Digital Twin）有机地集成在一起，实现 5G 端到端场景的自动化配置渲染、运行编排、日志采集和性能指标持久化。

## 目录

- [快速开始](#快速开始)
- [前置依赖](#前置依赖)
- [项目结构](#项目结构)
- [设计概述](#设计概述)
  - [总体架构](#总体架构)
  - [场景声明层](#场景声明层)
  - [配置渲染引擎](#配置渲染引擎)
  - [运行编排器](#运行编排器)
  - [Writer 数据采集管线](#writer-数据采集管线)
  - [ns-3 数字孪生](#ns-3-数字孪生)
  - [图快照持久化](#图快照持久化)
- [场景定义](#场景定义)
- [使用方式](#使用方式)
- [测试](#测试)

---

## 快速开始

```bash
# 1. 安装 Python 依赖
pip install -e .

# 2. 从场景文件渲染运行产物（配置、Compose 文件、脚本、清单）
python scripts/render_run.py scenarios/baseline_single_upf.yaml

# 3. 根据生成的 manifest 启动整个栈
python scripts/start_stack.py artifacts/runs/<run-id>/run-manifest.json

# 图驱动一键启动（网络图 -> YAML -> free5GC/UERANSIM/ns-3）
python3 scripts/start_graph_stack.py scenarios/graph_snapshot_real_smoke.yaml
```

## 前置依赖

| 组件 | 版本要求 |
|------|----------|
| Python | >= 3.10 |
| Docker & Docker Compose | v2+ |
| free5GC compose 仓库 | 位于本地，场景中通过 `compose_file` 指定 |
| UERANSIM | 由 free5GC compose 中的 `ueransim` 镜像提供 |
| ns-3 + NR 模块 | ns-allinone >= 3.43，路径通过场景 `ns3.ns3_root` 指定 |
| PostgreSQL（可选） | 用于图快照持久化，通过 `writer.graph_db_url` 配置 |

Python 依赖（`PyYAML`、`SQLAlchemy`、`psycopg`）通过 `pyproject.toml` 声明，执行 `pip install -e .` 即可安装。

---

## 项目结构

```
ns3_free5gc_integration/
├── adapters/                        # 外部系统适配器
│   └── free5gc_ueransim/
│       ├── bridge_setup.py          # Linux tap/bridge 网桥脚本生成
│       ├── compose_override.py      # Docker Compose 动态渲染
│       └── subscriber_bootstrap.py  # free5GC WebUI 订阅者自动注册
├── bridge/                          # 核心框架
│   ├── common/
│   │   ├── ids.py                   # 安全名称生成与 run_id 工具
│   │   ├── scenario.py              # 场景配置数据模型与 YAML 加载
│   │   └── schema.py                # Tick Snapshot / SimEvent 数据协议
│   ├── orchestrator/
│   │   ├── cli.py                   # prepare-run / start CLI 入口
│   │   ├── config_renderer.py       # 场景 → 全部运行产物的渲染引擎
│   │   └── process_plan.py          # RunManifest 与 CommandSpec 构建
│   └── writer/
│       ├── cli.py                   # 数据采集 CLI（ingest / follow）
│       ├── graph_mapper.py          # TickSnapshot → 图节点/边/指标映射
│       ├── http_sink.py             # 可选 HTTP 外部投递
│       ├── local_store.py           # SQLite 本地归档与事件存储
│       ├── log_parser.py            # free5GC / UERANSIM 日志语义解析
│       └── postgres_graph_store.py  # PostgreSQL 图快照持久化
├── scenarios/                       # 场景定义文件（YAML）
│   ├── baseline_single_upf.yaml
│   ├── baseline_multi_ue.yaml
│   └── baseline_ulcl.yaml
├── scripts/                         # 便捷脚本
│   ├── build_ns3_twin.sh            # 编译 ns-3 scratch 程序
│   ├── run_ns3_twin.sh              # 运行 ns-3 仿真并输出 JSONL
│   ├── render_run.py                # 渲染场景到 artifacts/
│   └── start_stack.py               # 按 manifest 启动全部命令
├── sim/
│   └── ns3/
│       └── nr_multignb_multiupf.cc  # ns-3 NR 数字孪生仿真程序
├── tests/                           # 单元 / 集成测试
├── artifacts/                       # 运行时产物（gitignore）
│   └── runs/<run-id>/
│       ├── run-manifest.json
│       ├── resolved-scenario.json
│       ├── generated/               # 渲染后的配置与脚本
│       ├── state/                   # SQLite 状态数据库
│       └── archive/                 # 归档的 tick 快照
└── pyproject.toml
```

---

## 设计概述

### 总体架构

本项目采用**声明式场景驱动**的分层架构，将 5G 核心网仿真、RAN 仿真和 ns-3 数字孪生的复杂部署抽象为一套可重复、可审计的自动化流水线：

```
┌─────────────────────────────────────────────────────────────────────┐
│                       Scenario YAML (声明层)                         │
│   定义 slices / UPFs / gNBs / UEs / sessions / ns3 / writer 参数     │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    Config Renderer (渲染引擎)                        │
│  scenario.yaml ──►  gnb configs, ue configs, smfcfg, upfcfg         │
│                 ──►  docker-compose.generated.yaml                   │
│                 ──►  subscriber JSON payloads                        │
│                 ──►  bridge setup script                             │
│                 ──►  run-manifest.json (执行计划)                     │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    Orchestrator CLI (运行编排器)                      │
│  按 manifest 中定义的 CommandSpec 依序 / 后台启动各组件：               │
│    1. docker compose up 核心网                                       │
│    2. Writer follow 核心网日志                                        │
│    3. 向 free5GC WebUI 注册订阅者                                     │
│    4. docker compose up RAN (gNB + UE)                               │
│    5. Writer follow UERANSIM 日志                                     │
│    6. Writer follow ns-3 输出                                         │
│    7. 编译并运行 ns-3 数字孪生                                         │
└──────────┬──────────────────┬───────────────────┬───────────────────┘
           │                  │                   │
           ▼                  ▼                   ▼
   ┌──────────────┐  ┌────────────────┐  ┌────────────────────┐
   │  free5GC     │  │  UERANSIM      │  │  ns-3 NR Twin      │
   │  (Docker)    │  │  gNB + UE      │  │  (C++ scratch)     │
   │  AMF/SMF/    │  │  (Docker)      │  │  输出 JSONL         │
   │  UPF/NRF/…  │  │                │  │  TickSnapshot 流    │
   └──────┬───────┘  └───────┬────────┘  └─────────┬──────────┘
          │                  │                      │
          ▼                  ▼                      ▼
   ┌─────────────────────────────────────────────────────────────┐
   │                  Writer 数据采集管线                          │
   │   Log Parser (语义事件提取)  +  JSONL Follower               │
   │              │                        │                     │
   │       ┌──────┴──────┐          ┌──────┴──────┐              │
   │       │ SQLite      │          │ PostgreSQL  │              │
   │       │ Local Store │          │ Graph Store │              │
   │       │ + Archive   │          │ (可选)      │              │
   │       └─────────────┘          └─────────────┘              │
   └─────────────────────────────────────────────────────────────┘
```

### 场景声明层

每个实验由一个 **Scenario YAML** 文件完整描述。场景文件是整个系统的唯一输入，包含：

- **Slices**：网络切片（SST/SD）定义
- **UPFs**：用户面功能实例及其角色（`upf` / `branching-upf` / `anchor-upf`）
- **gNBs**：基站配置，包括 TAC、NCI、切片绑定、回程 UPF 关联
- **UEs**：终端设备配置，含 SUPI、密钥、鉴权参数、PDU Session 定义
- **free5gc**：核心网 Compose 文件路径、模式（`single_upf` / `ulcl`）、bridge 名称
- **ns3**：ns-3 根路径、scratch 程序名、仿真时长
- **writer**：归档目录、状态数据库路径、可选 PostgreSQL URL
- **bridge**：可选的 inline tap harness 开关

场景数据模型定义在 `bridge/common/scenario.py`，使用 frozen dataclass，在加载时做完整的交叉引用校验（UE → gNB、Session → Slice）。

### Slice 级逻辑资源隔离

启用 `ns3.slice_isolation: true` 后，每个 slice 必须显式声明资源池：

```yaml
slices:
  - sst: 1
    sd: "000001"
    label: embb
    resource:
      capacity_dl_mbps: 80
      capacity_ul_mbps: 40
      guaranteed_dl_mbps: 50
      guaranteed_ul_mbps: 20
      priority: 1

ns3:
  slice_isolation: true
```

渲染器会生成 `generated/ns3/slice-resources.tsv`，ns-3 每个 tick 按 `(gNB, slice)` 资源池分配 UL/DL 带宽，并把每条 flow 的 allocation 写入 `generated/ns3/ns3-clock.json`。真实 UE UDP 发生器读取该 clock 文件限速，因此外部真实用户面和 ns-3 指标使用同一套时钟与资源分配。

free5GC/UERANSIM 侧只做逻辑隔离表达：S-NSSAI、DNN、UPF 选择、QoSFlow GBR/MBR 和 PDU Session AMBR。它们不提供 RAN scheduler 级硬隔离；本项目的隔离语义由 ns-3 slice 资源模型和真实流量限速闭环保证。

### 配置渲染引擎

`bridge/orchestrator/config_renderer.py` 中的 `render_run_assets()` 是核心渲染函数。它接收场景对象，执行以下转换：

1. **核心网配置渲染**：读取 free5GC 原始 `smfcfg.yaml` / `upfcfg.yaml`，替换服务发现地址为容器固定 IP
2. **gNB 配置渲染**：基于模板为每个 gNB 生成独立配置（IP、TAC、NCI、AMF 地址、切片列表）
3. **UE 配置渲染**：为每个 UE 生成独立配置（SUPI、密钥、gNB 搜索地址、Session/NSSAI）
4. **Compose 文件渲染**：在原始 compose 上动态叠加 gNB 和 UE 服务定义，分配容器 IP，挂载渲染后的配置卷
5. **订阅者 Payload 生成**：为每个 UE 构建 free5GC WebUI 格式的 JSON，包含鉴权、SM、策略订阅数据
6. **Bridge 脚本生成**：（可选）生成 Linux tap/bridge/veth/nsenter 网桥脚本，用于 ns-3 inline harness
7. **RunManifest 构建**：将所有步骤的命令行、工作目录、环境变量编码为 `run-manifest.json`

所有渲染产物写入 `artifacts/runs/<run-id>/generated/`，确保每次运行可追溯和可重放。

### 运行编排器

`bridge/orchestrator/cli.py` 提供两个子命令：

- **`prepare-run`**：加载场景 → 生成 run-id → 渲染全部产物 → 输出 manifest JSON
- **`start`**：读取 manifest → 按序执行命令（支持 `--step` 过滤、`--dry-run` 预览、后台命令的 `--wait-background`）

执行计划（`process_plan.py`）按以下顺序编排步骤：

| 步骤 | 名称 | 说明 |
|------|------|------|
| 1 | `compose-up-core` | 启动核心网服务（AMF, SMF, UPF, NRF, …） |
| 2 | `writer-follow-free5gc` | 后台：follow 核心网日志并提取语义事件 |
| 3 | `bootstrap-subscribers` | 等待 WebUI 就绪后通过 HTTP API 注册所有 UE 订阅者 |
| 4 | `compose-up-ran` | 启动 RAN 服务（gNB + UE 容器） |
| 5 | `writer-follow-ueransim` | 后台：follow UERANSIM 日志并提取语义事件 |
| 6 | `writer-follow-ns3` | 后台：tail ns-3 输出的 JSONL 文件 |
| 7 | `ns3-build` | 编译 ns-3 scratch 程序 |
| 8 | `ns3-run` | 运行 ns-3 NR 仿真，输出 TickSnapshot 流 |

### Writer 数据采集管线

`bridge/writer/` 是一套可组合的数据采集管线，负责将三个数据源的输出统一到本地存储和外部持久化层。

**日志语义解析器**（`log_parser.py`）：
- **free5GC 解析器**：基于正则规则从 Docker Compose 日志中提取注册请求/完成、PDU 会话建立、PFCP 关联等 12+ 种语义事件
- **UERANSIM 解析器**：提取 SCTP 连接、NG Setup、RRC 连接、注册成功/失败、PDU 会话建立、TUN 接口创建等 12+ 种事件
- 每个事件被标准化为 `SimEvent`（run_id, tick_index, event_type, entity_type, entity_id, payload_json）

**本地存储**（`local_store.py`）：
- 使用 SQLite（WAL 模式）存储运行元数据（`sim_run`）、tick 快照（`sim_tick`）和语义事件（`sim_event`）
- 自动将快照归档为 JSON 文件（按 tick_index 编号）

**图快照持久化**（`postgres_graph_store.py`）：
- 可选地将 TickSnapshot 映射为图结构（`NetworkGraphSnapshot` → `GraphNode` + `GraphEdge` + `GraphMetric`）
- 使用 SQLAlchemy ORM 写入 PostgreSQL，支持自动建表
- 节点类型：`ran_node`、`ue`、`core_node`、`slice`、`flow`、`app`
- 边类型：`attached_to`、`tunneled_via`、`serves_slice` 等
- 指标：delay_ms、jitter_ms、loss_rate、throughput、queue_bytes 等

**HTTP Sink**（`http_sink.py`）：可选的外部 HTTP 端点投递。

### ns-3 数字孪生

`sim/ns3/nr_multignb_multiupf.cc` 是一个 ns-3 NR 仿真程序，作为 5G RAN 的数字孪生运行：

- 支持多 gNB、多 UE-per-gNB、多 UPF 拓扑
- 使用 `FlowMonitor` 采集每条流的时延、抖动、丢包率、吞吐量
- 按 `tickMs` 间隔周期性输出 **TickSnapshot** JSONL 到文件
- 每个 TickSnapshot 包含完整的拓扑图（nodes, links, gnbs, ues, flows, slices）和聚合 KPI

`scripts/build_ns3_twin.sh` 负责将 `.cc` 文件复制到 ns-3 scratch 目录并编译；`scripts/run_ns3_twin.sh` 负责执行仿真并传递场景参数。

### 图快照持久化

`graph_mapper.py` 将每个 TickSnapshot 转换为一个自洽的图快照包（`GraphSnapshotBundle`），包含：

- **snapshot_row**：快照元数据（snapshot_id, trigger_event, graph_summary）
- **node_rows**：所有拓扑节点（gNB, UE, UPF, Slice, Flow, App）
- **edge_rows**：拓扑关系（UE→gNB, gNB→UPF, Flow→Slice, …）
- **metric_rows**：每个 Flow 的性能指标时间序列

这种设计使得下游系统（如多智能体决策平台）可以按图快照为单位查询和推理网络状态。

---

## 场景定义

### baseline_single_upf.yaml

单 UPF 基线场景：1 gNB + 1 UE + 1 UPF + 1 eMBB 切片。

```yaml
name: baseline-single-upf
slices:
  - sst: 1
    sd: "010203"
    label: embb
upfs:
  - name: upf
    role: upf
gnbs:
  - name: gnb1
    alias: gnb.free5gc.org
    backhaul_upf: upf
ues:
  - name: ue1
    supi: imsi-208930000000001
    gnb: gnb1
    sessions:
      - apn: internet
        slice_ref: slice-1-010203
```

      ### baseline_multi_ue.yaml

      多 UE 单 UPF 基线场景：1 gNB + 2 UE + 1 UPF + 1 eMBB 切片。这个场景用于验证生成链路是否能同时渲染多个 UE 配置、多个订阅者 payload，并在真实 free5GC + UERANSIM 栈里完成双 UE 注册和 PDU 建立。

### baseline_ulcl.yaml

ULCL（Uplink Classifier）场景：1 gNB + 1 UE + 2 UPF（branching-upf + anchor-upf）。

当前生成器会为这个场景额外渲染 ULCL 专用的 `smfcfg.yaml`、`i-upf-upfcfg.yaml` 和 `psa-upf-upfcfg.yaml`，并为 AMF、SMF、I-UPF、PSA-UPF、gNB 分配显式内网地址，确保真实双 UPF 拓扑里的 PFCP 和 N3/N9 路径不会再依赖 `*.free5gc.org` 解析结果。

### baseline_ulcl_multi_gnb.yaml

ULCL + 双 gNB 场景：2 gNB + 2 UE + 2 UPF。这个场景通过图文件自动派生 `i-upf`、`psa-upf`、`gnb1`、`gnb2`、`ue1`、`ue2`，并把 ULCL 的 `smfcfg.yaml` 渲染成多 AN 节点拓扑，让多个 gNB 同时指向 I-UPF，再由 I-UPF 连接 PSA-UPF。

---

## 使用方式

启动ssh隧道：
`ssh -L 5433:localhost:5432 xiezhengyi@172.19.160.1 -N`

### 1. 渲染运行产物

```bash
python scripts/render_run.py scenarios/baseline_single_upf.yaml
# 输出 run-manifest.json 到 stdout，产物写入 artifacts/runs/<run-id>/

# 双 UE 基线
python scripts/render_run.py scenarios/baseline_multi_ue.yaml

# ULCL 基线
python scripts/render_run.py scenarios/baseline_ulcl.yaml

# ULCL + 双 gNB 基线
python scripts/render_run.py scenarios/baseline_ulcl_multi_gnb.yaml
```

也可指定 run-id：

```bash
python -m bridge.orchestrator.cli prepare-run scenarios/baseline_single_upf.yaml --run-id my-test-001

python -m bridge.orchestrator.cli prepare-run scenarios/baseline_multi_ue.yaml --run-id my-multi-ue-001

python -m bridge.orchestrator.cli prepare-run scenarios/baseline_ulcl.yaml --run-id my-ulcl-001

python -m bridge.orchestrator.cli prepare-run scenarios/baseline_ulcl_multi_gnb.yaml --run-id my-ulcl-multi-gnb-001
```

### 2. 启动全栈

```bash
python scripts/start_stack.py artifacts/runs/<run-id>/run-manifest.json
```

图驱动的一键启动、日志查看和图增量写回说明见 [docs/start_graph_stack.md](docs/start_graph_stack.md)。

说明：针对真实 free5GC Compose 基线，生成器现在会为 SMF、UPF 和 gNB 渲染显式内网地址；在 ULCL 场景里还会为 I-UPF 和 PSA-UPF 分别生成独立配置，避免宿主机把 `*.free5gc.org` 解析到 `198.18.x.x` 后造成 PFCP/NGAP/N9 指向错误目标。

仅执行特定步骤：

```bash
python -m bridge.orchestrator.cli start artifacts/runs/<run-id>/run-manifest.json \
  --step compose-up-core \
  --step bootstrap-subscribers
```

预览命令（不实际执行）：

```bash
python -m bridge.orchestrator.cli start artifacts/runs/<run-id>/run-manifest.json --dry-run
```

### 3. 手动数据摄入

```bash
# 从文件摄入 tick 快照
python -m bridge.writer.cli ingest-file artifacts/runs/<run-id>/generated/ns3/tick-snapshots.jsonl \
  --state-db artifacts/runs/<run-id>/state/writer.db \
  --archive-dir artifacts/runs/<run-id>/archive

# 实时 tail JSONL 文件
python -m bridge.writer.cli follow-jsonl /path/to/tick-snapshots.jsonl \
  --state-db state/writer.db \
  --archive-dir archive/

# 带 PostgreSQL 图存储
python -m bridge.writer.cli ingest-file snapshots.jsonl \
  --state-db state.db \
  --archive-dir archive/ \
  --graph-db-url "postgresql://user:pass@localhost:5433/dbname" \
  --ensure-graph-schema
```

### 4. 订阅者引导

```bash
python -m adapters.free5gc_ueransim.subscriber_bootstrap \
  --base-url http://127.0.0.1:5000 \
  --timeout-seconds 120 \
  artifacts/runs/<run-id>/generated/subscribers/ue1-subscriber.json
```

---

## 测试

```bash
# 运行全部测试
python -m pytest tests/ -v

# 运行特定测试
python -m pytest tests/test_renderer.py -v
python -m pytest tests/test_graph_mapper.py -v
python -m pytest tests/test_log_parser.py -v
```

测试覆盖：
- `test_renderer.py`：场景渲染与产物生成验证
- `test_schema.py`：TickSnapshot / SimEvent 数据协议校验
- `test_graph_mapper.py`：图快照映射逻辑
- `test_graph_store.py`：PostgreSQL 图存储集成
- `test_log_parser.py`：free5GC / UERANSIM 日志解析器
- `test_subscriber_bootstrap.py`：订阅者 payload 构建
- `test_writer.py`：本地归档存储

---

## 许可证

详见项目根目录的 LICENSE 文件。
