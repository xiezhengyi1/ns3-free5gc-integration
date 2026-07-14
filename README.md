# ns3-free5gc-integration

本项目使用唯一的 split control-plane/user-plane 运行模型集成 free5GC、UERANSIM 和 ns-3 5G-LENA：

- free5GC 与 UERANSIM 建立真实注册、PFCP 和 PDU Session 控制面状态。
- ns-3 独立运行 NR 用户面，并由控制面事件启用或停用对应 flow。
- 场景显式声明多对多 gNB-UPF N3 边；每条 flow 通过 `upf_ref` 选择已连接的 UPF。
- Writer 将控制面事件、ns-3 KPI 和拓扑快照写入统一结果流。
- 快速重置接口复用 Compose 网络和已构建的 ns-3，仅重置本次 run 的进程与状态。

项目不再提供非 split 模式、旧 TAP gate、旧 GTP-U shadow peer 或兼容 URL。

## 快速开始

```bash
python -m venv .venv
.venv/bin/pip install -e .

python scripts/render_split_run.py \
  scenarios/split_mode/s1_basic_single_slice.yaml \
  --run-id demo

python scripts/start_split_mode.py \
  scenarios/split_mode/s1_basic_single_slice.yaml \
  --run-id demo
```

需要持续等待后台组件、但不希望启动状态和日志输出到当前终端时，加上 `--wait-background`：

```bash
python scripts/start_split_mode.py \
  scenarios/split_mode/s1_basic_single_slice.yaml \
  --run-id demo \
  --wait-background
```

组件日志仍会写入 `artifacts/runs/demo/logs/`。

默认 ns-3 路径在基础场景的 `ns3.ns3_root` 中配置，也可以通过运行清单的 `NS3_ROOT` 环境覆盖。

## 场景结构

`scenarios/split_mode/*.yaml` 只定义 split 运行参数，并通过 `base_scenario` 引用控制面和业务场景：

```yaml
name: basic-split
scenario_id: basic-split
base_scenario: ../s1_basic_single_slice.yaml

ns3:
  scratch_name: nr_multignb_multiupf_split
  policy_reload_ms: 100

radio:
  scheduler_type: pf
  tdd_pattern: DL|UL|UL|F|DL|UL|UL|F|
```

基础场景中的 N3 和 flow 选择示例：

```yaml
upfs:
  - name: upf-a
  - name: upf-b

gnbs:
  - name: gNB-1
    backhaul_upfs: [upf-a, upf-b]

flows:
  - flow_id: flow-1
    ue_name: ue-1
    upf_ref: upf-b

n3_network:
  name: n3net
  cidr: 10.201.1.0/29
```

渲染器会把完整 N3 边列表传给 ns-3。若 flow 选择的 UPF 未连接到其 serving gNB，场景加载或 ns-3 启动会直接失败。

## 快速重置

渲染后的 `run-manifest.split.json` 包含快速重置配置。启动监督器：

```bash
python -m bridge.orchestrator.fast_reset serve \
  artifacts/runs/demo/run-manifest.split.json \
  --host 127.0.0.1 \
  --port 18081
```

重置场景：

```bash
curl -X POST http://127.0.0.1:18081/v1/reset
```

首次调用执行 cold start；后续调用重启现有 Compose 服务、恢复订阅者和应用数据，并从仿真时间零启动新一代 ns-3 进程。

## 目录

```text
adapters/free5gc_ueransim/  free5GC/UERANSIM 配置与 Compose 适配
bridge/common/              场景、拓扑和协议模型
bridge/orchestrator/        控制面资产渲染与快速重置
bridge/split_mode/          唯一运行模式的配置、清单、gate 和 runner
bridge/writer/              事件、KPI 和图快照写入
sim/ns3/                    唯一 ns-3 split 程序
scenarios/                  基础场景与 split 入口场景
scripts/                    split 构建、渲染和启动脚本
tests/                      Python 回归测试
```

## 验证

```bash
python -m unittest discover -s tests
bash scripts/build_ns3_split.sh
```
