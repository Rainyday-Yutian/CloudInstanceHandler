# CloudInstanceHandler

统一接口的多云实例监控指标采集工具，支持阿里云、华为云、火山云、腾讯云等主流云厂商，提供标准化的数据获取与聚合统计能力。

[![Python Version](https://img.shields.io/badge/python-3.8%2B-blue)](https://www.python.org/)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

## 特性

- **统一接口**：通过 `fetch_instance_info()` / `get_metric_data()` / `statistic_metric_data()` 一致地获取不同云平台的实例与监控数据
- **多云支持**：阿里云（AliyunInstance）、华为云（HwyunInstance）、火山云（VolcInstance）、腾讯云（TencentCloudInstance，部分产品）
- **模块化设计**：按 `<云厂商>/<产品>.py` 组织（如 `aliyun/ECS.py`、`volcengine/VPC.py`），基于抽象基类 `BasicDataFrame`，易于扩展新云平台
- **自动分页**：智能处理云平台 API 的数据点限制（如华为云 ≤3000 点/请求）与各类分页机制（NextToken / marker / page_number）
- **灵活聚合**：支持按实例、时间等维度进行 `max`/`min`/`avg`/`95分位` 等统计
- **异步并发**：阿里云、火山云新模块采用 async 实现，内置并发控制
- **生产就绪**：包含错误重试、限流等待、类型校验等健壮性设计

## 安装

```bash
git clone https://github.com/Rainyday-Yutian/CloudInstanceHandler.git
cd CloudInstanceHandler
pip install -e .
```

## 项目结构

```
cloudinstancehandler/
├── common/                     # 公共层
│   ├── BasicDataFrame.py       # 核心抽象基类（instance_info / instance_data / metric_data 数据模型）
│   ├── funcs.py                # get_time_range 等工具函数
│   └── deprecated.py           # camelCase → snake_case 兼容别名装饰器
├── aliyun/                     # 阿里云（基类 AliyunInstance，异步实现）
│   ├── ECS.py                  # ECS / ENI / Disks / AutoSnapshotPolicy / Snapshot
│   ├── VPC.py                  # VPC / CBWP / EIP / IPv6
│   ├── SLB.py                  # 负载均衡
│   ├── NGW.py                  # NAT 网关
│   ├── AntiDDoS.py             # DDoS 防护
│   ├── DDosDip.py              # DDoS 高防（新BGP）
│   └── RAM.py                  # 访问控制
├── hwyun/                      # 华为云（基类 HwyunInstance）
│   ├── ECS.py  ELB.py  IAM.py
│   └── VPC.py                  # GEIP / GBWP（全球带宽包）/ PORT
├── tencentcloud/               # 腾讯云（基类 TencentCloudInstance）
│   ├── VPC.py                  # BWP / EIP / ENI
│   ├── CAM.py                  # 访问管理
│   └── CVM.py                  # 占位，待迁移
└── volcengine/                 # 火山云（基类 VolcInstance，全异步实现）
    ├── ECS.py                  # ECS / ENI / Disks
    ├── VPC.py                  # EIP / BandwidthPackage
    ├── RDS.py  Redis.py  CLB.py  NAT.py  IAM.py
    ├── VolcObserve.py          # 可观测告警规则
    ├── Resource.py             # ResourceCenter 资源搜索
    └── common_request.py       # VCM 通用签名请求
examples/                       # 使用示例
sender/                         # 消息通知（钉钉机器人等）
docs/                           # 开发文档（迁移说明等）
```

## 设计范式

### 类层次

```
BasicDataFrame                   # 数据模型 + 通用操作（统计、保存、过滤）
    └── <Cloud>Instance          # AliyunInstance / HwyunInstance / TencentCloudInstance / VolcInstance
            └── 产品类            # ECS / VPC / RDS / GBWP ...
```

### 统一约定（四朵云一致）

| 维度 | 约定 |
|---|---|
| 凭证 | `access_key` / `secret_key` |
| 实例元信息 | `self.instance_info`（累积型 DataFrame） |
| 原始指标缓存 | `self.metric_data` |
| 分析数据集 | `self.instance_data` |
| 实例拉取 | `fetch_instance_info()` |
| 指标查询 | `get_metric_data()` |
| 聚合统计 | `statistic_metric_data()` |
| 保存 | `save_instance_info()` / `save_instance_data()` |
| 云厂商标识 | `csp_name` 类属性 |

### 异步策略（按各云 SDK 能力适配）

- **阿里云 / 火山云**：新模块为 async 实现（火山云 SDK 无原生异步，通过 `asyncio.run_in_executor` 包装 + `Semaphore` 并发控制）
- **华为云 / 腾讯云**：同步实现

## 核心数据结构与工作流

`BasicDataFrame` 是本项目的核心抽象基类，定义了多云监控数据采集的统一数据模型与操作范式。它通过三个关键属性管理数据生命周期：

### 1. `self.instance_info` — 实例元信息仓库

- **用途**：存储从云平台获取的**实例基础信息**（如 ID、区域、类型、状态等）。
- **数据来源**：由子类实现的 `fetch_instance_info()` 方法填充。
- **累积行为**：每次调用 `fetch_instance_info()` **不会清空已有数据**，而是将新获取的实例信息**追加**到 `self.instance_info` 中。因此，`self.instance_info` 本质上是一个**累积型数据池**，可跨多次调用聚合不同区域的实例。

> 适用场景：先分批拉取多个地域的 ECS 实例，再统一查询其监控指标。

### 2. `self.metric_data` — 原始时序指标缓存

- **用途**：临时缓存 `get_metric_data()` 返回的原始监控数据（通常为时间序列）。
- **使用方式**：可直接作为 `statistic_metric_data()` 的默认数据源进行聚合统计，或作为中间结果供调试。
- **非持久化**：不参与 `save_instance_info()` / `save_instance_data()` 的自动保存流程。

### 3. `self.instance_data` — 实例监控数据视图

- **用途**：存储**结合元信息与监控指标**的最终分析数据集。
- **数据来源**：由 `instance_info` 与统计后的指标数据按实例 ID 对齐合并生成。
- **灵活性**：子类可重写相关逻辑以适配不同云产品的特殊需求（如指标命名差异、维度字段名不同等）。

> 适用场景：在已加载所有 ECS 实例后，一键生成包含 CPU、内存、网络等指标的完整分析表。

## 典型工作流示例

以阿里云 ECS 为例（异步接口；华为云/腾讯云为同步调用，火山云为全异步，参数细节见各模块 docstring）：

```python
import asyncio
from cloudinstancehandler.aliyun.ECS import ECS
from cloudinstancehandler.common.funcs import get_time_range

async def main():
    # 1. 初始化客户端（凭证建议通过环境变量注入）
    client = ECS("<access_key>", "<secret_key>")

    # 2. 分批加载实例（累积到 client.instance_info）
    await client.fetch_instance_info(region_id="cn-hangzhou")
    await client.fetch_instance_info(region_id="cn-shanghai")

    # 3. 查询监控指标（返回 DataFrame，并缓存到 client.metric_data）
    time_range = get_time_range(start_offset_hours=1)
    df = client.get_metric_data(
        region_id="cn-hangzhou",
        instance_list=client.instance_info["InstanceId"].to_list(),
        metric_name="CPUUtilization",
        TimeDict=time_range,
        period="300",
    )

    # 4. 聚合统计（max/avg/95分位等）并保存结果
    client.statistic_metric_data(field_name="Maximum", statistics_approach=["max", "avg"])
    client.save_instance_info()   # 保存所有实例元信息
    client.save_instance_data()   # 保存带指标的分析数据

asyncio.run(main())
```

## 设计理念

- **分层清晰**：元信息（`instance_info`）与指标数据（`metric_data` / `instance_data`）分离，便于复用与调试。
- **云无关抽象**：通过子类实现差异逻辑，基类提供统一接口与数据管理。
- **面向分析**：内置 `statistic_metric_data()` 支持常用聚合（max/avg/95分位等），直接输出分析就绪数据。

## 更多示例

- [华为云全域带宽指标推送 Prometheus](examples/push_hwyun_bwp_bandwidth_to_prometheus.py)

## 命名规范说明

自重构后，本项目统一采用 **snake_case** 命名（更符合 Python 规范）：

| 老命名（camelCase，已废弃） | 新命名（snake_case） |
|---|---|
| `getMetricData()` | `get_metric_data()` |
| `getMetricList()` | `get_metric_list()` |
| `getTimeDict()` | `get_time_range()` |
| `getInsInfo()` | `fetch_instance_info()` |
| `self.InsInfo` / `self.InsData` / `self.MetricData` | `self.instance_info` / `self.instance_data` / `self.metric_data` |
| `saveInsInfo()` / `saveInsData()` | `save_instance_info()` / `save_instance_data()` |

- 老 camelCase 方法名在基类中仍保留为**兼容别名**，调用会触发 `DeprecationWarning`
  （Python 默认不显示该类告警，可用 `python -W always::DeprecationWarning` 查看），功能不受影响，但将在未来版本移除。
- 各云的 `*insdatahandler.py` 单体老文件（如 `volcinsdatahandler.py`）**已从代码库移除并归档**，
  请改用 `cloudinstancehandler.<云厂商>.<模块>` 下的新实现（如 `cloudinstancehandler.volcengine.VPC`）。

## 变更日志

### v1.1.0

- **多云模块化迁移**：将单体 `*insdatahandler.py` 全量解耦为 `cloudinstancehandler/<云厂商>/<产品>.py` 模块结构（45 个文件，约 +8400/-3600 行）
  - 阿里云：新增 ECS（ECS/ENI/Disks/快照）、VPC（VPC/CBWP/EIP/IPv6）、SLB、NGW、AntiDDoS、DDosDip 模块，RAM 更新（异步实现）
  - 华为云：新增 ECS、ELB、IAM、VPC（GEIP/GBWP/PORT）模块
  - 腾讯云：基类重构为 `TencentCloudInstance`，新增 VPC（BWP/EIP/ENI）、CAM 模块（CVM 待迁移）
  - 火山云：新增 ECS、RDS、Redis、CLB、NAT、VPC（EIP/BandwidthPackage）、IAM、VolcObserve、Resource、common_request 模块（全异步实现）
- 命名统一 snake_case，老 camelCase 名称保留为带 `DeprecationWarning` 的兼容别名（`common/deprecated.py`）
- 老的 `*insdatahandler.py` 单体文件与 `common/common.py` 从代码库移除归档；examples 适配新模块结构
- 迁移细节与技术要点见 [docs/MIGRATION_SUMMARY.md](docs/MIGRATION_SUMMARY.md)

### v1.0.0

- 重构，通过采用包布局和抽象基类的方式对代码库进行重新组织和调整
- 修复华为共享带宽GEIP带宽排名中，因GEIP中有IPV6-ECS数据未适配而报错的问题

## 许可证

本项目基于 Apache License 2.0 开源，详见 [LICENSE](LICENSE)。
