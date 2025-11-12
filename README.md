# CloudInstanceHandler

> 统一接口的多云实例监控指标采集工具  
> 支持阿里云、华为云等主流云厂商，提供标准化的数据获取与聚合统计能力。

[![Python Version](https://img.shields.io/badge/python-3.8%2B-blue)](https://www.python.org/)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

---

## 🌟 特性
- **统一接口**：通过 `getMetricData()` 方法一致地获取不同云平台的监控数据
- **多云支持**：已支持阿里云（AliyunInstance）、华为云（HwyunInstance）、火山云（VolcInstance）
- **自动分页**：智能处理云平台 API 的数据点限制（如华为云 ≤3000 点/请求）
- **灵活聚合**：支持按实例、时间等维度进行 `max`/`min`/`avg`/`95分位` 等统计
- **模块化设计**：基于抽象基类 `BasicDataFrame`，易于扩展新云平台
- **生产就绪**：包含错误重试、限流等待、类型校验等健壮性设计

<br>

## 📦 安装
### 拉取并安装
```bash
git clone https://github.com/Rainyday-Yutian/CloudInstanceHandler.git
cd CloudIntanceHandler
pip install -e .
```

<!-- ## requirements
```txt
``` -->
<br>

## 🧠 核心数据结构与工作流
`BasicDataFrame` 是本项目的核心抽象基类，定义了多云监控数据采集的统一数据模型与操作范式。它通过三个关键属性管理数据生命周期：

### 1. `self.InsInfo` — 实例元信息仓库
- **用途**：存储从云平台获取的**实例基础信息**（如 ID、区域、类型、状态等）。
- **数据来源**：由子类实现的 `getInsInfo()` 方法填充。
- **累积行为**：  
  每次调用 `getInsInfo()` **不会清空已有数据**，而是将新获取的实例信息**追加**到 `self.InsInfo` 中。  
  因此，`self.InsInfo` 本质上是一个**累积型数据池**，可跨多次调用聚合不同区域的实例。
<!-- - **扩展设计**：  
  后续方法（如 `getMoreInfo()`）将支持 `merge=True` 参数，自动将扩展信息与 `self.InsInfo` 合并，保持数据完整性。 -->

> ✅ 适用场景：先分批拉取多个地域的 ECS 实例，再统一查询其监控指标。


### 2. `self.InsData` — 实例监控数据视图

- **用途**：存储**结合元信息与监控指标**的最终分析数据集。
- **数据来源**：由 `getInsData()` 方法生成，其内部会：
  1. 读取当前 `self.InsInfo` 中的所有实例；
  2. 根据实例类型、产品特性（如 Redis 集群版 vs 标准版）、字段映射（如共享带宽使用 `BandwidthPackageId`）等逻辑，**动态调用 `getMetricData()`**；
  3. 将返回的时序指标与 `self.InsInfo` **按实例 ID 对齐并合并**；
  4. 将结果写入 `self.InsData`。
- **覆盖行为**：  
  每次调用 `getInsData()` **会先清空 `self.InsData`**，再重新生成。  
  因此，`self.InsData` 始终反映**基于当前 `self.InsInfo` 的最新监控视图**。
- **灵活性**：  
  子类可重写 `getInsData()` 以适配不同云产品的特殊逻辑（如指标命名差异、维度字段名不同等）。

> ✅ 适用场景：在已加载所有 ECS 实例后，一键生成包含 CPU、内存、网络等指标的完整分析表。

### 3. `self.MetricData` — 原始时序指标缓存（可选）

- **用途**：临时缓存 `getMetricData()` 返回的原始监控数据（通常为时间序列）。
- **使用方式**：可直接用于 `statisticMetricData()` 进行聚合统计，或作为中间结果供调试。
- **非持久化**：不参与 `saveInsInfo()` / `saveInsData()` 的自动保存流程。

<br>

## 🚀 典型工作流示例

```python
# 1. 初始化客户端
# client = AliyunInstance(ak="xxx", sk="xxx")
client = ECS(ak="xxx", sk="xxx")

# 2. 分批加载实例（累积到 InsInfo）
client.getInsInfo(region_id="cn-hangzhou")
client.getInsInfo(region_id="cn-shanghai")  # InsInfo 现在包含两地实例

# 3. 生成带监控指标的完整数据集（覆盖 InsData）
client.getInsData(metric_name="CPUUtilization", period="300")

# 4. 保存结果
client.saveInsInfo()   # 保存所有实例元信息
client.saveInsData()   # 保存带指标的分析数据
```

<br>

## 💡 设计理念
- **分层清晰**：元信息（`InsInfo`）与指标数据（`InsData`）分离，便于复用与调试。
- **云无关抽象**：通过子类实现差异逻辑，基类提供统一接口与数据管理。
- **面向分析**：内置 `statisticMetricData()` 支持常用聚合（max/avg/95分位等），直接输出分析就绪数据。

<br>

## 🖼️ 更多示例
- [华为云全域带宽指标推送 Prometheus](examples/push_hwyun_bwp_bandwidth_to_prometheus.py)


<br>

## 📝 CHANGELOG
### v1.0.0
* 重构，通过采用包布局和抽象基类的方式对代码库进行重新组织和调整
* 修复华为共享带宽GEIP带宽排名中，因GEIP中有IPV6-ECS数据未适配而报错的问题


<br>

## 📜 许可证
* 本项目基于 Apache License 2.0 开源。

<br>

