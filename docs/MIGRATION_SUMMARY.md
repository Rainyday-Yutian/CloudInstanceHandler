# 多云模块化迁移说明（开发者向）

> 本文档记录本轮从单体 `*insdatahandler.py` 到 `cloudinstancehandler/<云厂商>/<产品>.py` 模块化架构的完整迁移过程，
> 主要面向开发者（自己）留档参考。对外简要说明见 [README.md](../README.md) 的 CHANGELOG。

## 📋 迁移概述

- **起点**：每个云厂商一个单体大文件（`aliyuninsdatahandler.py` / `hwyuninsdatahandler.py` / `volcinsdatahandler.py` / `awsinsdatahandler.py`），职责混杂、命名不统一（camelCase）、难以扩展
- **终点**：按 `<云厂商>/<产品>.py` 拆分的模块化结构，四朵云（阿里云、华为云、腾讯云、火山云）统一设计范式，命名统一 snake_case
- **规模**：45 个文件变更，约 +8400 / -3600 行
- **老代码去向**：4 个 `*insdatahandler.py` + `common/common.py`（重构前底稿）已移至本地 `archive/` 目录（已被 `.gitignore` 忽略，**不随仓库发布**）。因属工程代码且部分逻辑尚未完成迁移，仅保留给开发参考

## 📁 迁移后项目结构

```
cloudinstancehandler/
├── common/                          # 公共层
│   ├── BasicDataFrame.py            # 核心抽象基类：instance_info / instance_data / metric_data 数据模型
│   ├── funcs.py                     # get_time_range 等工具函数（唯一真源）
│   └── deprecated.py                # deprecated_alias 装饰器：camelCase 兼容别名
├── aliyun/                          # 阿里云（基类 AliyunInstance）
│   ├── ECS.py                       # ECS / ENI / Disks / AutoSnapshotPolicy / Snapshot
│   ├── VPC.py                       # VPC / CBWP / EIP / IPv6
│   ├── SLB.py                       # SLB
│   ├── NGW.py                       # NAT 网关
│   ├── AntiDDoS.py  DDosDip.py      # DDoS 防护
│   ├── RAM.py                       # RAM
│   └── old_IPv6.py                  # ⚠️ 旧版 IPv6 实现，待清理
├── hwyun/                           # 华为云（基类 HwyunInstance）
│   ├── ECS.py  ELB.py  IAM.py
│   └── VPC.py                       # GEIP / GBWP（全球带宽包）/ PORT
├── tencentcloud/                    # 腾讯云（基类 TencentCloudInstance）
│   ├── VPC.py                       # BWP / EIP / ENI
│   ├── CAM.py                       # CAM
│   └── CVM.py                       # ⚠️ 空占位，尚未迁移
└── volcengine/                      # 火山云（基类 VolcInstance，全异步实现）
    ├── ECS.py                       # ECS / ENI / Disks
    ├── VPC.py                       # EIP / BandwidthPackage
    ├── RDS.py  Redis.py  CLB.py  NAT.py  IAM.py
    ├── VolcObserve.py               # OBServer（可观测告警规则）
    ├── Resource.py                  # ResourceCenter 资源搜索
    └── common_request.py            # VCM 通用签名请求类
```

## 🏗️ 统一设计范式

### 类层次

```
BasicDataFrame (common/BasicDataFrame.py)      # 数据模型 + 通用操作
    └── <Cloud>Instance                        # AliyunInstance / HwyunInstance / TencentCloudInstance / VolcInstance
            └── 产品类                          # ECS / VPC / RDS / GBWP ...
```

### 统一约定

| 维度 | 统一后 | 迁移前（老代码） |
|---|---|---|
| 凭证命名 | `access_key` / `secret_key` | AK / SK 混用 |
| 实例元信息 | `self.instance_info`（累积型 DataFrame） | `self.InsInfo` |
| 指标缓存 | `self.metric_data` | `self.MetricData` |
| 分析数据集 | `self.instance_data` | `self.InsData` |
| 实例拉取 | `fetch_instance_info()` | `getInsInfo()` |
| 指标查询 | `get_metric_data()` | `getMetricData()` |
| 聚合统计 | `statistic_metric_data()` | `statisticMetricData()` |
| 保存 | `save_instance_info()` / `save_instance_data()` | `saveInsInfo()` / `saveInsData()` |
| 配置创建 | 基类统一方法（如火山云 `_create_configuration()`） | 每处新建 |
| 云厂商标识 | `csp_name` 类属性 | 无 |

### 命名规范与兼容层

- 全量 camelCase → snake_case（各云基类指标方法、`common/funcs.py::getTimeDict` → `get_time_range` 等）
- 老名称通过 `common/deprecated.py` 的 `deprecated_alias` 装饰器保留为兼容别名，调用触发
  `DeprecationWarning`（默认不可见，`python -W always::DeprecationWarning` 查看），未来版本移除
- `AliyunInstance.MetricData` 老属性**不提供**兼容适配（老代码已归档，新代码统一用 `metric_data`）

### 异步策略（各云 SDK 能力不同，按云适配）

- **火山云**：SDK 不支持原生异步 → `asyncio.run_in_executor` 包装同步调用 + `asyncio.Semaphore` 并发控制（默认 10）；Resource 模块用 `asyncio.to_thread`
- **阿里云**：新模块 `fetch_instance_info` / `fetch_regions_async` 为 async 实现（Tea SDK 异步客户端）
- **华为云 / 腾讯云**：同步实现

## 🔍 各云迁移明细

### 阿里云
- 基类 `AliyunInstance`：指标方法重命名（`get_metric_list` / `get_metric_data` / `get_metric_last_next_token`），指标缓存统一为 `self.metric_data`
- 新增产品模块：`ECS.py`（ECS/ENI/Disks/AutoSnapshotPolicy/Snapshot，async）、`VPC.py`（VPC/CBWP/EIP/IPv6，含 `client_token` 幂等参数）、`SLB.py`、`NGW.py`、`AntiDDoS.py`、`DDosDip.py`；`RAM.py` 更新
- `old_IPv6.py` 为旧版 IPv6 实现暂存，待清理

### 华为云
- 基类 `HwyunInstance`：`getMetricData` → `get_metric_data`
- 新增：`ECS.py`、`ELB.py`、`IAM.py`（含 `password_expires_at` 过滤）、`VPC.py`（GEIP/GBWP/PORT；GBWP 支持 `name_like` 服务端模糊过滤、marker/offset 双分页）
- 认证按服务区分 `GlobalCredentials`（IAM/GEIP 全局服务）与 `BasicCredentials`（区域服务）

### 腾讯云
- 基类重构：`TencentInstance.py` → `TencentCloudInstance.py`（`get_metric_data` snake_case）
- 新增：`VPC.py`（BWP/EIP/ENI）、`CAM.py`
- 删除旧的 `EIP.py` / `ENI.py`（功能并入新 `VPC.py`）
- **`CVM.py` 为空占位文件，CVM 尚未迁移**

### 火山云（全量迁移，全异步）
- `ECS.py` — ECS/ENI/Disks（`fetch_instance_info` / `fetch_specification_info` / `extract_eni_from_instance_info` / `modify_instance_attribute`；`fetch_regions` next_token 分页，语义对齐 aliyun `fetch_regions_async`）
- `RDS.py` — `fetch_regions` / `fetch_instance_info` / `fetch_instance_detail_info` 异步并发
- `Redis.py` — `fetch_instance_info`
- `CLB.py` — `fetch_instance_info`（注入 `billing_type_text`）
- `NAT.py` — ID 分批 + page_number 分页；监控 VCM_NAT/natgateway（out_bps/in_bps）；补全 `EIP.query_binded_info` 的 Nat 名称回查
- `VPC.py` — EIP（`query_binded_info` 支持 NetworkInterface/EcsInstance/ClbInstance/Nat 四类绑定分支）+ BandwidthPackage（`extract_eip_from_instance_info` / `get_eip_bandwidth_rank` 异步重写）
- `VolcObserve.py` — OBServer（`fetch_rules` / `expand_instance_info`）
- `Resource.py` — ResourceCenter 资源搜索（`fetch_instance_info` / `fetch_resource_counts`，asyncio.to_thread）
- `common_request.py` — VCM 通用签名请求（`VCMServiceCommonRequest` / `VCMSearchResourcesRequest` / `VCMGetResourceCountsRequest`，修复缺失的 datetime 导入）
- `volcinsdatahandler.py` 已归档删除（原计划"标记废弃保留兼容"，最终随其余老文件一并移入 `archive/`）

#### 火山云关键技术点

**SDK 异步适配**（同步 SDK + 线程池包装）：

```python
async def _fetch_batch(batch_ids: List[str], sem: asyncio.Semaphore) -> pd.DataFrame:
    async with sem:
        def sync_fetch():
            response = api_instance.get_metric_data(request)
            return response.to_dict()
        response_json = await loop.run_in_executor(None, sync_fetch)
        return pd.DataFrame(response_json)
```

**数据字段适配**（与阿里云字段命名差异）：

| 阿里云字段 | 火山云字段 | 处理方式 |
|-----------|-----------|---------|
| InstanceId | instance_id | 保持统一 |
| RegionId | region_id | 火山云需要手动添加 |
| NetworkInterfaces | network_interfaces | 需要适配展开 |

**分页**：火山云使用 `next_token` 机制（`while next_token: ...`），与阿里云 NextToken 类似、与华为云 marker/offset 不同。

## 📦 老代码归档说明

| 文件 | 去向 | 说明 |
|---|---|---|
| `aliyuninsdatahandler.py` | `archive/`（本地） | 部分工程逻辑未完成迁移，留作参考 |
| `hwyuninsdatahandler.py` | `archive/`（本地） | 同上 |
| `volcinsdatahandler.py` | `archive/`（本地） | 同上 |
| `awsinsdatahandler.py` | `archive/`（本地） | AWS 尚无新结构对应实现 |
| `common/common.py` | `archive/`（本地） | 重构前底稿，零引用；`funcs.py` 为唯一真源 |

- `archive/` 已加入 `.gitignore`，不会推送到 GitHub；git 历史中仍可见旧版本（已审计，无敏感数据）
- `examples/push_hwyun_bwp_bandwidth_to_prometheus.py` 已改为从新结构导入（`hwyun.VPC.GBWP` + `common.funcs.get_time_range`），不再依赖老文件

## 🎯 待办事项

### 短期
- [x] 四朵云产品模块迁移（腾讯云 CVM 除外）
- [x] 更新示例代码（examples 适配新结构）
- [x] 老代码归档（`archive/`，不发布）
- [ ] 腾讯云 CVM 迁移（`CVM.py` 空占位）
- [ ] 清理 `aliyun/old_IPv6.py`
- [ ] 编写单元测试

### 中期
- [ ] AWS 新结构实现（当前仅存归档的 `awsinsdatahandler.py`）
- [ ] 性能测试和优化

### 长期
- [ ] 考虑使用原生异步 HTTP 库替代 run_in_executor
- [ ] 支持火山云的批量操作接口

## 📚 参考资料

- **火山云SDK**: [volcengine-python-sdk](https://github.com/volcengine/volcengine-python-sdk)
- **阿里云Tea SDK**: [alibabacloud-tea-python](https://github.com/aliyun/tea-python)
- **设计范式参考**: `cloudinstancehandler/aliyun/ECS.py`
