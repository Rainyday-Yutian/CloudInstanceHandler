# coding: utf-8

import asyncio
from typing import Any, Dict, List, Optional

import pandas as pd

from cloudinstancehandler.volcengine.VolcInstance import VolcInstance

import volcenginesdkcore
import volcenginesdkvolcobserve
from volcenginesdkcore.rest import ApiException


class OBServer(VolcInstance):
    """
    火山云可观测平台（VolcObserve）告警规则

    重构要点（原 volcinsdatahandler.OBServer）：
    - getRules → fetch_rules（异步，page_number 分页）
      注意行为变更：老实现将新数据前插到 InsInfo（concat([df_data, self.InsInfo])），
      现改为与其他模块一致的正常追加（_accumulate_instance_info）
    - expandInsInfo → expand_instance_info（结果存 instance_data，原为 InsData）
    """

    def __init__(self, access_key: str, secret_key: str) -> None:
        super().__init__(access_key, secret_key)

    def _create_api_instance(self, region_id: str) -> volcenginesdkvolcobserve.VOLCOBSERVEApi:
        """创建 VolcObserve API 客户端（每请求独立 ApiClient，避免并发下 set_default 全局状态竞争）"""
        configuration = self._create_configuration(region_id)
        return volcenginesdkvolcobserve.VOLCOBSERVEApi(volcenginesdkcore.ApiClient(configuration))

    async def fetch_rules(
        self,
        rule_name: Optional[str] = None,
        region_id: str = "cn-beijing",
        page_size: int = 100,
        auto_accumulate: bool = True
    ) -> List[Dict[str, Any]]:
        """
        获取告警规则列表（异步，page_number 分页）

        Args:
            rule_name: 规则名称过滤（可选）
            region_id: 发起请求的地域，默认 cn-beijing
            page_size: 每页返回数量
            auto_accumulate: 是否自动累积到 instance_info（追加，行为变更见类 docstring）

        Returns:
            List[Dict[str, Any]]: 原始告警规则记录列表
        """
        api_instance = self._create_api_instance(region_id)
        loop = asyncio.get_running_loop()

        list_rules_request = volcenginesdkvolcobserve.ListRulesRequest()
        list_rules_request.page_size = page_size
        if rule_name:
            list_rules_request.rule_name = rule_name

        raw_records: List[Dict[str, Any]] = []
        page_number = 1
        has_next_page = True

        while has_next_page:
            list_rules_request.page_number = page_number
            try:
                def sync_fetch():
                    response = api_instance.list_rules(list_rules_request)
                    return response.to_dict()

                response_dict = await loop.run_in_executor(None, sync_fetch)
                data = response_dict.get('data') or []
                raw_records.extend(data)
                total_count = response_dict.get('total_count') or 0
                if total_count <= page_number * page_size:
                    has_next_page = False
                else:
                    page_number += 1
            except ApiException as e:
                print(f"Exception when fetching observe rules: {e}\n")
                break
            except Exception as e:
                print(f"Unexpected error when fetching observe rules: {e}\n")
                break

        if auto_accumulate and raw_records:
            self._accumulate_instance_info(raw_records)

        return raw_records

    def expand_instance_info(self) -> pd.DataFrame:
        """
        展开 instance_info 中告警规则的 original_dimensions 字段

        提取 original_dimensions 中的 ResourceID 并 explode 为逐资源行，
        结果存入 self.instance_data 并返回。

        Returns:
            pd.DataFrame: 展开后的数据表
        """
        if self.instance_info.empty:
            print("[Info] instance_info is empty. Please call fetch_rules first.")
            return pd.DataFrame()

        df = self.instance_info.copy()
        df['original_dimensions'] = df['original_dimensions'].apply(
            lambda x: {} if pd.isna(x) else x
        )
        df['ResourceID'] = df['original_dimensions'].apply(
            lambda x: x.get('ResourceID', []) if isinstance(x, dict) else []
        )
        df_exploded = df.explode('ResourceID').reset_index(drop=True)
        self.instance_data = df_exploded
        return df_exploded
