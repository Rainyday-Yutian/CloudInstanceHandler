# coding: utf-8
"""
华为云 IAM (身份与访问管理) 模块
重构版本：统一使用 fetch_instance_info 模式
"""
import pandas as pd
import asyncio
from typing import List, Optional, Dict, Any
from pprint import pprint

from cloudinstancehandler.hwyun.HwyunInstance import HwyunInstance
from cloudinstancehandler.common.funcs import get_time_range

from huaweicloudsdkcore.auth.credentials import GlobalCredentials
from huaweicloudsdkcore.exceptions import exceptions
from huaweicloudsdkiam.v3.region.iam_region import IamRegion
from huaweicloudsdkiam.v3 import *


class IAM(HwyunInstance):
    def __init__(self, access_key, secret_key) -> None:
        super().__init__(access_key, secret_key)
        self.regions = pd.DataFrame()
        self.projects = pd.DataFrame()

    def fetch_regions(
        self,
    ) -> List[Dict[str, Any]]:
        credentials = GlobalCredentials(self.access_key, self.secret_key)
        client = IamClient.new_builder() \
            .with_credentials(credentials) \
            .with_region(IamRegion.value_of("cn-north-4")) \
            .build()

        raw_records = []
        try:
            request = KeystoneListRegionsRequest()
            response_json = client.keystone_list_regions(request).to_json_object()
            raw_records = response_json.get('regions', [])
        except exceptions.ClientRequestException as e:
            print(f"[Error] Failed to fetch IAM regions: Request id: {e.request_id}\n"
                  f"Status code: {e.status_code}, error code: {e.error_code}\n"
                  f"Error message: {e.error_msg}")

        self.regions = pd.DataFrame(raw_records)
        return raw_records

    def fetch_instance_info(
        self,
        name: Optional[str] = None,
        enabled: Optional[bool] = None,
        password_expires_at: Optional[str] = None,
        auto_accumulate: bool = True
    ) -> List[Dict[str, Any]]:
        """
        获取 IAM 用户信息

        华为云API文档：https://console.huaweicloud.com/apiexplorer/#/openapi/IAM/doc?version=v3&api=KeystoneListUsers

        注意：此方法会先调用 fetch_projects() 从 self.projects 中提取 domain_id

        Args:
            name: IAM用户名（精确匹配），不指定则获取所有用户
            enabled: 是否启用用户（True/False），不指定则返回所有状态
            password_expires_at: 密码过期时间过滤条件
                格式："{operator}:{timestamp}"，例如 "lt:2016-12-08T22:02:00Z"
                operator: lt/lte/gt/gte/eq/neq
                timestamp: YYYY-MM-DDTHH:mm:ssZ
            auto_accumulate: 是否自动累加到 self.instance_info

        Returns:
            List[Dict]: 原始用户记录列表（完整数据）

        API参数说明：
            - domain_id: IAM用户所属账号ID（必需，从 self.projects 中获取）
            - name: IAM用户名（精确匹配，可选）
            - enabled: 是否启用用户（true/false，可选）
            - password_expires_at: 密码过期时间过滤（可选）
        """
        # 如果 projects 为空，先调用 fetch_projects
        if self.projects.empty:
            self.fetch_projects()

        # 从 self.projects 中提取第一个项目的 domain_id
        if not self.projects.empty:
            domain_id = self.projects.iloc[0].get('domain_id')
            if not domain_id:
                return []
        else:
            return []

        credentials = GlobalCredentials(self.access_key, self.secret_key)
        client = IamClient.new_builder() \
            .with_credentials(credentials) \
            .with_region(IamRegion.value_of("cn-north-4")) \
            .build()

        raw_records = []

        try:
            request = KeystoneListUsersRequest()
            request.domain_id = domain_id

            if enabled is not None:
                request.enabled = enabled
            if name is not None:
                request.name = name
            if password_expires_at is not None:
                request.password_expires_at = password_expires_at

            response_json = client.keystone_list_users(request).to_json_object()
            raw_records = response_json.get('users', [])

        except exceptions.ClientRequestException as e:
            print(f"[Error] Failed to fetch IAM users: Request id: {e.request_id}\n"
                  f"Status code: {e.status_code}, error code: {e.error_code}\n"
                  f"Error message: {e.error_msg}")

        if auto_accumulate and raw_records:
            self._accumulate_instance_info(raw_records)

        return raw_records

    def fetch_projects(self) -> List[Dict[str, Any]]:
        """
        获取当前账号的项目列表

        华为云API文档：https://console.huaweicloud.com/apiexplorer/#/openapi/IAM/doc?version=v3&api=KeystoneListProjects

        Returns:
            List[Dict]: 原始项目记录列表（完整数据）

        API参数说明：
            - 项目列表包含当前账号下的所有项目
            - 每个项目包含 domain_id（账号ID）、id（项目ID）、name（项目名称）等字段
        """
        credentials = GlobalCredentials(self.access_key, self.secret_key)
        client = IamClient.new_builder() \
            .with_credentials(credentials) \
            .with_region(IamRegion.value_of("cn-north-4")) \
            .build()

        raw_records = []

        try:
            request = KeystoneListProjectsRequest()
            response_json = client.keystone_list_projects(request).to_json_object()
            raw_records = response_json.get('projects', [])

        except exceptions.ClientRequestException as e:
            print(f"[Error] Failed to fetch projects: Request id: {e.request_id}\n"
                  f"Status code: {e.status_code}, error code: {e.error_code}\n"
                  f"Error message: {e.error_msg}")

        # 将项目列表保存到 self.projects
        if raw_records:
            self.projects = pd.DataFrame(raw_records)

        return raw_records
    
    # 华为云SDK原生不支持异步调用
    # 使用 asyncio.to_thread() + ThreadPoolExecutor 处理同步阻塞调用
    async def fetch_users_batch_async(
        self,
        user_name_list: List[str],
        max_concurrency: int = 10,
        auto_accumulate: bool = True
    ) -> List[Dict[str, Any]]:
        """
        异步批量查询用户信息（支持并发）

        通过并发调用 fetch_instance_info(name=xxx) 实现

        Args:
            user_name_list: 用户名列表
            max_concurrency: 最大并发数（默认10，可根据API限流调整）
            auto_accumulate: 是否自动累加到 self.instance_info

        Returns:
            List[Dict]: 所有用户的原始记录列表
        """
        if not user_name_list:
            print("[Warning] user_name_list is empty. Returning empty list.")
            return []

        semaphore = asyncio.Semaphore(max_concurrency)

        async def fetch_single(name: str) -> Optional[Dict]:
            """并发获取单个用户"""
            async with semaphore:
                try:
                    # 在线程池中调用同步方法 fetch_instance_info
                    def sync_fetch():
                        return self.fetch_instance_info(name=name, auto_accumulate=False)

                    users = await asyncio.to_thread(sync_fetch)
                    if users:
                        return users[0]  # 返回第一个用户（精确匹配，应该只有一个）
                    return None
                except Exception as e:
                    print(f"[Error] Failed to fetch user {name}: {e}")
                    return None

        # 创建所有并发任务
        tasks = [fetch_single(name) for name in user_name_list]

        # 并发执行（忽略异常）
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # 过滤有效结果
        raw_records = []
        for result in results:
            if isinstance(result, dict):
                raw_records.append(result)
            elif isinstance(result, Exception):
                print(f"[Error] Fetch task failed: {result}")

        if auto_accumulate and raw_records:
            self._accumulate_instance_info(raw_records)

        return raw_records

    def fetch_users_batch(
        self,
        user_names: List[str],
        max_concurrency: int = 10,
        auto_accumulate: bool = True
    ) -> List[Dict[str, Any]]:
        """
        批量查询用户信息（同步接口）

        注意：此方法内部会调用异步方法

        Args:
            user_name_list: 用户名列表
            max_concurrency: 最大并发数（默认10，可根据API限流调整）
            auto_accumulate: 是否自动累加到 self.instance_info

        Returns:
            List[Dict]: 所有用户的原始记录列表
        """
        if not user_names:
            print("[Warning] user_name_list is empty. Returning empty list.")
            return []

        # 运行异步并发方法
        try:
            loop = asyncio.get_running_loop()
            return loop.run_until_complete(
                self.fetch_users_batch_async(user_names, max_concurrency, auto_accumulate)
            )
        except RuntimeError:
            # 没有运行的事件循环，创建新循环
            return asyncio.run(
                self.fetch_users_batch_async(user_names, max_concurrency, auto_accumulate)
            )

