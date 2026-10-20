#!/usr/bin/env python
# coding=utf-8

import asyncio
from typing import List, Optional, Dict, Any
from pprint import pprint

from cloudinstancehandler.volcengine.VolcInstance import VolcInstance
from volcenginesdkcore.rest import ApiException
import volcenginesdkcore
import volcenginesdkiam


class IAM(VolcInstance):
    def __init__(self, access_key: str, secret_key: str, region_id: str = "cn-beijing") -> None:
        super().__init__(access_key, secret_key)
        self.region_id = region_id
        self.users: List[Dict[str, Any]] = []

    def _create_configuration(self, region_id: str = None) -> volcenginesdkcore.Configuration:
        """
        创建火山引擎配置对象（覆盖父类方法，支持 IAM 特定配置）
        """
        if region_id is None:
            region_id = self.region_id

        configuration = volcenginesdkcore.Configuration()
        configuration.ak = self.access_key
        configuration.sk = self.secret_key
        configuration.region = region_id
        return configuration

    def fetch_instance_info(
        self,
        user_name: Optional[str] = None,
        enabled: Optional[bool] = None,
        max_items: Optional[int] = 100,
        auto_accumulate: bool = True
    ) -> List[Dict[str, Any]]:
        """
        获取 IAM 用户信息

        火山引擎 IAM API 文档参考：
        - GetUser: 获取单个用户信息
        - ListUsers: 获取用户列表

        Args:
            user_name: IAM 用户名（精确匹配），不指定则获取所有用户
            enabled: 是否启用用户（True/False），不指定则返回所有状态
                注意：火山引擎 API 可能不支持此参数，作为预留接口
            max_items: 最大返回数量（默认 100），用于分页查询
            auto_accumulate: 是否自动累加到 self.instance_info

        Returns:
            List[Dict]: 原始用户记录列表（完整数据）

        API 参数说明：
            - user_name: 用户名（可选，用于精确查询）
            - max_items: 单次查询最大数量（分页参数）
            - marker: 分页标记（用于后续查询）
        """
        raw_records = []

        try:
            # 如果指定了用户名，使用 GetUser 精确查询
            if user_name is not None:
                user = self.fetch_user(user_name)
                if user:
                    raw_records = [user]
            else:
                # 否则使用 ListUsers 批量查询
                raw_records = self.fetch_all_users(max_items=max_items)

        except ApiException as e:
            print(f"[Error] Failed to fetch IAM users: {e}")
        except Exception as e:
            print(f"[Error] Unexpected error when fetching IAM users: {e}")

        if auto_accumulate and raw_records:
            self._accumulate_instance_info(raw_records)

        return raw_records

    def fetch_user(self, user_name: str) -> Optional[Dict[str, Any]]:
        """
        获取单个用户信息

        Args:
            user_name: 用户名

        Returns:
            Optional[Dict[str, Any]]: 用户信息字典（已去除外层嵌套），不存在返回 None

        Raises:
            ApiException: API 调用失败时抛出异常
        """
        configuration = self._create_configuration()
        volcenginesdkcore.Configuration.set_default(configuration)

        api_instance = volcenginesdkiam.IAMApi()
        get_user_request = volcenginesdkiam.GetUserRequest(
            user_name=user_name,
            pii_redaction=0,  # 0: 不脱敏；1: 脱敏
        )

        try:
            response = api_instance.get_user(get_user_request)
            if response and hasattr(response, 'to_dict'):
                response_dict = response.to_dict()
                # 去除外层嵌套，直接返回 user 字段
                if 'user' in response_dict:
                    return response_dict['user']
                return response_dict
            return None
        except ApiException as e:
            if "EntityNotExist" in str(e) or "UserNotExist" in str(e):
                return None
            raise

    def fetch_all_users(
        self,
        max_items: int = 100,
        marker: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        获取所有用户信息（分页查询）

        Args:
            max_items: 单次查询最大数量（默认 100）
            marker: 分页标记，用于后续查询（首次调用传 None）

        Returns:
            List[Dict[str, Any]]: 所有用户信息列表

        Note:
            火山引擎 IAM API 支持分页查询，通过 marker 参数实现
        """
        configuration = self._create_configuration()
        volcenginesdkcore.Configuration.set_default(configuration)

        api_instance = volcenginesdkiam.IAMApi()
        list_users_request = volcenginesdkiam.ListUsersRequest(
            max_items=max_items,
            marker=marker,
            pii_redaction=0,  # 0: 不脱敏；1: 脱敏
        )

        records: List[Dict[str, Any]] = []

        try:
            while True:
                response = api_instance.list_users(list_users_request)

                # 提取用户列表并去除外层嵌套
                if hasattr(response, 'to_dict'):
                    response_dict = response.to_dict()
                    users = response_dict.get('users', [])
                elif hasattr(response, 'users'):
                    users = response.users
                else:
                    users = []

                for user in users:
                    if isinstance(user, dict):
                        # 已经是字典，直接添加
                        records.append(user)
                    elif hasattr(user, 'to_dict'):
                        # 转换为字典
                        records.append(user.to_dict())
                    else:
                        # 其他情况，尝试转换
                        records.append(dict(user))

                # 检查是否有更多数据
                has_more = getattr(response, 'is_truncated', False)
                if not has_more:
                    break

                # 更新 marker 继续查询
                next_marker = getattr(response, 'marker', None)
                if not next_marker:
                    break

                list_users_request.marker = next_marker

        except ApiException as e:
            print(f"[Error] Failed to fetch users: {e}")

        return records

    async def fetch_users_batch_async(
        self,
        user_names: List[str],
        max_concurrency: int = 10,
        auto_accumulate: bool = True
    ) -> List[Dict[str, Any]]:
        """
        异步批量查询用户信息（支持并发）

        通过并发调用 fetch_user() 实现

        Args:
            user_names: 用户名列表
            max_concurrency: 最大并发数（默认 10，可根据 API 限流调整）
            auto_accumulate: 是否自动累加到 self.instance_info

        Returns:
            List[Dict]: 所有用户的原始记录列表（已去除外层嵌套）
        """
        if not user_names:
            print("[Warning] user_names is empty. Returning empty list.")
            return []

        semaphore = asyncio.Semaphore(max_concurrency)

        async def fetch_single(name: str) -> Optional[Dict]:
            """并发获取单个用户"""
            async with semaphore:
                try:
                    # 在线程池中调用同步方法 fetch_user
                    loop = asyncio.get_running_loop()
                    user = await loop.run_in_executor(None, self.fetch_user, name)
                    return user
                except Exception as e:
                    print(f"[Error] Failed to fetch user {name}: {e}")
                    return None

        # 创建所有并发任务
        tasks = [fetch_single(name) for name in user_names]

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
            user_names: 用户名列表
            max_concurrency: 最大并发数（默认 10，可根据 API 限流调整）
            auto_accumulate: 是否自动累加到 self.instance_info

        Returns:
            List[Dict]: 所有用户的原始记录列表
        """
        if not user_names:
            print("[Warning] user_names is empty. Returning empty list.")
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

    def get_user_groups(self, user_name: str) -> List[Dict[str, Any]]:
        """
        获取用户所属的用户组

        Args:
            user_name: 用户名

        Returns:
            List[Dict[str, Any]]: 用户组列表（已去除外层嵌套）
        """
        configuration = self._create_configuration()
        volcenginesdkcore.Configuration.set_default(configuration)

        api_instance = volcenginesdkiam.IAMApi()
        list_groups_for_user_request = volcenginesdkiam.ListGroupsForUserRequest(
            user_name=user_name,
        )

        try:
            response = api_instance.list_groups_for_user(list_groups_for_user_request)
            groups = []
            if hasattr(response, 'to_dict'):
                response_dict = response.to_dict()
                group_list = response_dict.get('groups', [])
            elif hasattr(response, 'groups'):
                group_list = response.groups
            else:
                group_list = []

            for group in group_list:
                if isinstance(group, dict):
                    groups.append(group)
                elif hasattr(group, 'to_dict'):
                    groups.append(group.to_dict())
                else:
                    groups.append(dict(group))
            return groups
        except ApiException as e:
            print(f"[Error] Failed to get groups for user '{user_name}': {e}")
            return []

    def get_user_policies(self, user_name: str) -> Dict[str, List[Dict[str, Any]]]:
        """
        获取用户关联的策略（包括直接附加和通过组附加）

        Args:
            user_name: 用户名

        Returns:
            Dict[str, List[Dict[str, Any]]]: 策略信息，包含：
                - attached_policies: 直接附加的策略
                - group_policies: 通过组附加的策略
        """
        configuration = self._create_configuration()
        volcenginesdkcore.Configuration.set_default(configuration)

        api_instance = volcenginesdkiam.IAMApi()
        result = {
            "attached_policies": [],
            "group_policies": []
        }

        try:
            # 获取直接附加的策略
            list_attached_user_policies_request = volcenginesdkiam.ListAttachedUserPoliciesRequest(
                user_name=user_name,
            )
            response = api_instance.list_attached_user_policies(list_attached_user_policies_request)

            if hasattr(response, 'policies') and response.policies:
                for policy in response.policies:
                    if hasattr(policy, 'to_dict'):
                        result["attached_policies"].append(policy.to_dict())
                    else:
                        result["attached_policies"].append(policy)

            # 获取用户组及其策略
            groups = self.get_user_groups(user_name)
            for group in groups:
                group_name = group.get("group_name") or group.get("GroupName")
                if group_name:
                    list_attached_group_policies_request = volcenginesdkiam.ListAttachedGroupPoliciesRequest(
                        group_name=group_name,
                    )
                    response_gp = api_instance.list_attached_group_policies(list_attached_group_policies_request)

                    if hasattr(response_gp, 'policies') and response_gp.policies:
                        for policy in response_gp.policies:
                            if hasattr(policy, 'to_dict'):
                                result["group_policies"].append(policy.to_dict())
                            else:
                                result["group_policies"].append(policy)

        except ApiException as e:
            print(f"[Error] Failed to get policies for user '{user_name}': {e}")

        return result

    def create_user(
        self,
        user_name: str,
        display_name: Optional[str] = None,
        description: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        创建 IAM 用户

        Args:
            user_name: 用户名（必填）
            display_name: 显示名称（可选）
            description: 描述信息（可选）

        Returns:
            Dict[str, Any]: 创建的用户信息
        """
        configuration = self._create_configuration()
        volcenginesdkcore.Configuration.set_default(configuration)

        api_instance = volcenginesdkiam.IAMApi()
        create_user_request = volcenginesdkiam.CreateUserRequest(
            user_name=user_name,
            display_name=display_name,
            description=description,
        )

        try:
            response = api_instance.create_user(create_user_request)
            if response and hasattr(response, 'to_dict'):
                return response.to_dict()
            return {}
        except ApiException as e:
            print(f"[Error] Failed to create user '{user_name}': {e}")
            return {}

    def delete_user(self, user_name: str) -> bool:
        """
        删除 IAM 用户

        Args:
            user_name: 用户名

        Returns:
            bool: 删除成功返回 True，失败返回 False
        """
        configuration = self._create_configuration()
        volcenginesdkcore.Configuration.set_default(configuration)

        api_instance = volcenginesdkiam.IAMApi()
        delete_user_request = volcenginesdkiam.DeleteUserRequest(
            user_name=user_name,
        )

        try:
            api_instance.delete_user(delete_user_request)
            return True
        except ApiException as e:
            print(f"[Error] Failed to delete user '{user_name}': {e}")
            return False


