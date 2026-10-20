# -*- coding: utf-8 -*-
"""
腾讯云访问管理(CAM) - 对应阿里云RAM

功能：管理用户、用户组、角色、策略等访问控制资源
参考：https://cloud.tencent.com/document/product/598
"""

import os
import json
import asyncio
from typing import List, Dict, Any, Optional
from cloudinstancehandler.tencentcloud.TencentCloudInstance import TencentCloudInstance

from tencentcloud.common import credential
from tencentcloud.common.profile.client_profile import ClientProfile
from tencentcloud.common.profile.http_profile import HttpProfile
from tencentcloud.common.exception.tencent_cloud_sdk_exception import TencentCloudSDKException
from tencentcloud.cam.v20190116 import cam_client_async as cam_client, models


class CAM(TencentCloudInstance):
    def __init__(self, access_key: str, secret_key: str):
        super().__init__(access_key=access_key, secret_key=secret_key)

    def _create_client(self) -> cam_client.CamClient:
        cred = credential.Credential(self.access_key, self.secret_key)
        httpProfile = HttpProfile()
        httpProfile.endpoint = "cam.tencentcloudapi.com"

        clientProfile = ClientProfile()
        clientProfile.httpProfile = httpProfile
        # 异步客户端不需要 region_id，传空字符串
        client = cam_client.CamClient(cred, "", clientProfile)
        return client

    async def fetch_instance_info(
        self,
        user_names: List[str] = None,
        page_size: int = 50,
        auto_accumulate: bool = True
    ) -> List[Dict[str, Any]]:
        """
        获取CAM用户信息

        支持两种查询模式：
        1. 批量用户名查询 (user_names)
        2. 全量查询 (默认)

        Args:
            user_names: 用户名列表，用于批量查询（单个用户也用此参数）
            page_size: 单页数量 (1-50)
            auto_accumulate: 是否自动累积到instance_info

        Returns:
            List[Dict[str, Any]]: 用户信息列表
        """
        raw_records = []

        try:
            if user_names and len(user_names) > 0:
                raw_records = await self.fetch_users_batch_async(user_names, limit=10)
            else:
                raw_records = await self.fetch_all_users_async(page_size)

            if auto_accumulate and raw_records:
                self._accumulate_instance_info(raw_records)

        except Exception as e:
            print(f"[Error] Failed to fetch CAM users: {e}")
            raise

        return raw_records

    async def fetch_users_batch_async(
        self,
        user_names: List[str],
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """
        批量查询用户信息（并发）

        Args:
            user_names: 用户名列表
            limit: 并发数量限制

        Returns:
            List[Dict[str, Any]]: 所有找到的用户信息列表（过滤掉不存在的用户）
        """
        if len(user_names) > 50:
            print(f"[Warning] user_names length ({len(user_names)}) exceeds recommended limit of 50")

        if not user_names:
            return []

        semaphore = asyncio.Semaphore(limit)
        results = []

        async def _fetch_one_with_semaphore(name: str):
            async with semaphore:
                return await self.fetch_user_async(name)

        tasks = [_fetch_one_with_semaphore(name) for name in user_names]
        batch_results = await asyncio.gather(*tasks, return_exceptions=False)

        # 过滤掉None（不存在的用户）
        results = [info for info in batch_results if info is not None]
        return results
    
    async def fetch_user_async(
        self,
        user_name: str
    ) -> Optional[Dict[str, Any]]:
        """
        获取单个用户信息

        Args:
            user_name: 用户名

        Returns:
            Optional[Dict[str, Any]]: 用户信息字典，不存在返回None

        Raises:
            TencentCloudSDKException: API调用失败时抛出异常
        """
        client = self._create_client()
        req = models.GetUserRequest()
        req.Name = user_name

        try:
            resp = await client.GetUser(req)
            return json.loads(resp.to_json_string())
        except TencentCloudSDKException as e:
            if "UserNotExist" in str(e):
                return None
            raise

    async def fetch_all_users_async(
        self,
        page_size: int = 50
    ) -> List[Dict[str, Any]]:
        """
        获取所有用户信息（分页）

        Args:
            page_size: 单页数量 (1-50)

        Returns:
            List[Dict[str, Any]]: 所有用户信息列表
        """
        if not 1 <= page_size <= 50:
            print(f"[Warning] page_size must be between 1 and 50, using default 50")
            page_size = 50

        client = self._create_client()
        records: List[Dict[str, Any]] = []
        total_count: Optional[int] = None

        req = models.ListUsersRequest()
        req.PageSize = page_size
        req.Page = 1

        while True:
            try:
                resp = await client.ListUsers(req)
                users = resp.Data or []

                if not users:
                    break

                for user in users:
                    records.append(json.loads(user.to_json_string()))

                if total_count is None:
                    total_count = resp.TotalNum or 0

                if len(records) >= total_count:
                    break

                req.Page += 1

            except TencentCloudSDKException as e:
                print(f"[Error] Failed to fetch page {req.Page}: {e}")
                break

        return records

    async def get_user_groups_async(
        self,
        user_name: str
    ) -> List[Dict[str, Any]]:
        """
        获取用户所属的用户组

        Args:
            user_name: 用户名

        Returns:
            List[Dict[str, Any]]: 用户组列表
        """
        client = self._create_client()
        req = models.ListGroupsForUserRequest()
        req.UserName = user_name

        try:
            resp = await client.ListGroupsForUser(req)
            groups = resp.Groups or []
            return [json.loads(group.to_json_string()) for group in groups]
        except TencentCloudSDKException as e:
            print(f"[Error] Failed to get groups for user '{user_name}': {e}")
            raise

    async def get_user_policies_async(
        self,
        user_name: str
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        获取用户关联的策略（包括直接附加和通过组附加）

        Args:
            user_name: 用户名

        Returns:
            Dict[str, List[Dict[str, Any]]]: 策略信息，包含：
                - attached_policies: 直接附加的策略
                - group_policies: 通过组附加的策略
        """
        client = self._create_client()
        result = {
            "attached_policies": [],
            "group_policies": []
        }

        try:
            # 获取直接附加的策略
            req = models.ListAttachedUserPoliciesRequest()
            req.TargetName = user_name
            resp = await client.ListAttachedUserPolicies(req)
            result["attached_policies"] = [json.loads(p.to_json_string()) for p in (resp.List or [])]

            # 获取用户组及其策略
            groups = await self.get_user_groups_async(user_name)
            for group in groups:
                group_name = group.get("GroupName")
                if group_name:
                    req_gp = models.ListAttachedGroupPoliciesRequest()
                    req_gp.TargetName = group_name
                    resp_gp = await client.ListAttachedGroupPolicies(req_gp)
                    policies = [json.loads(p.to_json_string()) for p in (resp_gp.List or [])]
                    result["group_policies"].extend(policies)

        except TencentCloudSDKException as e:
            print(f"[Error] Failed to get policies for user '{user_name}': {e}")
            raise

        return result

