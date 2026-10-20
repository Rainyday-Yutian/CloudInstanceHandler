# coding: utf-8
"""
火山云通用签名请求模块

用于尚未适配官方 SDK 的火山云 OpenAPI 接口（如 ResourceCenter 资源中心），
基于 HMAC-SHA256 签名协议直接构造 HTTP 请求。

原代码位于 volcinsdatahandler.py（VCMServiceCommonRequest 等），迁移至此并修复：
- 补充缺失的 datetime 导入（老代码 request() 中引用 datetime.datetime.utcnow()
  但从未 import，调用必然 NameError）
- 构造参数由 ak/sk 统一为 access_key/secret_key，与新范式对齐

签名协议文档：https://www.volcengine.com/docs/6369/67269
"""
import datetime
import hashlib
import hmac
import json
from typing import Any, Dict, List, Optional
from urllib.parse import quote
from pprint import pprint

import requests


class VCMServiceCommonRequest:
    """
    火山云通用签名请求基类

    用于未适配 SDK 的接口，子类需实现具体的参数组装与 do_request 方法。
    注意：本类为同步实现（基于 requests），异步消费方请使用
    ``await asyncio.to_thread(req.do_request)`` 包装。
    """

    def __init__(
        self,
        access_key: str,
        secret_key: str,
        region: str,
        host: str,
        method: str,
        service: str,
        action: str,
        version: str,
        content_type: str = "application/x-www-form-urlencoded",
        debug: bool = False
    ) -> None:
        self.access_key = access_key
        self.secret_key = secret_key
        self.region = region
        self.host = host
        self.method = method
        self.content_type = content_type
        self.service = service
        self.action = action
        self.version = version
        self.query: Dict[str, Any] = {}
        self.headers: Dict[str, Any] = {}
        self.body: Optional[Dict[str, Any]] = {}
        self.debug = debug

    def norm_query(self, params: Dict[str, Any]) -> str:
        """规范化 query 参数（按 key 排序 + URL 编码）"""
        query = ""
        for key in sorted(params.keys()):
            if isinstance(params[key], list):
                for k in params[key]:
                    query = (
                        query + quote(key, safe="-_.~") + "=" + quote(k, safe="-_.~") + "&"
                    )
            else:
                query = (query + quote(key, safe="-_.~") + "=" + quote(params[key], safe="-_.~") + "&")
        query = query[:-1]
        return query.replace("+", "%20")

    def hmac_sha256(self, key: bytes, content: str) -> bytes:
        """HMAC-SHA256 签名"""
        return hmac.new(key, content.encode("utf-8"), hashlib.sha256).digest()

    def hash_sha256(self, content: str) -> str:
        """SHA256 哈希"""
        return hashlib.sha256(content.encode("utf-8")).hexdigest()

    def request(self) -> Dict[str, Any]:
        """
        计算签名并发送 HTTP 请求

        Returns:
            Dict[str, Any]: 响应 JSON
        """
        credential = {
            "access_key_id": self.access_key,
            "secret_access_key": self.secret_key,
            "service": self.service,
            "region": self.region,
        }
        request_param = {
            "body": self.body,
            "host": self.host,
            "path": "/",
            "method": self.method,
            "content_type": self.content_type,
            "date": datetime.datetime.utcnow(),
            "query": {"Action": self.action, "Version": self.version, **self.query},
        }
        if request_param["body"] is None:
            request_param["body"] = ""

        x_date = request_param["date"].strftime("%Y%m%dT%H%M%SZ")
        short_x_date = x_date[:8]
        x_content_sha256 = self.hash_sha256(request_param["body"])
        sign_result = {
            "Host": request_param["host"],
            "X-Content-Sha256": x_content_sha256,
            "X-Date": x_date,
            "Content-Type": request_param["content_type"],
        }

        signed_headers_str = ";".join(
            ["content-type", "host", "x-content-sha256", "x-date"]
        )
        canonical_request_str = "\n".join(
            [
                request_param["method"].upper(),
                request_param["path"],
                self.norm_query(request_param["query"]),
                "\n".join(
                    [
                        "content-type:" + request_param["content_type"],
                        "host:" + request_param["host"],
                        "x-content-sha256:" + x_content_sha256,
                        "x-date:" + x_date,
                    ]
                ),
                "",
                signed_headers_str,
                x_content_sha256,
            ]
        )

        hashed_canonical_request = self.hash_sha256(canonical_request_str)

        credential_scope = "/".join(
            [short_x_date, credential["region"], credential["service"], "request"]
        )
        string_to_sign = "\n".join(
            ["HMAC-SHA256", x_date, credential_scope, hashed_canonical_request]
        )

        if self.debug:
            print("正规化的请求:", canonical_request_str)
            print("hash值:", hashed_canonical_request)
            print("最终计算的签名字符串:", string_to_sign)

        k_date = self.hmac_sha256(credential["secret_access_key"].encode("utf-8"), short_x_date)
        k_region = self.hmac_sha256(k_date, credential["region"])
        k_service = self.hmac_sha256(k_region, credential["service"])
        k_signing = self.hmac_sha256(k_service, "request")
        signature = self.hmac_sha256(k_signing, string_to_sign).hex()

        sign_result["Authorization"] = "HMAC-SHA256 Credential={}, SignedHeaders={}, Signature={}".format(
            credential["access_key_id"] + "/" + credential_scope,
            signed_headers_str,
            signature,
        )
        header = {**self.headers, **sign_result}
        r = requests.request(
            method=self.method,
            url="https://{}{}".format(request_param["host"], request_param["path"]),
            headers=header,
            params=request_param["query"],
            data=request_param["body"],
        )
        return r.json()


class VCMSearchResourcesRequest(VCMServiceCommonRequest):
    """
    ResourceCenter SearchResources 接口请求类，用于跨地域搜索云资源

    API文档：https://www.volcengine.com/docs/6565/97262
    """

    def __init__(
        self,
        access_key: str,
        secret_key: str,
        filter: Optional[List[Dict[str, Any]]] = None,
        sort_by: Optional[str] = None,
        sort_order: Optional[str] = None,
        max_results: int = 100,
        next_token: Optional[str] = None,
        action: str = "SearchResources",
        version: str = "2023-06-01"
    ) -> None:
        super().__init__(
            access_key, secret_key,
            region="cn-north-1",
            host="open.volcengineapi.com",
            method="POST",
            service="resourcecenter",
            action=action,
            version=version,
            content_type="application/json"
        )
        self.max_results = max_results
        self.filter = filter
        self.sort_by = sort_by
        self.sort_order = sort_order
        self.next_token = next_token
        self.response: Optional[Dict[str, Any]] = None

    def do_request(self) -> Optional[Dict[str, Any]]:
        """组装请求体并发送请求，返回响应中的 Result 字段"""
        params_dict = {
            'MaxResults': self.max_results,
            'Filter': self.filter,
            'SortBy': self.sort_by,
            'SortOrder': self.sort_order,
            'NextToken': self.next_token
        }
        params_dict = {k: v for k, v in params_dict.items() if v is not None}
        if self.debug:
            print("请求参数：")
            pprint(params_dict)
        self.body = json.dumps(params_dict)
        self.response = self.request()
        if self.debug:
            print("完整响应：")
            pprint(self.response)
        return self.response.get("Result", None)


class VCMGetResourceCountsRequest(VCMServiceCommonRequest):
    """
    ResourceCenter GetResourceCounts 接口请求类，用于统计现存资源类型/地域分布

    API文档：https://www.volcengine.com/docs/6565/97262
    """

    def __init__(
        self,
        access_key: str,
        secret_key: str,
        filter: Optional[List[Dict[str, Any]]] = None,
        group_by_key: Optional[str] = None,
        action: str = "GetResourceCounts",
        version: str = "2023-06-01"
    ) -> None:
        super().__init__(
            access_key, secret_key,
            region="cn-north-1",
            host="open.volcengineapi.com",
            method="POST",
            service="resourcecenter",
            action=action,
            version=version,
            content_type="application/json"
        )
        self.filter = filter
        self.group_by_key = group_by_key
        self.response: Optional[Dict[str, Any]] = None

    def do_request(self) -> Optional[Dict[str, Any]]:
        """组装请求体并发送请求，返回响应中的 Result 字段"""
        if self.group_by_key not in ["ResourceType", "Region"]:
            raise ValueError("group_by_key must be one of ['ResourceType','Region']")
        params_dict = {
            'Filter': self.filter,
            'GroupByKey': self.group_by_key
        }
        params_dict = {k: v for k, v in params_dict.items() if v is not None}
        if self.debug:
            print("请求参数：")
            pprint(params_dict)
        self.body = json.dumps(params_dict)
        self.response = self.request()
        if self.debug:
            print("完整响应：")
            pprint(self.response)
        return self.response.get("Result", None)
