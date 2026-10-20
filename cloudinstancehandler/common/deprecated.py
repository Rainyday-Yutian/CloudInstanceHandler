# coding: utf-8
"""
厂商基类 camelCase → snake_case 重命名兼容别名工具。

本模块提供 ``deprecated_alias`` 装饰器工厂，用于将老的 camelCase 方法名
（如 ``getMetricData``）保留为发出 ``DeprecationWarning`` 的薄转发别名，
新功能请一律使用 snake_case 命名（如 ``get_metric_data``）。

注意：
    Python 默认不显示 DeprecationWarning（除 __main__ 中触发的以外），
    如需查看告警请使用 ``python -W always::DeprecationWarning ...`` 运行，
    或在代码中 ``warnings.simplefilter("always", DeprecationWarning)``。
    别名在告警不可见时功能完全不受影响。

用法示例::

    from cloudinstancehandler.common.deprecated import deprecated_alias

    class SomeInstance(Base):
        def get_metric_data(self, ...):
            ...

        @deprecated_alias("get_metric_data")
        def getMetricData(self, *args, **kwargs):  # noqa: N802
            '''[已废弃别名] 请改用 get_metric_data'''
            return self.get_metric_data(*args, **kwargs)
"""
import functools
import inspect
import warnings
from typing import Callable, Union


def deprecated_alias(new_name: Union[str, Callable], removal: str = "a future release") -> Callable:
    """
    装饰器工厂：将旧的 camelCase 方法包装为发出 DeprecationWarning 的兼容别名。

    被装饰的函数应为薄转发桩（``*args/**kwargs`` 透传到新方法），
    本装饰器负责：
        1. 在每次调用时发出 DeprecationWarning（stacklevel=2，指向调用方代码行）；
        2. 自动识别同步/异步桩函数，异步桩会被包装为协程函数并正确 await；
        3. 通过 functools.wraps 保留桩函数的元数据（签名、docstring 等）。

    Args:
        new_name: 新方法的名称（字符串）或方法对象本身（用于 introspection 展示）。
        removal: 提示信息中说明别名将被移除的版本，默认 "a future release"。

    Returns:
        Callable: 装饰器。
    """
    def decorator(stub: Callable) -> Callable:
        target = new_name if isinstance(new_name, str) else new_name.__name__
        message = (
            f"{stub.__qualname__} 已重命名为 {target}，"
            f"该兼容别名将在 {removal} 中移除，请改用 {target}。"
        )

        if inspect.iscoroutinefunction(stub):
            @functools.wraps(stub)
            async def async_wrapper(*args, **kwargs):
                warnings.warn(message, DeprecationWarning, stacklevel=2)
                return await stub(*args, **kwargs)
            wrapper = async_wrapper
        else:
            @functools.wraps(stub)
            def sync_wrapper(*args, **kwargs):
                warnings.warn(message, DeprecationWarning, stacklevel=2)
                return stub(*args, **kwargs)
            wrapper = sync_wrapper

        # 若传入了真实方法对象，借用其文档用于 introspection 展示
        if not isinstance(new_name, str):
            functools.update_wrapper(wrapper, new_name, updated=("__doc__",))
        wrapper.__wrapped_alias_target__ = target
        return wrapper
    return decorator
