import datetime,warnings
from pprint import pprint

# 通用时间范围生成器 #
def getTimeDict(start_offset_days=0,end_offset_days=0,start_offset_hours=0,end_offset_hours=0,start_offset_minutes=0,end_offset_minutes=0,start_datetime=None,end_datetime=None,print_time_range=True):
    """
    生成时间范围字典，适用于云监控等需要时间区间的场景。

    计算逻辑：
    - 结束时间：若未指定，则以当前时间为基础，减去 end_offset_* 得到。
    - 起始时间：若未指定，则以结束时间为基础，减去 start_offset_* 得到。
    - 若指定了 start_datetime 或 end_datetime，则直接使用（并清零秒和微秒）。
    - start_offset_* 参数仅在 start_datetime 为 None 时生效。

    Args:
        start_offset_days (int): 起始时间自动计算时向前偏移的天数
        end_offset_days (int): 结束时间自动计算时向前偏移的天数
        start_offset_hours (int): 起始时间偏移小时数
        end_offset_hours (int): 结束时间偏移小时数
        start_offset_minutes (int): 起始时间偏移分钟数
        end_offset_minutes (int): 结束时间偏移分钟数
        start_datetime (datetime, optional): 手动指定起始时间
        end_datetime (datetime, optional): 手动指定结束时间
        print_time_range (bool): 是否打印时间范围

    Returns:
        dict: 包含以下字段：
            - start_datetime, end_datetime: datetime 对象（秒和微秒清零）
            - start_timestamp, end_timestamp: 秒级时间戳
            - start_timestring, end_timestring: 'YYYY-MM-DD HH:MM:SS'
            - start_timestring_iso8601, end_timestring_iso8601: ISO8601 格式 + '+08:00'（表示北京时间）
    """
    if end_datetime is None:
        end_datetime = datetime.datetime.now()
        end_datetime = end_datetime - datetime.timedelta(days=end_offset_days,hours=end_offset_hours,minutes=end_offset_minutes)
    if start_datetime is None:
        # 当start_datetime未被指定时，默认使用end_datetime作为基底时间进行偏移计算。这样在调用时方便计算时间范围。
        start_datetime = end_datetime - datetime.timedelta(days=start_offset_days,hours=start_offset_hours,minutes=start_offset_minutes)
    start_datetime = start_datetime.replace(second=0, microsecond=0)
    end_datetime = end_datetime.replace(second=0, microsecond=0)
    if start_datetime > end_datetime:
        raise ValueError("Start time must be earlier than End time!")
    if end_datetime - start_datetime > datetime.timedelta(days=60):
        warnings.warn("Time range exceeds 60 days, may exceed API limits.", UserWarning)
    TimeDict = {
        "start_datetime":start_datetime,
        "end_datetime":end_datetime,
        "start_timestamp":int(start_datetime.timestamp()),
        "end_timestamp":int(end_datetime.timestamp()),
        "start_timestring":start_datetime.strftime('%Y-%m-%d %H:%M:%S'),
        "end_timestring":end_datetime.strftime('%Y-%m-%d %H:%M:%S'),
        "start_timestring_iso8601":start_datetime.strftime("%Y-%m-%dT%H:%M:%S")+"+08:00",
        "end_timestring_iso8601":end_datetime.strftime("%Y-%m-%dT%H:%M:%S")+"+08:00"
        }
    if print_time_range:
        print(f"{TimeDict['start_timestring']} -> {TimeDict['end_timestring']}")
    return TimeDict
