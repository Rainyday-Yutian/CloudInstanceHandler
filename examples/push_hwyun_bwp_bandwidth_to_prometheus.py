"""
华为云 BGP 带宽包（BWP）带宽监控指标推送至 Prometheus Pushgateway

功能：
- 拉取名称包含 'vpn' 的 BWP 实例
- 查询最近 5~3 分钟的 upstream/downstream_bandwidth
- 按实例打上标签（instanceId, name, region, isp, direction, csp）
- 推送至指定 Pushgateway

依赖环境变量（或直接填入）：
  HWYUN_AK, HWYUN_SK
  PROMETHEUS_PUSHGATEWAY_URL
"""

from prometheus_client import CollectorRegistry, Gauge, push_to_gateway
from cloudinstancehandler.hwyuninsdatahandler import *

if __name__ == "__main__":
   # ak = "xxx"
   # sk = "xxx"
   ak = os.getenv("HWYUN_AK",None)
   sk = os.getenv("HWYUN_SK",None)
   
   VPN_BWP = GBWP(ak,sk)
   VPN_BWP.getInsInfo()
   VPN_BWP.InsInfo = VPN_BWP.InsInfo[VPN_BWP.InsInfo['name'].str.contains("vpn")]
   
   TimeDict = getTimeDict(start_offset_minutes=5, end_offset_minutes=3)
   df_data_upstream = VPN_BWP.getMetricData('cn-north-4',instance_list=VPN_BWP.InsInfo['id'].to_list(),metric_name="upstream_bandwidth",period="60",TimeDict=TimeDict,statistics_approach=['last'])
   df_data_downstream = VPN_BWP.getMetricData('cn-north-4',instance_list=VPN_BWP.InsInfo['id'].to_list(),metric_name="downstream_bandwidth",period="60",TimeDict=TimeDict,statistics_approach=['last'])
   df_data = pd.merge(df_data_upstream, df_data_downstream, on='geip_internet_bandwidth_id', how='outer')
   df_data.rename(columns={'geip_internet_bandwidth_id': 'id'}, inplace=True)
   print(df_data)
   df = pd.merge(VPN_BWP.InsInfo, df_data, on='id', how='left')

   registry = CollectorRegistry()
   gauge_bandwidth = Gauge("vpn_bwp_bandwidth", "BWP Bandwidth", ["instanceId","name","region","isp","direction","csp"], registry=registry)
   for index, row in df.iterrows():
      gauge_bandwidth.labels(instanceId=row["id"], name=row["name"], region=row["access_site"], isp=row["isp"], direction="out",csp="huawei").set(row["upstream_bandwidth_last"])
      gauge_bandwidth.labels(instanceId=row["id"], name=row["name"], region=row["access_site"], isp=row["isp"], direction="in",csp="huawei").set(row["downstream_bandwidth_last"])
   push_to_gateway('http://your.domain.com:9091', job='vpn_hwyun_bwp', registry=registry)


