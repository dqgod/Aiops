# %%
import pandas as pd
import numpy as np
import os
from data_path import get_data_path
from sklearn.ensemble import IsolationForest
import show_Kpis 
import anomaly_detection
# %%读取所有Kpis
day = '2020_04_22'
path = os.path.join(get_data_path(day), "平台指标")
kpis = show_Kpis.getKpis(["dcos_docker.csv"], path)
print("Get ALL KPIS")


# %% step1 获取一条具体的数据
def get_specific_kpi(kpis, filter_list):
    """[筛选出满足条件的具体指标]
    Args:
        kpis ([dict]): [指标字典]:
        filter_list ([list]): [description]
    Returns:[list]: [[(key,np.array),(),()]]
    """
    res = []
    for k, v in kpis.items():
        if not list(filter(lambda x: x not in k, filter_list)):
            res.append((k, np.array(v)))
    return res

filter_list = ["docker_003", "cpu"]
data = get_specific_kpi(kpis, filter_list)[-1][1]
show_Kpis.showKpiCurve(kpis, filter_list)
#%% 排序并转换为dataframe
data = pd.DataFrame(data)
data.columns=['itemid', 'name', 'bomc_id', 'timestamp', 'value', 'cmdb_id']
data.sort_values("timestamp",inplace=True)
print(data.head(5))
print(type(data['timestamp'].values))
# %% step2 使用iforest 孤立森林

ilf = IsolationForest(n_estimators=100,
                      n_jobs=-1,          # 使用全部cpu
                      verbose=2,
                      )
# data = pd.read_csv('data.csv', index_col="id")
# data = data.fillna(0)
# 选取特征，不使用标签(类型)
X_cols = ["value"]
print (data.shape)

# 训练
ilf.fit(data[X_cols])
shape = data.shape[0]
batch = 10**6

all_pred = []
for i in range(shape//batch+1):
    start = i * batch
    end = (i+1) * batch
    test = data[X_cols][start:end]
    # 预测
    pred = ilf.predict(test)
    all_pred.extend(pred)

data['pred'] = all_pred
data.to_csv('outliers.csv', columns=["timestamp","pred", ], header=False)

# %%
business_path = os.path.join(get_data_path(day), "业务指标", "esb.csv")
# 获取业务指标数据，去掉表头,np.array
data = pd.read_csv(business_path).values
# 根据时间序列排序
data = data[np.argsort(data[:, 1])]
# todo step1 异常时间序列
# 异常数据
abnormal_data = anomaly_detection.find_abnormal_data(data)
# 异常时间序列
execption_times = abnormal_data[:, 1].astype(np.int64)
#! 异常时间区间
interval_times = anomaly_detection.to_interval(execption_times)
#! 对应时间区间是否是网络故障
is_net_error = anomaly_detection.is_net_error_func(interval_times,abnormal_data)
print(len(interval_times))
# period_times = anomaly_detection.fault_time()
for i,j in zip(interval_times,is_net_error):
    print(i,j)
# 画出找到的异常区间
anomaly_detection.draw_abnormal_period(data, interval_times)

# %%
