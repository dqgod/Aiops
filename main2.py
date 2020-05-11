# %%
import numpy as np
import os
import sys
import re
import json
import matplotlib.pyplot as plt
import data_path  # 路径
import data_cleaning
from read_data import readCSV, read_xlrd, readCsvWithPandas
from datetime import datetime
from xlrd import xldate_as_tuple
from show_Kpis import getKpis
kpi_opened = {}
# 是否是执行者调用
isExecutor = {"JDBC": False, "LOCAL": False, "CSF": False,
              "FlyRemote": True, "OSB": True, "RemoteProcess": True}
# %%


def find_abnormal_indicators(execption_Interval, cmdb_id):
    """[该时间区间内那个指标错误]

    Args:
        execption_Interval ([type]): [时间区间]
        cmdb_id ([type]): [网源]
    """
    kpis = None
    abnormal_indicators = []
    # os,docker,db
    file_name = data_path.fileNames[cmdb_id.split('_')[0]]
    # file_path = os.path.join(data_path.get_data_path(),"平台指标",file_name)
    # 查看当前文件是否已经分解
    if kpi_opened.get(file_name) == None:
        kpis = getKpis([file_name])
        kpi_opened[file_name] = kpis
    else:
        kpis = kpi_opened[file_name]
    # 逐个指标的进行判断
    for k, v in kpis.items():
        temp = k.split(',')  # (cmdb_id,name,bomc_id,itemid)
        if cmdb_id == temp[0]:
            # todo 进行异常评估，给出得分
            score = anomaly_detection(execption_Interval, v)
            abnormal_indicators.append((temp[0], temp[1], temp[2], score))
    # 排序返回得分最高的三个
    return abnormal_indicators


def anomaly_detection(execption_Interval, data):
    """[异常检测算法]

    Args:
        execption_Interval ([tutle]): [时间区间(start-time,end-time)]
        data ([type]): [itemid,name,bomc_id,timestamp,valuee,cmdb_id]
    """
    

    pass


def find_abnormal_span(trace):
    """按照图的遍历方式遍历trace中的所有span\n
    Args:
        trace ([dict]): 一条trace，格式{ startTime:str,{spanId:{},spanId:{}}}        \n
    Returns:
        [list]: 返回异常节点       \n
    """
    spans = trace['spans']  
    graph = data_cleaning.generateGraph(spans)
    if graph.get('root') == None:
        return []

    abnormal_cmdb_ids = []
    Break = True
    # isError代表上溯的节点是否有异常
    def traverse(root_id, abn_ids, isError=False):
        root = spans[root_id]
        # 如果上溯有异常或本身有异常
        if isError or root['success'] == 'False':
            # 当发现是数据库出现问题时，将其他的清空，只保存数据库cmdb_id,并退出递归
            if root['db'] and root['success'] == 'False':
                abn_ids.clear()
                abn_ids.append(root["db"])
                return Break
            # 找出上一个失败的下一个成功
            if isExecutor[root['callType']] and root['callType'] != 'OSB' \
                and root['success'] == 'True':
                abn_ids.clear()
                abn_ids.append(root["cmdb_id"])
        isError = root['success'] == 'False'
        # 如果没有子节点，直接返回
        if graph.get(root_id) == None:
            return not Break
        for span_id in graph[root_id]:
            if traverse(span_id, abn_ids, isError) == Break:
                return Break
        return not Break

    for span_id in graph.get('root'):
        abn_ids = []
        traverse(span_id, abn_ids)
        abnormal_cmdb_ids += abn_ids
    return abnormal_cmdb_ids


def find_abnormal_trace(execption_Interval, traces):
    """找到改异常区间内所有trace

    Args:
        execption_Interval ([type]): [时间区间]
        traces ([type]): [description]
    """
    abnormal_trace = []
    for trace in traces.values():
        startTime = int(trace['startTime'])
        if startTime > execption_Interval[0] and startTime < execption_Interval[1]:
            abnormal_trace.append(trace)
    return abnormal_trace


def fault_time(bias=0):
    """[summary]
    直接读文件读出 故障时间
    """
    table = read_xlrd(os.path.join(
        data_path.get_data_path(), "数据说明", "0故障内容.xlsx"))
    table_head = table.row_values(0)
    time_index, duration_index = 0, 0
    for i in range(table.ncols):
        if table_head[i] == 'time':
            time_index = i
        elif table_head[i] == 'duration':
            duration_index = i
    res = []
    for i in range(1, table.nrows):
        row = table.row_values(i)
        cell = table.cell_value(i, time_index)
        date = datetime(*xldate_as_tuple(cell, 0))
        # print(date)
        time_stamp = int(datetime.timestamp(date))*1000
        duration = int(re.match('\d*', row[duration_index])[0])*60*1000
        res.append([time_stamp, time_stamp+duration])
    return res


def find_time_interval(data):
    """
    在入口服务中，找到异常的数据的时间戳
    """
    avg_times = data[:, 2].astype(np.float32)
    # 大于1的数据舍弃，不去计算均值和方差
    avg_times_temp = list(filter(lambda x:x<1,avg_times))
    succee_rate = data[:, 5].astype(np.float32)
    std = np.std(avg_times_temp, ddof=1)
    means = np.mean(avg_times_temp)

    high = means+3*std
    low = means-3*std
    # print(high,low)
    def lam(i): return avg_times[i] > high or avg_times[i]<low or succee_rate[i] < 1
    res = data[list(filter(lam,  range(len(data))))]
    return res[:, 1].astype(np.int64)


def to_period_time(timestamps, bias=1.5*60*1000):
    '''
    将时间戳的序列转换为时间段,区间合并
    '''
    def merge(intervals):
        intervals.sort(key=lambda x: x[0])

        merged = []
        for interval in intervals:
            # 如果列表为空，或者当前区间与上一区间不重合，直接添加
            if not merged or merged[-1][1] < interval[0]:
                merged.append(interval)
            else:
                # 否则的话，我们就可以与上一区间进行合并
                merged[-1][1] = max(merged[-1][1], interval[1])
        return merged
    r = list(map(lambda t: [t-bias, t+bias], timestamps))
    return merge(r)


def draw(data,period_times=None):
    x = range(len(data))

    index = [[] for _ in range(len(period_times))]
    for i,d in enumerate(data[:, 1].astype(np.int64)):
        for j,t in enumerate(period_times):
            if d<t[1] and d>t[0]:
                index[j].append(i)

    plt.figure(figsize=(8, 5))
    # 获取平均处理时间一列
    y = data[:, 2].astype(np.float32)
    plt.subplot(2, 1, 1)
    # plt.title("平均调用时间")
    plt.plot(x, y, label="平均调用时间")
    for i in index:
        plt.plot(i,np.ones(len(i))*40,color='y')

    # 获取成功率一列
    y = data[:, 5].astype(np.float32)
    plt.subplot(2, 1, 2)
    # plt.title("成功率")
    plt.plot(x, y, color='r', label="成功率")
    
        
    for i in index:
        plt.plot(i,np.ones(len(i))*0.5,color='y')

    plt.show()

 
# %%
# 结果
result = []
# 业务指标
business_path = os.path.join(data_path.get_data_path(), "业务指标", "esb.csv")
# 调用链指标,平台指标,数据说明
trace_p, plat_p, data_instruction_p = data_cleaning.getPath()
# 获取业务指标数据，去掉表头
data = readCsvWithPandas(business_path)
# 根据时间序列排序
data = data[np.argsort(data[:,1])]
# todo step1 异常时间序列
execption_times = find_time_interval(data)

# todo step2 异常时间区间
period_times = to_period_time(execption_times)
# for i in period_times:
#     print(i)
# 画出找到的异常区间
draw(data,period_times)

# period_times = fault_time()
# %%
# todo step3 获取所有trace
traces = data_cleaning.build_trace(trace_p)

# %%
# todo step4 找出这段时间内的trace
for i, execption_Interval in enumerate(period_times):
    # execption_Interval = (0,15865349796310)
    abnormal_traces = find_abnormal_trace(execption_Interval, traces)
    # print(len(abnormal_traces))
    abnormal_cmdb_ids = []
    # todo step5 找出异常数据中的异常节点
    for trace in abnormal_traces:
        #! 以下未实现
        abnormal_cmdb_ids += find_abnormal_span(trace)
    abnormal_cmdb_ids = list(set(abnormal_cmdb_ids))
    print(i+1, abnormal_cmdb_ids)
    # todo step6 判断该节点是哪个指标有异常
    # for cmdb_id in abnormal_cmdb_ids:
    #     # ? 找到异常指标
    #     abnormal_indicators = find_abnormal_indicators(
    #         execption_Interval, cmdb_id)
    #     #sorted(abnormal_indicators, key=lambda x: x[3], reversed=True)[:3]
    #     result.append(abnormal_indicators)

# print(result)

# %%
