import numpy as np
import os
import sys
import re
import matplotlib.pyplot as plt
import data_path # 路径
import data_cleaning
from read_data import readCSV,read_xlrd
from xlrd import
from datetime import datetime
from xlrd import xldate_as_tuple
def main():
   
    business_path = os.path.join(data_path.get_data_path(), "业务指标", "esb.csv")
    # 调用链指标,平台指标,数据说明
    trace_p,plat_p,data_instruction_p = data_cleaning.getPath()
    # 获取业务指标数据，去掉表头
    data = np.array(readCSV(business_path)[1:])
    #todo step1 异常时间序列
    execption_times = find_time_interval(data)
    #todo step2 异常时间区间
    # period_times = to_period_time(execption_times)
    period_times = fault_time()
    #todo step3 获取所有trace
    traces = data_cleaning.build_trace()
    #todo step4 找出这段时间内的trace
    for execption_Interval in period_times:
        abnormal_traces = find_abnormal_trace(execption_Interval,traces)
        #todo step5 找出异常数据中的异常节点
        for traceId,trace in abnormal_traces.items():
            abnormal_spans = find_abnormal_span(trace)
            cmdb_ids = list(set([span['cmdb_id'] for span in abnormal_spans]))
            #todo step6 判断该节点是哪个指标有异常
            for cmdb_id in cmdb_ids:
                #? 找到异常指标
                abnormal_indicators = find_abnormal_indicators(execption_Interval,cmdb_id)

def find_abnormal_indicators(execption_Interval,cmdb_id):
    """[该时间区间内那个指标错误]

    Args:
        execption_Interval ([type]): [时间区间]
        cmdb_id ([type]): [网源]
    """
    pass

def find_abnormal_span(trace):
    """按照图的遍历方式遍历trace中的所有span

    Args:
        trace ([dict]): 一条trace，格式{ startTime:str,{spanId:{},spanId:{}}}        \n

    Returns:
        [list]: 返回异常节点       \n
    """
    graph = data_cleaning.generateGraph()
    pass

def find_abnormal_trace(execption_Interval,traces):
    """找到改异常区间内所有trace

    Args:
        execption_Interval ([type]): [时间区间]
        traces ([type]): [description]
    """
    pass

def fault_time(bias=0):
    table = read_xlrd(os.path.join(data_path.get_data_path(),"数据说明","0故障内容.xlsx"))     
    table_head = table.row_values(0)
    time_index,duration_index = 0,0
    for i in range(table.ncols):
        if table_head[i]=='time':
            time_index = i
        elif table_head[i]=='duration':
            duration_index = i
    res = []
    for i in range(1,table.nrows):
        row = table.row_values(i)
        cell = table.cell_value(i, time_index)
        date = datetime(*xldate_as_tuple(cell, 0))
        # print(date)
        time_stamp = int(datetime.timestamp(date))*1000
        duration = int(re.match('\d*',row[duration_index])[0])*60*1000
        res.append((time_stamp,time_stamp+duration))
    return res
def find_time_interval(data):
    """
    在入口服务中，找到异常的数据的时间戳
    """
    avg_times = data[:, 2].astype(np.float32)
    succee_rate = data[:, 5].astype(np.float32)
    std = np.std(avg_times, ddof=1)
    means = np.mean(avg_times)
    high = means+3*std
    # low = means-3*std
    lam = lambda i: avg_times[i]>high or succee_rate[i]<1
    res = data[list(filter(lam,  range(len(data)) ))]
    return res[:,1].astype(np.int64)

def to_period_time(timestamps,bias=5*1000):
    '''
    将时间戳的序列转换为时间段
    '''
    r = list(map(lambda t:(t-bias,t+bias),timestamps))
    return r



def draw(data):
    x = range(len(data))
    plt.figure(figsize=(8,5))
    # 获取平均处理时间一列
    y = data[:, 2].astype(np.float32)
    plt.subplot(2, 1,1)
    # plt.title("平均调用时间")
    plt.plot(x,y,label="平均调用时间")

    # 获取成功率一列
    y = data[:, 5].astype(np.float32)
    plt.subplot(2, 1,2)
    # plt.title("成功率")
    plt.plot(x,y,color='r',label="成功率")

    plt.show()

if __name__ == '__main__':
    main()
