#%%
from read_data import readCsvWithPandas,read_json
import data_cleaning
from tqdm import tqdm
import numpy as np
import json
import os
import data_path
def count(traces):
    """[计算一条trace 的调用延迟时间]

    Args:
        traces (List[dict]): list[{startTime:t,spans:{}},{},{}]

    Returns:
        [type]: [dict]
    """
    call_time=dict()
    for trace in tqdm(traces, desc='计算中', ncols=100, ascii=' =', bar_format='{l_bar}{bar}|'):
        for span_id,span in trace["spans"].items():
            #temp=span_id['target'].spilt()[0]
            #if temp=='db' or span['db']!=null
            parent_span_id=span['parentId']
            if parent_span_id=='root':
                continue
            parent_span=trace['spans'][parent_span_id]
            key=parent_span['cmdb_id']+'->'+span['cmdb_id']
            value=float(parent_span["duration"])-float(span['duration'])
            if key in call_time.keys():
                #new_value=np.append(call_time[key],value)
                call_time[key].append(value)
                #call_time[key]=new_value
            else:
                call_time[key]=[value]
                
    return call_time

# if __name__ == "__main__":
#%% 计算所有的
# path=os.path.join(data_path.get_data_path("2020_04_20"),"调用链指标")
# # 获取所有trace
# traces= data_cleaning.build_trace(path)
# res=count(traces.values())
#
# #%%计算平均
# avg_res = {}
# for k,v in res.items():
#     avg_res[k] = sum(v)/len(v)
# #print(res)
# json_str = json.dumps(avg_res,indent=4)
# # print(json_str)
# save_path = data_path.get_save_path()
# if not os.path.exists(save_path):
#     os.mkdir(save_path)
# with open(os.path.join(save_path,"count.json"),'w')as f:
#     f.write(json_str)
        
def Travel_time(period_time,traces):
    #获取period_time时间内的trace
    # traces2=[]
    # for trace_id,trace in traces.items():
    #     if int(trace['startTime'])>=period_time[0] and int(trace['startTime'])<=period_time[1]:
    #         traces2.append(trace)
    res=count(traces)
    #计算该事件区间内的调用时间均值
    avg_res = {}
    for k,v in res.items():
        avg_res[k] = sum(v)/len(v)
    #print(res)
    #与正常情况下的平均调用世家对比，找出异常
    abnormal_calls=[]
    abnormal_amdbs=[]
    with open("count.json",'r')as f:
        normal=json.load(f)
    for key,value in avg_res.items():
        normal_value=normal[key]
        #print(str(normal_value)+"  "+str(value))
        if value>3*normal_value:
            abnormal_calls.append(key)
    print(abnormal_calls)
    for abnormal_call in abnormal_calls:
        abnormal_amdb=abnormal_call.split("->")[1]
        abnormal_amdbs.append(abnormal_amdb)
    return abnormal_amdbs

# %%
