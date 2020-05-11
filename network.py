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
path=os.path.join(data_path.get_data_path("2020_04_20"),"调用链指标")
# 获取所有trace
traces= data_cleaning.build_trace(path)
res=count(traces.values())

#%%计算平均
avg_res = {}
for k,v in res.items():
    avg_res[k] = sum(v)/len(v)
#print(res)
json_str = json.dumps(avg_res,indent=4)
# print(json_str)
with open("count.json",'w')as f:
    f.write(json_str)
        


# %%
