from read_data import readCsvWithPandas,read_json
import data_cleaning
from tqdm import tqdm
import numpy as np
import json
def count(traces):
    call_time=dict()
    for trace_id,trace in tqdm(traces.items(), desc='计算中', ncols=100, ascii=' =', bar_format='{l_bar}{bar}|'):
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
if __name__ == "__main__":
    path='D:\\aiops\\2020_04_11\\调用链指标\\'
    traces=read_json(path+"trace.json")
    res=count(traces)
    #print(res)
    json_str = json.dumps(res,indent=4)
    print(json_str)
    with open(path+"count.json",'w')as f:
        f.write(json_str)
        
