import csv
import os
import sys
import json
from tqdm import tqdm
path="F:\\aiops\\data_all\\2020_04_11\\调用链指标\\"
# datestamps=["datestamp=2020-02-14","datestamp=2020-02-15","datestamp=2020-02-16","datestamp=2020-02-17","datestamp=2020-02-18","datestamp=2020-02-19","datestamp=2020-02-20"]
datestamps=[""]
csvNames=["trace_osb","trace_csf","trace_fly_remote","trace_remote_process","trace_local","trace_jdbc"]


def readCSV(path):
    res = []
    with open(path, 'r') as f:
        reader = csv.reader(f)
        res=list(reader)
    return res
def getKPIs(datestamp,comd_id=None):
    pass
def build_trace():
    res = {}
    print("开始trace数据合并！")
    for day in datestamps:
        for csvName in csvNames:
            p=os.path.join(path,csvName)+".csv"
            print("正在读取文件 "+csvName)
            temp = readCSV(p)
            for i in tqdm(range(len(temp)-1), desc=csvName, ncols=100, ascii=' =', bar_format='{l_bar}{bar}|'):
                # 0 callType,1 startTime,2 elapsedTime,3 success,4 traceId,5 id,6 pid,7 cmdb_id,8 serviceName
                row = temp[i+1]
                length = len(row)
                if length<=3:
                    continue
                span = {}
                span["parentId"]=row[6] if row[6]!="None" else "root"
                span["target"]=row[8].replace('"',"").rstrip()
                #span["source"]=#pid 的target
                span["timestamp"]=row[1]
                span["success"]=row[3]
                span["duration"]=row[2]
                span['cmdb_id'] = row[7]
                span["db"] = None if length<10 else row[-1].replace('"',"").rstrip()
                # 通过其他文件获取其他 KPIs
                span["KPIs"] = getKPIs(span["timestamp"],span['cmdb_id'])

                traceId, span_id= row[4], row[5]
                if res.get(traceId)==None:
                    res[traceId] = {"startTime":span["timestamp"]}
                trace = res[traceId] 
                trace[span_id] = span
            print("文件"+csvName+"_"+day+".csv "+"合并完毕")
            break
        break 
    print("Trace 合并完毕！")
    return res
# res 是一个字典
def saveJson(res):
    p = path+"test_data.json"
    def processJson(js):
        return json.dumps(val)
        # return str(js)
    print("保存路径: "+p)
    with open(p,'w') as f:
        items = list(res.items())
        for i in tqdm(range(len(items)-1), desc="保存数据中", ncols=100, ascii=' #', bar_format='{l_bar}{bar}|'):
            key,val = items[i]
            f.write('{"'+key+'": '+processJson(val)+"}\n")  
    print("保存完毕！")

if __name__=="__main__":
    # saveJson(build_trace())
    with open(path+"test_data.json","r") as f:
        line = f.readline()
        print(line)
        js = json.loads(line)
        print(js)
        
            
