import csv
import os
import sys
import json
from tqdm import tqdm
path = 'F:\\aiops\\data_all\\'
datestamps=["datestamp=2020-02-14","datestamp=2020-02-15","datestamp=2020-02-16","datestamp=2020-02-17","datestamp=2020-02-18","datestamp=2020-02-19","datestamp=2020-02-20"]
csvNames=["aiops_trace_remote_process","aiops_trace_osb","aiops_trace_local",
"aiops_trace_jdbc","aiops_trace_fly_remote","aiops_trace_csf"]


def readCSV(path):
    res = []
    with open(path, 'r') as f:
        reader = csv.reader(f)
        res=list(reader)
    return res
def build_trace():
    res = {}
    print("开始trace数据合并！")
    for day in datestamps:
        for csvName in csvNames:
            p=path+day+"\\"+csvName+"_"+day+".csv"
            # p=r"F:\\aiops\data_all\\datestamp=2020-02-14\\aiops_trace_local_datestamp=2020-02-14.csv"
            print("正在读取文件 "+csvName)
            temp = readCSV(p)
            # print(temp[2])
            # print(temp[1])
            for i in tqdm(range(len(temp)-1), desc=csvName, ncols=100, ascii=' =', bar_format='{l_bar}{bar}|'):
                row = temp[i+1]
                
            # for row in temp[1:]:
                length = len(row)
                if length<=3:
                    continue
                span = {}
                span["parent"]=row[5] if row[5]!="None" else "root"
                span["target"]=row[6].replace('"',"").rstrip()
                #span["source"]=#pid 的target
                span["timestamp"]=row[0]
                span["success"]=row[2]
                span["duration"]=row[1]
                span["db"] = None if length<8 else row[length-1].replace('"',"").rstrip()
                if res.get(row[3])==None:
                    res[row[3]] = {}
                trace = res[row[3]] 
                trace[row[4]] = span
            print("文件"+csvName+"_"+day+".csv "+"合并完毕")
        break 
    print("Trace 合并完毕！")
    # print("开始转换成json串！")
    # print("Json串转换完毕！")
    # print("开始保存！")
    # with open(path+"test_data.json",'w') as f:
    #     f.write("{\n")
    #     for key,val in map.items():
    #         js = json.dumps(val,indent=4)
    #         f.write(key+": "+js)
    #     f.write("}\n")
    # # with open(path+"test.data.json",'w') as f:
    # #     f.write(json_str)
    # print("保存完毕！")
    return res
def build_trace2():
    res = []
    print("开始trace数据合并！")
    for day in datestamps:
        for csvName in csvNames:
            p=path+day+"\\"+csvName+"_"+day+".csv"
            print("正在读取文件 "+csvName)
            temp = readCSV(p)
            for i in tqdm(range(len(temp)-1), desc=csvName, ncols=100, ascii=' =', bar_format='{l_bar}{bar}|'):
                row = temp[i+1]
                length = len(row)
                if length<=3:
                    continue
                span = {}
# [0'startTime', 1'elapsedTime', 2'success', 3'traceId ', 4'id', 5'pid', 6'serviceName']
                span['traceId']=row[3]
                span['name'] = row[6].replace('"',"").rstrip()
                span['id'] = row[4]
                if row[5]!="None":
                    span["parentId"]=row[5]
                span['timestamp'] = int(row[0]+"000")
                span["duration"]=int(row[1]+'000')
                # span["success"]=row[2]
                span['localEndpoint'] = {'serviceName':span['name'].replace('"',"").rstrip()}
                if length>=8:
                    span['remoteEndpoint'] = {'serviceName':row[-1]}
                # span["db"] = None if length<8 else row[length-1].replace('"',"").rstrip()
                res.append(span)
            print("文件"+csvName+"_"+day+".csv "+"合并完毕")
        break 
    print("Trace 合并完毕！")
    # print("开始转换成json串！")
    # print("Json串转换完毕！")
    print("开始保存！")
    length = len(res)
    with open(path+"test_data2.json",'w') as f:
        f.write("[\n")
        for i in tqdm(range(len(res)), desc='保存中', ncols=100, ascii=' #', bar_format='{l_bar}{bar}|'):
            span = res[i]
        # for i,span in enumerate(res):
            if i==length-1:
                break
            js = json.dumps(span,indent=4)
            f.write(js+',\n')
        js = json.dumps(res[-1],indent=4)
        f.write(js+'\n')
        f.write("]\n")
    print("保存完成！")
    # return res
if __name__=="__main__":
    res = build_trace()
    for key,val in res.items():
        print(key,": \n",json.dumps(val,indent=4))
        break
    # traceId = "2f4951703f478c793094"
    # trace = []
    # for span in res:
    #     if span['traceId'] == traceId:
    #         trace.append(span)
    # with open(path+"one_trace.json",'w') as f:
    #     json_str = json.dumps(trace,indent=4)
    #     f.write(json_str)
    # print("wenjian")
    # with open(path+"test_data2.json","r") as f:
    #     for i in range(1000):
    #         line = f.readline()
    #         # if "{" in line and "}" in line :
    #         #     print("}")
    #         #     break
    #         print(line)