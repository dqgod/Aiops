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
# [0'startTime', 1'elapsedTime', 2'success', 3'traceId ', 4'id', 5'pid', 6'serviceName']
def build_trace():
    res = {}
    graph = {}
    print("开始trace数据合并！")
    for day in datestamps:
        for csvName in csvNames:
            p=path+day+"\\"+csvName+"_"+day+".csv"
            # p=r"F:\\aiops\data_all\\datestamp=2020-02-14\\aiops_trace_local_datestamp=2020-02-14.csv"
            merge_trace(p,res,csvName,day)
            # break
        for trace in tqdm(res.values(), desc='构建图中', ncols=100, ascii=' #', bar_format='{l_bar}{bar}|'):
            generate_graph_2(trace,graph)
        res = {}
        break 

    print("Trace 合并完毕！")
    print("正在构建全图!")
    # for trace in tqdm(res.values(), desc='构建图中', ncols=100, ascii=' #', bar_format='{l_bar}{bar}|'):
    #     generate_graph_2(trace,graph)
    for val in graph.values():
        traverGraph(val)
    print("构建完毕")
    print("正在保存")
    with open(path+"graph3.json",'w') as f:
        json_str = json.dumps(graph,indent=4)
        f.write(json_str)
    print("保存成功!")

def merge_trace(path,res,csvName,day):
    print("正在读取文件 "+csvName)
    temp = readCSV(path)
    for i in tqdm(range(len(temp)-1), desc=day+" "+csvName, ncols=100, ascii=' =', bar_format='{l_bar}{bar}|'):
        row = temp[i+1]
        length = len(row)
        if length<=3:
            continue
        span = {}
        span["parentId"]=row[5] if row[5]!="None" else "root"
        span["target"]=row[6].replace('"',"").rstrip()
        #span["source"]=#pid 的target
        span["timestamp"]=row[0]
        span["success"]=row[2]
        span["duration"]=row[1]
        span["db"] = None if length<8 else row[length-1].replace('"',"").rstrip()
        # 当前数据归属的traceId 是否存在，如果不存在创建出该traceId的dict
        if res.get(row[3])==None:
            res[row[3]] = {}
        trace = res[row[3]] 
        #将span放到trace里
        trace[row[4]] = span
    print("文件"+csvName+"_"+day+".csv "+"合并完毕")
def generate_graph_1(trace,graph):
    for span in trace.values():
        if span["parentId"] == 'root':
            graph[span['target']] = []
            continue
        parentSpan = trace[span["parentId"]] 
        if graph.get(parentSpan['target']) == None:
            graph[parentSpan['target']] = []
        if span['target'] not in graph[parentSpan['target']]:
            graph[parentSpan['target']].append(span['target'])
def generate_graph_2(trace,graph):
    '''
    tree :{
        id:[]
        id:[]
    }
    '''
    tree = {}
    for current_id, span in trace.items():
        if tree.get(span['parentId']) == None:
            tree[span['parentId']] = []
        tree[span['parentId']].append(current_id)
    deepFirst('root',tree,graph,trace)

# 深度优先
def deepFirst(tree_id,tree,graph,trace):
    if tree_id not in tree.keys():
        return
    for current_id in tree[tree_id]:
        span = trace[current_id]
        name = span['target']
        if graph.get(name)==None:
            # 塑造全图节点,每一个节点有KPIs
            graph[name] = {'KPIs':{}}
        # 获取当前结点的KPIs
        KPIs = graph[name]['KPIs']
        if KPIs.get('num') == None:
            KPIs['num'] = 0
            KPIs['duration'] = 0
            KPIs['max'] = 0
            KPIs['min'] = 10000000000
        KPIs["num"] += 1
        duration = int(span['duration'])
        KPIs["duration"] += duration
        KPIs['max'] = duration if duration > KPIs['max'] else KPIs['max']
        KPIs['min'] = duration if duration < KPIs['min'] else KPIs['min']
        deepFirst(current_id,tree,graph[name],trace)
# { osb:{}} 从osb的value开始
def traverGraph(graph):
     
    KPIs = graph['KPIs']
    KPIs["average_duration"] = KPIs["duration"]/KPIs["num"]
    for key,val in graph.items():
        if key != 'KPIs':
            traverGraph(val)

if __name__ == "__main__":
    build_trace()
