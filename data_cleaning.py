from readExcel import fill_all_indicators
from sort_data import divide_file
import csv
import os
import sys
import json
from tqdm import tqdm
import time
import data_path 
from read_data import readCSV
def getPath():
    '''
    return (p1,p2,p3) (调用链指标,平台指标,数据说明)
    '''
    prex_path_all = data_path.get_data_path()
    p1 = os.path.join(prex_path_all,"调用链指标")
    p2 = os.path.join(prex_path_all,"平台指标")
    p3 = os.path.join(prex_path_all,"数据说明")
    return p1,p2,p3

path, path2, path3 = getPath() #路径


fileNames = data_path.fileNames
# datestamps=["datestamp=2020-02-14","datestamp=2020-02-15","datestamp=2020-02-16","datestamp=2020-02-17","datestamp=2020-02-18","datestamp=2020-02-19","datestamp=2020-02-20"]
datestamps = [""]
traceNames = data_path.traceNames
# deploys = {'csf_001': {"docker": "docker"}}

# todo { cmd_id:{ timestamp:123456,values:{}}}
kpis = {}  # 记录下指标
# todo {db:{  indicator1:time1,indcator2:time2}}
indicators = fill_all_indicators(path3)
# todo 保存当前已经加载的 文件（后面可能会用到）
file_now = {}

bias = 10*1000
def main():
    time0 = time.time()
    if len(os.listdir(path2))<=5:
        divide_file(path2)
    save_path = os.path.join(path,"test_data.json")
    saveJson(build_trace(path),save_path)
    # f2 = open(os.path.join(path,"problem_trace.json"), "w")
    # with open(save_path, "r") as f:
    #     line = f.readline()
    #     while line:
    #         js = json.loads(line)
    #         startTime = int(js[list(js.keys())[0]]['startTime'])
    #         if startTime>problemTime[0] and startTime<problemTime[1]:
    #             f2.write(json.dumps(js, indent=4))
    #             break
    #         line = f.readline()
    # f2.close
    print("程序运行完毕！花费: "+str(time.time()-time0)+" 秒")


def build_trace(path):
    """[summary]

    Args:
        path ([str]):  trace调用链路径 \n

    Returns:
        [type]: traces字典 格式{traceId:{ startTime:str,spans:{spanId}}} \n
    """
    # todo 将所有文件合并成 trace
    res = {}
    print("开始trace数据合并！")
    merge_time = []
    for day in datestamps:
        for traceName in traceNames:
            # csvName = "trace_csf"
            p = os.path.join(path, traceName+".csv")
            time1 = time.time()
            print("正在读取文件 "+traceName)
            temp = readCSV(p)
            print("读取"+traceName+"完毕，开始生成trace")
            for i in tqdm(range(len(temp)-1), desc=traceName, ncols=100, ascii=' =', bar_format='{l_bar}{bar}|'):
                # 0 callType,1 startTime,2 elapsedTime,3 success,4 traceId,5 id,6 pid,7 cmdb_id,8 serviceName
                row = temp[i+1]
                if len(row) <= 3:
                    continue
                # 通过row 构造span
                span = generate_span(row)
                # 将span放到相应的trace中
                traceId, span_id = row[4], row[5]
                if res.get(traceId) == None:
                    res[traceId] = {
                        "startTime": span["timestamp"], "spans": {}}
                spans = res[traceId]["spans"]
                spans[span_id] = span
                # break
            time_spend = time.time()-time1
            merge_time.append(time_spend)
            print("文件"+traceName+"_"+day+".csv " +
                  "合并完毕,共花费 "+str(time_spend)+"S")
            # break
        # break
    print("Trace 合并完毕！共花费 " + str(sum(merge_time))+"S,分别是", merge_time)
    return res



def getKPIs(timeStamp, cmd_id, bias=0):
    if not cmd_id:
        return {}
    key = cmd_id.split('_')[0]
    # 时间偏差不大，直接返回已经记录的
    if kpis.get(cmd_id) != None and abs(kpis[cmd_id]["timestamp"] - timeStamp) <= bias:
        return kpis[cmd_id]["values"]
    valueJson = {}
    # 遍历所有指标名，闭关将相应的指标获取
    for bomc_id, (sample_period,indicator_name) in indicators[key].items():
        # csv 路径
        file_path = os.path.join(path2, key, indicator_name + ".csv")
        # 指标名，取样周期
        valueJson.update(get_kpis_for_an_indicator(
            timeStamp, cmd_id, bomc_id , sample_period*1000,file_path))

    kpis[cmd_id] = {"timestamp": timeStamp, "values": valueJson}
    return valueJson





def get_kpis_for_an_indicator(timeStamp, cmd_id, bomc_id, sample_period,file_path):
    '''
    timeStamp:时间戳 \n
    cmd_id:类似docker_007   \n
    key: docker,cmd_id 前面部分，用作路径   \n
    indicator_name:指标名   \n
    sample_period:取样周期  \n
    '''
    # todo 获取该指标文件存储路径
    res = ""
    # todo 如果该文件没有读取过
    if file_now.get(file_path) == None:
        csv_file = readCSV(file_path)
        if not csv_file:
            return {}
        res = sorted(csv_file[1:], key=lambda x: x[3])
        file_now[file_path] = res
    else:  # 否则直接从dict中读
        res = file_now[file_path]
    valueJson = {}
    timeJson = {}
    low_index, high_index = binarySearch(res, timeStamp-sample_period, 0, len(res)-1), \
        binarySearch(res, timeStamp+sample_period, 0, len(res)-1)
    # print(cmd_id,indicator_name,low_index,high_index)
    # itemid,name,bomc_id,timestamp,value,cmdb_id
    for i in range(low_index, high_index):
        row = res[i]
        time = abs(int(row[3])-timeStamp)
        if row[5] == cmd_id and bomc_id==row[2]:
            # 记录的KEY
            new_key = '(%s,%s,%s)' % (row[0],row[1],row[2])
            if valueJson.get(new_key) != None:  # 如果已经有了
                if time < timeJson[new_key]:
                    valueJson[new_key] = row[4]
                    timeJson[new_key] = time
                else:  # res是按照时间递增的,因此开始时一定是逐渐减少，之后一定是开始增大
                    break
            else:
                valueJson[new_key] = row[4]
                timeJson[new_key] = time
    return valueJson


def binarySearch(res, timestamp, low, high):
    # todo 二分查找
    while low < high:
        middle = (low+high)//2
        cur_time = int(res[middle][3])
        if cur_time == timestamp:  # 在偏差之内不管了
            return middle
        if cur_time > timestamp:
            high = middle-1
        else:
            low = middle+1
    return low


def generate_span(row):
    '''
    0 callType,1 startTime,2 elapsedTime,3 success,4 traceId,5 id,6 pid,7 cmdb_id,8 serviceName
    '''
    # todo 将一行数据 row 放到字典中
    span = {}
    span["parentId"] = row[6] if row[6] != "None" else "root"
    span["target"] = row[8].replace('"', "").rstrip()
    # span["source"]=#pid 的target
    span["timestamp"] = row[1]
    span["success"] = row[3]
    span["duration"] = row[2]
    span['cmdb_id'] = row[7]
    span["db"] = None if len(row) < 10 else row[-1].replace(
        '"', "").rstrip()
    span['callType'] = row[0]
    # 通过其他文件获取其他 KPIs
    # span["KPIs"] = getKPIs(
    #     int(span["timestamp"]), span['cmdb_id'], span["db"])
    return span


def saveJson(res,save_path):
    '''
    res中每一条数据都是 traceId: {starttime:111111, spans:{}}
    '''
    def processJson(js):
        return json.dumps(js)
        # return str(js)
    print("保存路径: "+save_path)
    start_time = time.time()
    with open(save_path, 'w') as f:
        # res中每一条数据都是 traceId: {starttime:111111, spans:{}}
        items = list(res.items())
        length = len(items)  # 数据量
        for i in tqdm(range(length), desc="保存数据中", ncols=100, ascii=' #', bar_format='{l_bar}{bar}|'):
            # ? 保存数据时同时 得到 KPIs
            # traceId, {starttime:111111, spans:{}}
            traceId, trace = items.pop(0)
            generate_KPIs_for_trace(trace)
            f.write('{"'+traceId+'": '+processJson(trace)+"}\n")

    print("保存完毕!花费 "+str(time.time()-start_time)+"S")


def generate_KPIs_for_trace(trace):
    """
    传入一条 trace， 并得到他的KPIs \n
    trace格式 {starttime:111111, spans:{}}
    """
    spans = trace['spans']
    # graph = generateGraph(spans)

    for span_id, span in spans.items():
        # child1 = graph.get(k)  # child保存的时 k 的子节点的 id，是一个列表
        '''
        #todo 如果他有字节点，通过子节点找到自己属于哪个docker，如果没有
        #todo 则说明他是叶子节点，也就是db层，此时传入db name
        '''
        # cmd_id = trace[child1[0]]['cmdb_id'] if child1 != None else (
        #     v['target'] if "db" in v['target'] else None)

        cmd_id = span['cmdb_id'] if not span['db'] else span['db']
        span['KPIs'] = getKPIs(int(span["timestamp"]), span['cmdb_id'], bias)


def generateGraph(trace_spans):
    """传入一条trace中的所有span 将他的所有span生成一个数据字典数据字典的key为span_id, 其值是他子节点的id列表

    Args:
        trace_spans ([dict]): { span_id:{},span_id2:{}}\n
    Returns:
        graph ([dict]): {root:[id1,id2...],id1:[id3,id4..]}
    """
    graph = {}
    for key, val in trace_spans.items():
        if graph.get(val["parentId"]) == None:
            graph[val["parentId"]] = []
        graph[val["parentId"]].append(key)
    return graph


class Trace:
    def __init__(self, trace):
        self.traceId = list(trace.keys())[0]
        self.startTime = trace[self.traceId]['startTime']
        self.spans, self.graph = {}, {}
        for key, val in trace[self.traceId].items():
            if key != "startTime":
                self.spans[key] = Span(key, val, self.traceId)
                if self.graph.get(val["parentId"]) == None:
                    self.graph[val["parentId"]] = []
                self.graph[val["parentId"]].append(key)

    def getSpanById(self, span_id):
        return self.spans.get(span_id)

    def getStartTime(self):
        return self.startTime

    def getGraph(self):
        return self.graph

    def __str__(self):
        span_str = ''
        span_length = len(self.spans)
        for i, val in enumerate(self.spans.values()):
            span_str += (str(val)+',') if i != (span_length-1) else str(val)
        res = '{"%s": {"startTime": %s, %s}}' % (
            self.traceId, self.startTime, span_str)
        return res


class Span:
    def __init__(self, key, val, traceId):
        self.id = key
        self.traceId = traceId
        self.parentId = val['parentId']
        self.target = val['target']
        self.timestamp = val['timestamp']
        self.success = val['success']
        self.duration = val['duration']
        self.cmdb_id = val['cmdb_id']
        self.db = val['db']
        self.KPIs = val['KPIs']

    def __str__(self):
        res = '"%s": {"parentId": %s, "target": %s,"timestamp": %s, "success": %s, "duration": %s, "comdb_id": %s, "db": %s, "KPIs": %s }' % \
            (self.id, self.parentId, self.target, self.timestamp,
             self.success, self.duration, self.cmdb_id, self.db, self.KPIs)
        return res


if __name__ == "__main__":
    main()
