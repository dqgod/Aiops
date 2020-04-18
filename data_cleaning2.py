import csv
import os
import sys
import json
from tqdm import tqdm
import time
path = "F:\\aiops\\data_all\\2020_04_11\\调用链指标\\"
path2 = "F:\\aiops\\data_all\\2020_04_11\\平台指标\\"
maxPeriod = 600000
fileNames = {'os': 'os_linux.csv', 'container': 'dcos_container.csv',
             'db': 'db_oracle_11g.csv', 'docker': 'dcos_docker.csv'}
# datestamps=["datestamp=2020-02-14","datestamp=2020-02-15","datestamp=2020-02-16","datestamp=2020-02-17","datestamp=2020-02-18","datestamp=2020-02-19","datestamp=2020-02-20"]
datestamps = [""]
traceNames = ["trace_osb", "trace_csf", "trace_fly_remote",
              "trace_remote_process", "trace_local", "trace_jdbc"]
deploys = {'csf_001': {"docker": "docker"}}


def run():
    # saveJson(build_trace())
    f2 = open(path+"one_trace.json", "w")
    with open(path+"test_data.json", "r") as f:
        line = f.readline()
        print(line)
        js = json.loads(line)
        f2.write(json.dumps(js, indent=4))
        # trace = Trace(js)
        # print(trace)
        # print(trace.getGraph())
    f2.close
# todo 读取文件，并以列表的形式返回


def readCSV(p):
    res = []
    with open(p, 'r') as f:
        reader = csv.reader(f)
        res = list(reader)
    return res


file_now = {}  # todo 保存当前已经加载的 文件（后面可能会用到）


def getKPIs(timeStamp, cmd_id, dsName=None):
    if not cmd_id and not dsName:
        return {}
    key = cmd_id.split('_')[0] if not dsName else dsName.split('_')[0]
    fileName = fileNames.get(key)
    res = ""
    if file_now.get(fileName) == None:
        csv_file = readCSV(path2+fileName)
        res = sorted(csv_file[1:], key=lambda x: x[3])
        fileNames[fileName] = res
    else:
        res = fileNames[fileName]
    valueJson = {}
    timeJson = {}
    low_index, high_index = binarySearch(res, timeStamp-maxPeriod, 0, len(res)-1), \
        binarySearch(res, timeStamp+maxPeriod, 0, len(res)-1)
    for i in range(low_index, high_index):
        row = res[i]
        time = abs(int(row[3])-timeStamp)
        if row[5] == cmd_id or row[5] == dsName:
            if valueJson.get(row[1]) != None:  # 如果已经有了
                if time < timeJson[row[1]]:
                    valueJson[row[1]] = row[4]
                    timeJson[row[1]] = time
                else:  # res是按照时间递增的,因此开始时一定是逐渐减少，之后一定是开始增大
                    break
            else:
                valueJson[row[1]] = row[4]
                timeJson[row[1]] = time
    return valueJson

# ? 二分查找


def binarySearch(res, timestamp, low, high):
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
# 将所有文件合并成 trace


def build_trace():
    res = {}
    print("开始trace数据合并！")
    merge_time = []
    for day in datestamps:
        for traceName in traceNames:
            # csvName = "trace_csf"
            p = os.path.join(path, traceName)+".csv"
            print("正在读取文件 "+traceName)
            temp = readCSV(p)
            print("读取"+traceName+"完毕，开始生成trace")
            time1 = time.time()
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
                    res[traceId] = {"startTime": span["timestamp"]}
                trace = res[traceId]
                trace[span_id] = span

            time_spend = time.time()-time1
            merge_time.append(time_spend)
            print("文件"+traceName+"_"+day+".csv " +
                  "合并完毕,共花费 "+str(time_spend)+"S")
            # break
        break
    print("Trace 合并完毕！共花费 " + str(sum(merge_time))+"S,分别是", merge_time)
    return res
# 将一行数据 row 放到字典中


def generate_span(row):
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
    # 通过其他文件获取其他 KPIs
    # span["KPIs"] = getKPIs(
    #     int(span["timestamp"]), span['cmdb_id'], span["db"])
    return span
# res 是一个字典


def saveJson(res: dict):
    p = path+"test_data.json"

    def processJson(js):
        return json.dumps(js)
        # return str(js)
    print("保存路径: "+p)
    start_time = time.time()
    with open(p, 'w') as f:
        # res中每一条数据都是 traceId: trace
        items = list(res.items())
        length = len(items)
        for i in tqdm(range(length), desc="保存数据中", ncols=100, ascii=' #', bar_format='{l_bar}{bar}|'):
            # ? 保存数据时同时 得到 KPIs
            traceId, trace = items.pop(0)
            generate_KPIs_for_trace(trace)

            f.write('{"'+traceId+'": '+processJson(trace)+"}\n")
    print("保存完毕!花费 "+str(time.time()-start_time)+"S")


# todo 传入一条 trace， 并得到他的KPIs
def generate_KPIs_for_trace(trace):
    graph = generateGraph(trace)
    for k, v in trace.items():
        if k != "startTime":
            child1 = graph.get(k)  # child保存的时 k 的子节点的 id，是一个列表
            '''
            #todo 如果他有字节点，通过子节点找到自己属于哪个docker，如果没有
            #todo 则说明他是叶子节点，也就是db层，此时传入db name
            '''
            cmd_id = trace[child1[0]]['cmdb_id'] if child1 != None else (
                v['target'] if "db" in v['target'] else None)
            v['KPIs'] = getKPIs(int(v["timestamp"]), cmd_id)


def generateGraph(trace):
    graph = {}
    for key, val in trace.items():
        if key != "startTime":
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
    run()
