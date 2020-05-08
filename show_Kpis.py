#%%
from read_data import readCSV
import numpy as np
import os
import matplotlib.pyplot as plt
import data_path
path = os.path.join(data_path.get_data_path(),"平台指标")
#%%
def getKpis(files = None):
    """[获取KPI曲线数据,将所有指标数据读到内存]

    Args:
        files ([type], optional): [传入文件列表 如list(["dcos_docker.csv"])]. Defaults to None.\n

    Returns:
        [type]: 字典{(cmdb_id,name,bomc_id,itemid):[row1,row2......]} 每一行 ['itemid', 'name', 'bomc_id', 'timestamp', 'value', 'cmdb_id']
    """
    files = os.listdir(path) if not path else files
    kpis = {}
    for f in files:
        # f = "dcos_docker.csv"
        p = os.path.join(path,f)
        if not os.path.isfile(p):
            continue
        data = readCSV(p)
        # print(data[0])
        for row in data[1:]:
            ['itemid', 'name', 'bomc_id', 'timestamp', 'value', 'cmdb_id']
            key = "%s,%s,%s,%s" % (row[5],row[1],row[2],row[0])
            if kpis.get(key) == None:
                kpis[key] = []
            kpis[key].append(row)
        # break
    return kpis

# print("加载完毕getKpis方法完毕")


#%% 
# 展示一条曲线
def show_a_kpi_Curve(values,title=None):
    values = np.array(sorted(values, key=lambda x: x[3]))
    x = range(len(values))
    y = values[:,4].astype(np.float)
    # print(type(y))
    max_ = int(max(y)+1)
    min_ = int(min(y))
    # new_ticks = np.linspace(min_,max_,6)
    # # print(new_ticks)
    # plt.yticks(new_ticks)
    plt.ylim((min_,max_))
    plt.plot(x,y,color='r')
    plt.title(title)

    # 异常时间区间
    start,end =  1586544600000, 1586544900000+240*1000
    lam = lambda x: int(x[3])<end and int(x[3])>start
    # abn_data = np.array(list(filter(lam,values)))
    # print("异常数据",abn_data[:,4])
    for i,x in enumerate(values):
        if lam(x):
            print(i,x[4])

    plt.show()
# print("加载show_a_kpi_Curve方法完毕")

#%% 
# filter_list筛选列表，满足列表中条件才会被展示，为 None时 展示全部
def showKpiCurve(kpis,filter_list=None):
    filter_list = [] if not filter_list else filter_list
    for key,val in kpis.items():
        temp = key.split(",")
        title = "(%s, %s, %s)" % (temp[0],temp[1],temp[2])
        if list(filter(lambda x: x not in title,filter_list)):
            continue
        show_a_kpi_Curve(val,title)
        # break
def main():
    print("读取文件信息：")
    kpis = getKpis(["dcos_docker.csv"])
    filter_list = ["docker_002","cpu"]    
    print("画图中：")
    showKpiCurve(kpis,filter_list)

# main()


# %%

