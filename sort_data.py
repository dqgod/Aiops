from filecmp import cmp
from numpy import long
from tqdm import tqdm
import os
import csv
import re
from read_data import readCSV
import data_path 
path = os.path.join(data_path.get_data_path(),"调用链指标")


def order(path):
    files = os.listdir(path)
    for file in files:
        temp = readCSV(os.path.join(path , file))
        row_first = temp[0]
        temp = sorted(temp[1:], key=lambda x: x[1])
        new_name = file.split(".")[0]+"_sorted"
        with open(path+new_name+".csv", 'w', newline="") as fd:
            writer = csv.writer(fd)
            writer.writerow(row_first)
            for row in tqdm(temp, desc=file, ncols=100, ascii=' =', bar_format='{l_bar}{bar}|'):
                writer.writerow(row)
        print("文件"+file+"排序完成")


def divide_file(path,save_path = None): # 返回文件存储路径
    files = os.listdir(path)
    save_path = path if not save_path else save_path
    for file_name in files:
        if "csv" not in file_name:
            continue
        # 读取一个文件数据
        data = readCSV(os.path.join(path,file_name))[1:]
        # 获取该文件的前缀如 db、docker、os、redis
        new_dir = file_name.split('_')[0] if 'dcos' not in file_name else re.split('[_.]',file_name)[1]
        new_dir = 'redis' if 'redis' in file_name else new_dir

        # 建立一个新的目录
        new_path = os.path.join(save_path, new_dir)
        if not os.path.exists(new_path):
            os.mkdir(new_path)
        # 存储所有指标对应的数据
        data_of_indicators = {}  #{ 指标名:data}
        # 遍历数据
        for row in data:
            indicator_name = row[1]  # 获取指标名
            # 判断是不是第一次遇见新的指标名,是的话创建
            if not data_of_indicators.get(indicator_name):
                data_of_indicators[indicator_name] = []
            data_of_indicators[indicator_name].append(row)
        # 将这些指标写入到文件
        print(file_name)
        for indicator_name, data in tqdm(data_of_indicators.items(), desc="写入文件中", ncols=100, ascii=' #', bar_format='{l_bar}{bar}|'):
            p = os.path.join(new_path, indicator_name+".csv")
            with open(p, 'w', newline="") as fd:
                writer = csv.writer(fd)
                for row in data:
                    writer.writerow(row)
    return save_path

def getKpi(timesta, comd_id=None):
    pass


if __name__ == '__main__':
    # order(path)
    p = os.path.join(data_path.get_data_path("2020_04_22"),"调用链指标")
    order(p)
    # os.mkdir(path="F:\\aiops\\data_all\\2020_04_11\\平台指标\\test")
    # s = "5min"
    # print(re.match('\d*',s))
