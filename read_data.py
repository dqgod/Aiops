from sys import path_hooks
from matplotlib.pyplot import table
import xlrd
import os
import csv
import pandas as pd
import json
import data_path
import os
def read_xlrd(excelFile):
    '''
    读取Execel
    '''
    data = xlrd.open_workbook(excelFile)
    table = data.sheet_by_index(0)
    return table


def readCSV(p):
    # todo 文件不存在直接返回
    if not os.path.exists(p):
        return []
    # todo 读取文件，并以列表的形式返回
    res = []
    with open(p, 'r') as f:
        reader = csv.reader(f)
        res = list(reader)
    return res


def readCsvWithPandas(p):
    '''
    读取CSV文件， 通过pandas
    '''
    # todo 文件不存在直接返回
    if not os.path.exists(p):
        return []
    # todo 读取文件，并以列表的形式返回
    res = []
    res = pd.read_csv(p,engine="python").values
    return res
def read_json(p):
    if not os.path.exists(p):
        return []
    res=[]
    with open(p,'r')as f:
        json_data=json.load(f)
        return json_data

if __name__ == '__main__':
    print("daf")
    day = "2020_05_22"
    path = os.path.join(data_path.get_data_path(day), "数据说明", "0故障说明.xlsx")
    table = read_xlrd(path)
    print(table)
    
