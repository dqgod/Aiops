# coding=utf-8
'''
读取excel文件
'''
import os
import data_path
from read_data import read_xlrd
import json


def fill_indicator(table, indicator):
    table_head = table.row_values(0)
    name_index, bomc_id_index, collect_index = 0, 0, 0
    for i in range(table.ncols):
        if table_head[i] == '指标名称':
            name_index = i
        elif table_head[i] == "bomc_id":
            bomc_id_index = i
        elif table_head[i] == '采集频率':
            collect_index = i
    for i in range(1, table.nrows):
        value = table.row_values(i)
        # indicator[(value[bomc_id_index],value[name_index].split(r'[')[0])] = int(value[collect_index])
        # indicator[value[name_index].split(r'[')[0]] = int(value[collect_index])
        indicator[value[bomc_id_index]] = (
            int(value[collect_index]), value[name_index].split(r'[')[0])

# 查询所有指标
def fill_all_indicators(path_prex):
    indicators = {}  # {db:{ id,indicator1:{}}}
    houzhui = '.xlsx'
    paths = ['2数据库指标', '2DCOS指标', '2操作系统指标', '2中间件指标']
    names = ['db', 'docker', 'os', 'redis']
    for p, name in zip(paths, names):
        path = os.path.join(path_prex, p+houzhui)  # path_prex+p+houzhui
        table = read_xlrd(excelFile=path)
        # 初始化，如果不存在，先初始化一个空的
        if not indicators.get(name):
            indicators[name] = {}
        fill_indicator(table, indicators[name])
    return indicators


if __name__ == '__main__':
    indicators = fill_all_indicators(data_path.data_instruction_path)
    print(json.dumps(indicators.get("db"), indent=4))
