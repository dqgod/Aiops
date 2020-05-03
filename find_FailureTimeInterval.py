import csv
import os
import numpy as np
import pandas as pd

n=3
path="D:\\data\\utf-8' '2020_04_11\\2020_04_11\\业务指标\\esb.csv"

def threeSigma():

    return
def readCSV(p):
    #todo 文件不存在直接返回
    if not os.path.exists(p):
        return []
    # todo 读取文件，并以列表的形式返回
    res = []
    res=pd.read_csv(p).values
    return res
def failureTimeInterval():
    res=readCSV(path)
    timeStamp=res[:,1]
    avg_time=res[:,2]
    succee_rate=res[:,-1]
    #print(avg_time[:5],timeStamp[:5],succee_rate[:5])
    threeSigma(timeStamp,avg_time,succee_rate)
if __name__ == '__main__':
    failureTimeInterval()