import csv
import os
import numpy as np
import pandas as pd
from sklearn import preprocessing

import data_path
from read_data import readCsvWithPandas
n=3
path= os.path.join(data_path.get_data_path(),"业务指标","esb.csv")

def threeSigma(timeStamp,avg_time,succee_rate):

    return

def failureTimeInterval():
    res=readCsvWithPandas(path)
    timeStamp=res[:,1]
    avg_time=res[:,2]
    succee_rate=res[:,-1]
    #print(avg_time[:5],timeStamp[:5],succee_rate[:5])
    threeSigma(timeStamp,avg_time,succee_rate)
if __name__ == '__main__':
    failureTimeInterval()
    res2=readCsvWithPandas(path)
    res=res2[:,2]
    # 建立MinMaxScaler对象
    minmax = preprocessing.MinMaxScaler()
    # 标准化处理
    
