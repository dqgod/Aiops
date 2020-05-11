#%%
import csv
import os
import numpy as np
import pandas as pd
from sklearn import preprocessing
import data_cleaning
import data_path
import matplotlib.pyplot as plt
from read_data import readCsvWithPandas, readCSV
n = 3
path = os.path.join(data_path.get_data_path(), "调用链指标")
#%%
path2=data_path.get_data_path()
trace_path=os.path.join(path2,"调用链指标")
print(trace_path)
trace=data_cleaning.build_trace(trace_path)

#%%
data_cleaning.saveJson(trace,trace_path+"\\trace.json")

def threeSigma(timeStamp, avg_time, succee_rate):

    return


def failureTimeInterval():
    res = readCsvWithPandas(path)
    timeStamp = res[:, 1]
    avg_time = res[:, 2]
    succee_rate = res[:, -1]
    # print(avg_time[:5],timeStamp[:5],succee_rate[:5])
    threeSigma(timeStamp, avg_time, succee_rate)


def dq_find_time_interval():
    data = readCsvWithPandas(path)
    avg_times = data[:, 2].astype(np.float32)
    succee_rate = data[:, 5].astype(np.float32)
    std = np.std(avg_times, ddof=1)
    means = np.mean(avg_times)
    high = means+3*std
    # low = means-3*std
    lam = lambda i: avg_times[i]>high or succee_rate[i]<1
    res = data[list(filter(lam,  range(len(data)) ))]
    print(res[:,1])

if __name__ == '__main__':
    # start=1586536500014
    # end=1586536794160
    print("完成")

# %%
