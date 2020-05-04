import csv
import os
import numpy as np
import pandas as pd
import data_path
from read_data import readCsvWithPandas, readCSV
n = 3
path = os.path.join(data_path.get_data_path(), "业务指标", "esb.csv")


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
    data = np.array(readCSV(path)[1:])
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
    dq_find_time_interval()
