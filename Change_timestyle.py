import time
import csv
from tqdm import tqdm
import numpy as np
import data_path
import os
from read_data import readCsvWithPandas

# timeStamp 单位ms
def timeStamp_to_datetime(timeStamp):
    datetime=[]
    for stamp in timeStamp:
        stamp=int(stamp)/1000
        timeArray=time.localtime(stamp)
        datetime.append(time.strftime("%Y-%m-%d %H:%M:%S", timeArray))
    return  datetime

def switch(timestamp):
    stamp=int(timestamp)/1000
    timeArray=time.localtime(stamp)
    return  timeArray
# 单位ms
def date_to_timestamp(day):
    timeArray = time.strptime(day, "%Y_%m_%d")
    return int(time.mktime(timeArray))*1000
if __name__ == '__main__':
    t = "1590527640000"
    # print(timeStamp_to_datetime(["1590527640000"]))
    print(date_to_timestamp("2020_05_22"))




