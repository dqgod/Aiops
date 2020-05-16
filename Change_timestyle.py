import time
import csv
from tqdm import tqdm
import numpy as np
import data_path
import os
from read_data import readCsvWithPandas
def timeStamp_to_datetime(timeStamp):
    datetime=[]
    for stamp in timeStamp:
        stamp=int(stamp)/1000
        timeArray=time.localtime(stamp)
        datetime.append(time.strftime("%Y--%m--%d %H:%M:%S", timeArray))
    return  datetime
if __name__ == '__main__':
    #timeStamp=[1586535484617,1586535484750]
    path=data_path.get_data_path("2020_04_22")
    path=os.path.join(path,"调用链指标")
    files=os.listdir(path)
    #files=["trace_fly_remote.csv"]
    for file in files[1:]:
        print(os.path.join(path, file))
        res=readCsvWithPandas(os.path.join(path,file))
        timeStamp=res[:,1]
        dateTime=timeStamp_to_datetime(timeStamp)
        #for i in tqdm(range(len(res)), desc="转换数据格式"+file, ncols=100, ascii=' #', bar_format='{l_bar}{bar}|'):
        res[:,1]=dateTime
        with open(os.path.join(path,"dateTimeStyle\\"+file),"w",newline="") as csvfile:
            writer=csv.writer(csvfile)
            writer.writerows(res)




