import numpy as np
import csv
import os
import matplotlib.pyplot as plt
import data_path
from read_data import readCSV
path=os.path.join(data_path.get_data_path(),"调用链指标","Order_by_cmdid")

def draw(value,title):
    plt.plot(value,color='r')
    plt.title(title)
    plt.show()

if __name__ == '__main__':
    #value=[1,3,7,4,8,9,2,5,6,7,8]
    fileName="docker_003.csv"
    filepath=os.path.join(path,fileName)
    res=np.array(readCSV(filepath))
    print(res.shape)
    print(1)
    value=res[:,2].astype(np.float)
    draw(value,title=fileName.split(".")[0]+"_"+"csf_001")