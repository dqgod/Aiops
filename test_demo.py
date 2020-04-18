import datetime
import time
import os
import re
import numpy as np
def swichTime(t:int,plus=0) -> str:
    t=(t/1000)+plus
    return datetime.datetime.fromtimestamp(t)

def run():
    print(np.log(1000000))
    # print(time.time())
    # print(swichTime(time.time()))
    # print(swichTime(1587113900685,))
    # print(1581609715000-60000)
    # print(time.time())
    # with open('a','r') as f:
    #     it = f.readline()
    #     while it:
    #         print(it.rstrip())
    #         it = f.readline()        

 
def operate_file():
    # print (os.sep)
    # print (os.name)
    # print (os.getenv('path'))
    path = os.getcwd()
    files= os.listdir(path)
    print (files)
    #拼接了路径
    fullpath=os.path.join(path,files[0])
    print (fullpath)
    #判断一个路径是否是一个文件，是否目录
    if os.path.isfile(fullpath):
        print ('我是一个文件')
    elif os.path.isdir(fullpath):
        print ('我是一个目录')

if __name__=="__main__":
    run()
  