from filecmp import cmp
from numpy import long
from tqdm import tqdm
import os
import csv
path="F:\\aiops\\data_all\\2020_04_11\\平台指标\\"
def readCSV(path):
    res = []
    with open(path, 'r') as f:
        reader = csv.reader(f)
        res=list(reader)
    return res
def order(path):
    files=os.listdir(path)
    for file in files:
        temp=readCSV(path+file)
        row_first = temp[0]
        temp=sorted(temp[1:],key=lambda x:x[3])
        new_name = file.split(".")[0]+"_sorted"
        with open(path+new_name+".csv",'w',newline="") as fd:
            writer=csv.writer(fd)
            writer.writerow(row_first)
            for row in tqdm(temp, desc=file, ncols=100, ascii=' =', bar_format='{l_bar}{bar}|'):
                writer.writerow(row)
        print("文件"+file+"排序完成")
        
def getKpi(timesta,comd_id=None):
    pass
if __name__ == '__main__':
    order(path)