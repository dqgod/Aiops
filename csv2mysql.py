import csv
import pymysql
import codecs  
import time
import datetime
from tqdm import tqdm

path = 'F:\\aiops\\data_all\\'
datestamps=["datestamp=2020-02-14","datestamp=2020-02-15","datestamp=2020-02-16","datestamp=2020-02-17","datestamp=2020-02-18","datestamp=2020-02-19","datestamp=2020-02-20"]
csvNames=["aiops_trace_remote_process","aiops_trace_osb","aiops_trace_local",
"aiops_trace_jdbc","aiops_trace_fly_remote","aiops_trace_csf"]
'''
python读取csv文件到mysql上
'''
class PyMysql:
    def __init__(self):
        self.conn=self.conn_mysql()
        self.cur=self.conn.cursor()
	#连接数据库
    def conn_mysql(self):
        conn=pymysql.connect(
            host='localhost',   #你的主机IP
            port=3306,    #主机端口，不能加双引号
            user='root',   #MySQL用户
            password='123456',   #MySQL密码
            charset='utf8'   #使用的编码格式，不能使用  utf-8 ，不能加多一个横杠
        )
        return conn

    def close(self):
        self.cur.close()
        self.conn.close()
        
	#创建数据库
    def create_db(self,db_name):
        cur=self.cur #创建光标
        cur.execute("create database if not exists {} character set UTF8MB3;".format(db_name))  #创建数据库
        #切换至改数据库
        cur.execute("use {};".format(db_name))   
        self.conn.commit()  #一定要进行事务更新
        print('创建数据库成功')
	
	#创建表
    def create_table_head(self,head,table_name):
        sql='create table if not exists {}('.format(table_name)  #创建表
        for i,(key,val) in enumerate(head.items()):   
            sql+='{} {}'.format(key,val)
            if i!=len(head)-1: 
                sql+=','
            sql+='\n'
        sql+=');'
        print(sql)
        #创建光标
        self.cur.execute(sql)   #执行命令
        self.conn.commit()  #一定要进行事务更新
        time.sleep(0.1)  
        print('创建表完成')

	#插入数据
    def insert_table_info(self,info,table):
        sql='insert into {} values ('.format(table)
        for i in range(0,len(info)):
            sql+='"{}" '.format(info[i])
            if i!=len(info)-1:
                sql+=','
        sql+=');'
        try:
            self.cur.execute(sql)
            self.conn.commit()  #一定要进行事务更新
            # time.sleep(0.1)
            # print('插入数据成功')
        except Exception as e:
            print('插入数据失败,失败原因',e)

def readCSV(path):
    res = []
    with open(path, 'r') as f:
        reader = csv.reader(f)
        res=list(reader)
    return res
def swichTime(t):
    t=t/1000
    return datetime.datetime.fromtimestamp(t)

if __name__=='__main__':
    pysql=PyMysql()
    pysql.create_db('aiops')
    # p="F:\\aiops\data_all\\datestamp=2020-02-14\\aiops_trace_osb_datestamp=2020-02-14.csv"
    head = {
        "item_id":"int(32) primary key auto_increment",
        "startTime":"datetime(3)",
        "elapsedTime": "int",
        "success": "varchar(5)",
        "traceId":"varchar(30)",
        "id":"varchar(30)",
        "pid":"varchar(30)",
        "serviceName":"varchar(20)",
        "db":"varchar(20)"
    }
    # table = "aiops_trace_osb"
    table_names=csvNames
    for day in datestamps:
        for table in table_names:
            p=path+day+"\\"+table+"_"+day+".csv"
            pysql.create_table_head(head,table)
            rows = readCSV(p)[1:]
            for row in tqdm(rows, desc=day+" "+table, ncols=100, ascii=' #', bar_format='{l_bar}{bar}|'):
                if len(row) < 6:
                    continue
                if len(row) == 7:
                    row.append(None)
                row[0] = swichTime(int(row[0]))
                row[1] = int(row[1])
                row.insert(0,0)
                pysql.insert_table_info(row,table)
        break
    # pysql.create_table_head(head,table)
    # rows = readCSV(p)[1:]
    # for row in tqdm(rows, desc='插入数据库中', ncols=100, ascii=' #', bar_format='{l_bar}{bar}|'):
    #     if len(row) < 6:
    #         continue
    #     if len(row) == 7:
    #         row.append(None)
    #     row[0] = swichTime(int(row[0]))
    #     row[1] = int(row[1])
    #     pysql.insert_table_info(row,table)



