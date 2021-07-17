import os
prex_path = 'D:/文档/aiops'

# 得到数据存放路径
def get_data_path(day="2020_04_22"):

    data_path = os.path.join(prex_path, "final_data")
    if os.name == 'nt':  # windows , linux 是posix
        data_path = "D:/文档/aiops/final_data"
    return os.path.join(data_path, day)

# 得到数据存储路径
def result_save_path():
    return "result/"
# 得到数据存储路径
def data_save_path():
    return "data/"


fileNames = {'os': 'os_linux.csv', 'container': 'dcos_container.csv',
             'db': 'db_oracle_11g.csv', 'docker': 'dcos_docker.csv'}
traceNames = ["trace_osb", "trace_csf", "trace_fly_remote",
              "trace_remote_process", "trace_local", "trace_jdbc"]

# 数据说明路径
data_instruction_path = os.path.join(prex_path, "数据说明")
