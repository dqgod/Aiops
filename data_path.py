import os
def get_data_path(day="2020_04_11"):
    prex_path = "/home/bdilab/aiops"
    if os.name=='nt':# windows , linux 是posix
        prex_path = "D:/aiops"
    return os.path.join(prex_path,day)
def get_save_path():
    return "result/"
fileNames = {'os': 'os_linux.csv', 'container': 'dcos_container.csv',
             'db': 'db_oracle_11g.csv', 'docker': 'dcos_docker.csv'}
traceNames = ["trace_osb", "trace_csf", "trace_fly_remote",
              "trace_remote_process", "trace_local", "trace_jdbc"]