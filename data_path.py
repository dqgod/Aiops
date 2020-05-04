import os
def get_data_path(day="2020_04_11"):
    prex_path = "/home/bdilab/aiops"
    if os.name=='nt':# windows , linux 是posix
        prex_path = "D:/aiops"
    return os.path.join(prex_path,day)

