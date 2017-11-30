'''
统计每天周换手率超过3%，以及20日均线距离250日均线小于1%的的股票数量及占比
过滤条件：全体A股，剔除当日停牌，上市不满一年的
输入：WIND数据
输出：mysql数据库

history
v0.0 20171030，制作模板，
v0.1 20171031，每日计数的变量用DataFrame存储，
v0.2 20171031，上市满一年和满足条件的逻辑关系梳理，代码修改，
v0.3 20171128，迁移到WIND
v0.4 20171129，逻辑梳理，各个板块的统一以及多线程
'''
# 导入函数库
from pylab import *
mpl.rcParams['font.sans-serif'] = ['SimHei'] # 中文乱码的问题
from WindPy import * # 导入wind接口
from sqlalchemy import create_engine #导入数据库接口  
import datetime,time,calendar
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy
import math
import threading

if __name__ == '__main__':
    w.start()
    now = datetime.datetime.now()
    date_str = now.strftime("%Y-%m-%d")
    # 1.获取当前股票列表
    target_list = w.wset("sectorconstituent","date="+date_str+";sectorid=a001010100000000").Data[1] #当日标的成分
    # 2.获取每一只股票的首发日
    array = []
    for security in target_list:
        try:       
            ipo_date = w.wsd(security, "ipo_date", "ED0D", "2017-11-28", "PriceAdj=F").Data[0][0]
            if (now-ipo_date).days < 1095:
                ipo_date_str = ipo_date.strftime("%Y-%m-%d")
                begin = w.tdaysoffset(1, ipo_date_str, "").Data[0][0].strftime("%Y-%m-%d")
                trade_days = w.tdays(begin, date_str, "").Data[0]
                _wsd = w.wsd(security, "rel_ipo_chg", begin, date_str, "PriceAdj=F").Data[0]
                for i in range(0,len(_wsd)-1):
                    list_temp = []
                    if _wsd[i]>0 and _wsd[i+1]<0:
                        print(security)
                        list_temp.append(security)
                        list_temp.append(trade_days[i+1])
                        array.append(list_temp)
        except BaseException:
            print(BaseException)
            continue
        else:
            continue
    df_result = pd.DataFrame(array, index=range(0, len(array)), columns=['code', 'break_date'])
    engine = create_engine('mysql+pymysql://root:root@localhost:3306/micro?charset=utf8')#用sqlalchemy创建引擎
    df_result.to_sql('break', engine, if_exists='replace', index=True)  #增量入库
    print("mission complete!")