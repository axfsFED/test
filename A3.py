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
from string import Template

if __name__ == '__main__':
    start_time = datetime.datetime.now()
    #启动WIND
    w.start()
    #将结果封装成df_array并写入mysql数据库
    engine = create_engine('mysql+pymysql://root:root@localhost:3306/micro?charset=utf8')#用sqlalchemy创建引擎
    query_sql = """
      select * from $arg1
      """
    query_sql = Template(query_sql) # template方法
    df = pd.read_sql_query(query_sql .substitute(arg1='break'),engine) # 配合pandas的方法读取数据库值
    print(df)
    #获取df['code'], df['break_date']
    pct_chg_5 = []
    pct_chg_10 = []
    pct_chg_20 = []
    pct_chg_30 = []
    pct_chg_60 = []
    code_list = list(df['code'])
    date_list = list(df['break_date'])
    for i in range(0, len(code_list)):
        #获取5/10/20/30/60日涨跌幅
        pct_chg_n5d = w.wss(code_list[i], "pct_chg_nd","days=5;tradeDate="+date_list[i].strftime("%y%m%d")).Data[0][0]
        pct_chg_n10d = w.wss(code_list[i], "pct_chg_nd","days=5;tradeDate="+date_list[i].strftime("%y%m%d")).Data[0][0]
        pct_chg_n20d = w.wss(code_list[i], "pct_chg_nd","days=5;tradeDate="+date_list[i].strftime("%y%m%d")).Data[0][0]
        pct_chg_n30d = w.wss(code_list[i], "pct_chg_nd","days=5;tradeDate="+date_list[i].strftime("%y%m%d")).Data[0][0]
        pct_chg_n60d = w.wss(code_list[i], "pct_chg_nd","days=5;tradeDate="+date_list[i].strftime("%y%m%d")).Data[0][0]
        pct_chg_5.append(pct_chg_n5d)
        pct_chg_10.append(pct_chg_n10d)
        pct_chg_20.append(pct_chg_n20d)
        pct_chg_30.append(pct_chg_n30d)
        pct_chg_60.append(pct_chg_n60d)
    df['pct_chg_5'] = pct_chg_5
    df['pct_chg_10'] = pct_chg_10
    df['pct_chg_20'] = pct_chg_20
    df['pct_chg_30'] = pct_chg_30
    df['pct_chg_60'] = pct_chg_60
    #重新写入数据库
    df.to_sql('new', engine, if_exists='replace', index=True)  #增量入库
    print("mission complete!")
    end_time = datetime.datetime.now()
    print('执行时间:%d mins'%(int((end_time - start_time).seconds)/60))