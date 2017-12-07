'''
统计跌破发行价的股票5/20/60/120/240日的涨跌幅情况
输入：WIND数据
输出：mysql数据库
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
    df = pd.read_sql_query(query_sql .substitute(arg1='break_new_1'),engine) # 配合pandas的方法读取数据库值
    #获取df['code'], df['break_date']
    pct_chg_5 = []
    pct_chg_20 = []
    pct_chg_60 = []
    pct_chg_120 = []
    pct_chg_240 = []
    code_list = list(df['code'])
    date_list = list(df['break_date'])
    for i in range(0, len(code_list)):
        #获取5/10/20/30/60日涨跌幅
        security_code = code_list[i]
        temp = date_list[i].strftime("%Y-%m-%d")
        date_str = w.tdaysoffset(1, temp, "").Data[0][0].strftime("%Y%m%d")
        print(security_code)
        print(date_str)
        #print(_wss.ErrorCode)
        pct_chg_n5d = w.wss(security_code, "pct_chg_nd","days=5;tradeDate="+date_str).Data[0][0]
        pct_chg_n20d = w.wss(security_code, "pct_chg_nd","days=20;tradeDate="+date_str).Data[0][0]
        pct_chg_n60d = w.wss(security_code, "pct_chg_nd","days=60;tradeDate="+date_str).Data[0][0]
        pct_chg_n120d = w.wss(security_code, "pct_chg_nd","days=120;tradeDate="+date_str).Data[0][0]
        pct_chg_n240d = w.wss(security_code, "pct_chg_nd","days=240;tradeDate="+date_str).Data[0][0]
        
        pct_chg_5.append(pct_chg_n5d)
        pct_chg_20.append(pct_chg_n20d)
        pct_chg_60.append(pct_chg_n60d)
        pct_chg_120.append(pct_chg_n120d)
        pct_chg_240.append(pct_chg_n240d)
        
    date_list = [d.date() for d in date_list]
    df['break_date'] = date_list
    df['pct_chg_5(%)'] = pct_chg_5
    df['pct_chg_20(%)'] = pct_chg_20
    df['pct_chg_60(%)'] = pct_chg_60
    df['pct_chg_120(%)'] = pct_chg_120
    df['pct_chg_240(%)'] = pct_chg_240
    #重新写入数据库
    df.to_sql('break_new_3', engine, if_exists='replace', index=True)  #增量入库
    print("mission complete!")
    end_time = datetime.datetime.now()
    print('执行时间:%d mins'%(int((end_time - start_time).seconds)/60))