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

# 目标研究指数
dict_index = {'szzs': '000001.SH', 'zxbz': '399005.SZ', 'cybz': '399006.SZ', 'sz50':'000016.SH', 'hs300':'399300.SZ', 'zz500':'000905.SH'}
# 设置需要用到的参数变量
long_period = 250
short_period = 20
threshold = 0.01
test_turnover = 3 # 换手率%
countDays = 5 # 均换手率时间长度

# 输入：起止日期，以及统计的标的
def micro_statistics(start_date, end_date, target_index):
    '''
    count_1：20日均线距离250日均线小于1%股票数量
    count_2：周均换手率大于3%的股票数量
    '''
    # 字符串常量，定义统计的输出字段
    cols_name = ['总数', 'MA20/MA250>'+str(1-threshold), 'percentage_1(%)', '周均换手率(>'+str(test_turnover)+'%)', 'percentage_2(%)']

    list_all = [] #列表，存放所有的统计结果
    ipo_one_year_jduge = False
    if target_index == '000001.SH':
        target =  "a001010100000000"
        ipo_one_year_jduge = True
    elif target_index == '399005.SZ':
        target =  "a001010400000000"
        ipo_one_year_jduge = True
    elif target_index == '399006.SZ':
        target =  "a001010r00000000"
        ipo_one_year_jduge = True
    else: target =  target_index
        
    trade_days = w.tdays(start_date, end_date, "").Data[0]
    
    for date in trade_days:#每日统计      
        list_daily = [] #列表，list_daily存放每天的统计结果
        date_str1 = date.strftime('%Y-%m-%d')
        date_str2 = date.strftime('%Y%m%d')
        print("%s: %s"%(target_index,date_str1))
        if ipo_one_year_jduge: #板块成分
            target_list = w.wset("sectorconstituent","date="+date_str1+";sectorid="+target).Data[1] #当日标的成分
        else: #指数成分
            target_list = w.wset("sectorconstituent","date="+date_str1+";windcode="+target).Data[1] #上证500
        paused_list = w.wset("tradesuspend","startdate="+date_str1+";enddate="+date_str1).Data[1]#当日停牌股票
        
        avg_turn_nd_list = w.wss(target_list, "avg_turn_nd","days=-"+str(countDays)+";tradeDate="+date_str2).Data[0]# 获取当天标的成分的5日平均换手率
        MA_long = w.wss(target_list, "MA","tradeDate="+date_str2+";MA_N="+str(long_period)+";priceAdj=F;cycle=D").Data[0]
        MA_short = w.wss(target_list, "MA","tradeDate="+date_str2+";MA_N="+str(short_period)+";priceAdj=F;cycle=D").Data[0]
        
        ipo_list_one_year = [True for security in target_list]
        if ipo_one_year_jduge:
            ipo_listdays_list = w.wss(target_list, "ipo_listdays","tradeDate="+date_str2).Data[0] # 获取当天标的成分的上市天数（自然日）
            ipo_list_one_year = [True if (ipo_days > 365) else False for ipo_days in ipo_listdays_list] #判断标的成分是否上市满一年
        #各个指标的统计计数
        count_total = 0
        count_1 = 0
        count_2 = 0
        for i in range(0, len(target_list)):
            security = target_list[i]
            if security in  paused_list:
                continue #继续下一只股票
            is_listed_one_year = ipo_list_one_year[i] #判断上市是否满一年
            if not is_listed_one_year:
                continue
            count_total += 1
            if MA_short[i]/MA_long[i] >= 1-threshold:
                count_1 += 1
            if avg_turn_nd_list[i] >= test_turnover:
                count_2 += 1
        list_daily.append(count_total)
        list_daily.append(count_1)
        list_daily.append(round(count_1/count_total+0.00001, 4)*100)
        list_daily.append(count_2)
        list_daily.append(round(count_2/count_total+0.00001, 4)*100)
        list_all.append(list_daily)
    # 将list_all封装成dataframe
    df_result = pd.DataFrame(list_all, index=[day.date() for day in trade_days], columns=cols_name)
    # 对于目标指数，获取区间的指数收盘价
    _close = w.wsd(target_index, "close", start_date, end_date, "PriceAdj=F").Data[0]
    df_result.insert(0, target_index, _close) #将收盘价插入
    return df_result


if __name__ == '__main__':
    start_time = datetime.datetime.now()
    #启动WIND
    w.start()
    start_date = '2000-01-01'
    end_date = '2017-11-28'
    
    idx = 'szzs'
    df = micro_statistics(start_date, end_date, dict_index[idx])
    w.stop() #关闭WIND接口
    #将结果封装成df_array并写入mysql数据库
    engine = create_engine('mysql+pymysql://root:root@localhost:3306/micro?charset=utf8')#用sqlalchemy创建引擎
    df.to_sql(idx, engine, if_exists='replace', index=True)  #增量入库
    print("mission complete!")
    
    end_time = datetime.datetime.now()
    print('执行时间:%d mins'%(int((end_time - start_time).seconds)/60))