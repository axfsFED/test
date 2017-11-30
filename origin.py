'''
统计每天周换手率超过3%的股票数量及占比
过滤条件：全体A股，中小板，创业板，剔除当日停牌，上市不满一年的；指数剔除停牌的
输入：无（聚宽行情数据）
输出：结果文件（csv）

history
v0.0 制作模板，20171030
v0.1 每日计数的变量用DataFrame存储，20171031
v0.2 上市满一年和满足条件的逻辑关系梳理，代码修改，20171031
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

# 字符串常量，定义输出文件的名称和列字段名称
cols_name = ['szzs', 'count_szzs', 'total_szzs', 'percentage_szzs', \
             'zxbz', 'count_zxbz', 'total_zxbz', 'percentage_zxbz', \
             'cybz', 'count_cybz', 'total_cybz', 'percentage_cybz', \
             'sz50', 'count_sz50', 'total_sz50', 'percentage_sz50', \
             'hs300', 'count_hs300', 'total_hs300', 'percentage_hs300', \
             'zz500', 'count_zz500', 'total_zz500', 'percentage_zz500']

output_filenName = '周均换手率超过3%的股票数量统计.'
'''
000001.XSHG # 上证指数
399005.XSHE # 中小板指
399006.XSHE # 创业板指
000016.XSHG # 上证50
399300.XSHE # 沪深300
000905.XSHG # 中证500
'''
# 目标研究指数
indexs = ['000001.SH','399005.SZ','399006.SZ','000016.SH','399300.SZ', '000905.SH']
cols = ['close', 'count', 'total', 'percentage']
df_daily = pd.DataFrame(index = indexs, data = 0.0, columns = cols)
# 创建的df_daily如下所示
'''
             close  count  total  percentage
000001.XSHG      0      0      0           0
399005.XSHE      0      0      0           0
399006.XSHE      0      0      0           0
000016.XSHG      0      0      0           0
399300.XSHE      0      0      0           0
000905.XSHG      0      0      0           0
'''

# 每天要执行的函数
def daily_run(date):
    list_daily = [] #列表，list_daily存放每天的统计结果
    df_daily[:][:] = 0.0 #每日初始化

    test_turnover = 3 # 换手率%
    countDays = 5 # 均换手率时间长度

    date_str1 = date.strftime('%Y-%m-%d')
    date_str2 = date.strftime('%Y%m%d')
    # 初始化df_daily
    A_list = w.wset("sectorconstituent","date="+date_str1+";sectorid=a001010100000000").Data[1] #当日所有A股成分
    ZXB_list = w.wset("sectorconstituent","date="+date_str1+";sectorid=a001010400000000").Data[1] #中小企业版
    CYB_list = w.wset("sectorconstituent","date="+date_str1+";sectorid=a001010r00000000").Data[1] #创业板
    SZ50_list = w.wset("sectorconstituent","date="+date_str1+";windcode=000016.SH").Data[1] #上证500
    HS300_list = w.wset("sectorconstituent","date="+date_str1+";windcode=000300.SH").Data[1] #沪深300
    ZZ500_list = w.wset("sectorconstituent","date="+date_str1+";windcode=000905.SH").Data[1]#中证500
     
    PAUSED_list = w.wset("tradesuspend","startdate="+date_str1+";enddate="+date_str1).Data[1]#停牌股票
    
    avg_turn_nd_list = w.wss(A_list, "avg_turn_nd","days=-"+str(countDays)+";tradeDate="+date_str2).Data[0]# 获取当天所有股票的5日平均换手率
    ipo_listdays_list = w.wss(A_list, "ipo_listdays","tradeDate="+date_str2).Data[0] # 获取当天所有股票的上市天数（自然日）
    ipo_list_one_year = [True if (ipo_days > 365) else False for ipo_days in ipo_listdays_list] #判断所有股票是否上市满一年
    
    for i in range(0, len(A_list)):
        security = A_list[i]
        is_paused = security in  PAUSED_list#判断当日是否停牌
        if is_paused:
            continue #继续下一只股票
        is_listed_one_year = ipo_list_one_year[i] #判断上市是否满一年

        # 判断单只股票属于哪个板块，哪个指数？
        is_zxb = False
        is_cyb = False
        is_sz50 = False
        is_hs300 = False
        is_zz500 = False
        # flag，该股票是否满足条件
        is_selected = False

        if security in ZXB_list:
            is_zxb = True
        elif security in CYB_list:
            is_cyb = True
        if security in SZ50_list:
            is_sz50 = True
        if security in HS300_list:
            is_hs300 = True
        if security in ZZ500_list:
            is_zz500 = True
        if avg_turn_nd_list[i] > test_turnover:
            is_selected = True

        if is_listed_one_year:
            df_daily.loc[indexs[0]]['total'] += 1
            if is_selected:
                df_daily.loc[indexs[0]]['count'] += 1
            # 统计中小板和创业板的股票总量
            if is_zxb:
                df_daily.loc[indexs[1]]['total'] += 1
                if is_selected:
                    df_daily.loc[indexs[1]]['count'] += 1
            elif is_cyb:
                df_daily.loc[indexs[2]]['total'] += 1
                if is_selected:
                    df_daily.loc[indexs[2]]['count'] += 1
        if is_sz50:
            df_daily.loc[indexs[3]]['total'] += 1
            if is_selected:
                df_daily.loc[indexs[3]]['count'] += 1
        if is_hs300:
            df_daily.loc[indexs[4]]['total'] += 1
            if is_selected:
                df_daily.loc[indexs[4]]['count'] += 1
        if is_zz500:
            df_daily.loc[indexs[5]]['total'] += 1
            if is_selected:
                df_daily.loc[indexs[5]]['count'] += 1

    # 对于每一个指数（行），获取当天的指数收盘价，计算当天的占比
    past_index = w.wss(indexs, "close","tradeDate="+date_str2+";priceAdj=F;cycle=D").Data[0] #获取目标指数的收盘价
    for idx in range(0, len(indexs)):
        if df_daily.iloc[idx]['total'] == 0:
            df_daily.iloc[idx]['percentage'] = 0
        else:
            df_daily.iloc[idx]['percentage'] = round(float(df_daily.iloc[idx]['count'])/df_daily.iloc[idx]['total']+0.001, 2)
        
        df_daily.iloc[idx]['close'] = round(past_index[idx]+0.001, 2)
        #print(list(df_daily.iloc[idx][:]))
        list_daily += list(df_daily.iloc[idx][:])
    return list_daily
    
if __name__ == '__main__':
    engine = create_engine('mysql+pymysql://root:root@localhost:3306/micro?charset=utf8')#用sqlalchemy创建引擎
    # 存放每一天的统计结果
    df_array = []
    #启动WIND
    w.start()
    start_date = '2017-11-28'
    end_date = '2017-11-28'
    
    start_time = datetime.datetime.now()

    trade_days = w.tdays(start_date, end_date, "").Data[0]
    for day in trade_days:
        print(day.strftime('%Y-%m-%d'))
        df_array.append(daily_run(day))
        pass
    w.stop() #关闭WIND接口
    # 将df_array并写入mysql数据库
    df = pd.DataFrame(df_array, index=[day.date() for day in trade_days], columns=cols_name)
    df.to_sql("micro", engine, if_exists='append', index=True)  #增量入库
    print("mission complete!")
    
    end_time = datetime.datetime.now()
    print('执行时间:%d mins'%(int((end_time - start_time).seconds)/60))