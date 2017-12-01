'''
统计2015年以来上市的新股，跌破发行价后5/10/20/30/60日的涨跌幅情况
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

if __name__ == '__main__':
    w.start()
    now = datetime.datetime.now()
    base_date = datetime.datetime(2014, 1, 1)
    date_str = now.strftime("%Y-%m-%d")
    # 1.获取当前股票列表
    target_list = w.wset("sectorconstituent","date="+date_str+";sectorid=a001010100000000").Data[1] #当日标的成分
    # 2.获取每一只股票的首发日
    array = []
    counter = 0
    for security in target_list:
        try:       
            ipo_date = w.wsd(security, "ipo_date", "ED0D", "2017-11-28", "PriceAdj=F").Data[0][0]
            if ipo_date > base_date: #2015年1月1日以来上市的新股
                print(security)
                counter += 1
                ipo_date_str = ipo_date.strftime("%Y-%m-%d")
                begin = w.tdaysoffset(1, ipo_date_str, "").Data[0][0].strftime("%Y-%m-%d")
                trade_days = w.tdays(begin, date_str, "").Data[0]
                _wsd = w.wsd(security, "rel_ipo_chg", begin, date_str, "PriceAdj=B").Data[0]
                for i in range(0,len(_wsd)):
                    list_temp = []
                    if _wsd[i]<0:
                        list_temp.append(security)
                        list_temp.append(trade_days[i])
                        array.append(list_temp)
                        break
        except BaseException:
            print(BaseException)
            continue
        else:
            continue
    df_result = pd.DataFrame(array, index=range(0, len(array)), columns=['code', 'break_date'])
    engine = create_engine('mysql+pymysql://root:root@localhost:3306/micro?charset=utf8')#用sqlalchemy创建引擎
    df_result.to_sql('break_new_1', engine, if_exists='replace', index=True)  #增量入库
    print("新股个数：%d"%(counter))
    print("mission complete!")