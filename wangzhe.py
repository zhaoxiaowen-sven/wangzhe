# coding:utf-8


import os
import sqlite3

import pandas as pd
import simplejson
import xlsxwriter
from pandas import DataFrame
import time

# 日志的记录格式
# device_id           	string
# device_model        	string
# device_version      	string
# event_time          	bigint
# event_date          	timestamp
# start_time          	bigint
# start_date          	timestamp
# end_time            	bigint
# end_date            	times
# day                 	string
# event_id            	string
# event_label         	string
# tamp
# use_number          	string
# duration            	bigint
# save_date           	timestamp
# params              	map<string,string>
# day                 	string
# event_id            	string
# event_label         	string

results = {'imei': [], 'model': [], 'version': [], 'event_date': [],
           'dropFrameTimes': [], 'minFps': [], 'killed': [], 'packageName': [],
           'netLatencyTimes': [], "avgFps": [], "temp": [], "network": [],
           'time': [], 'date': []
           }


# 打开数据库
def openDb():
    return sqlite3.connect("wangze.db")


# 生成各个参数分布的表格
def generate_scope(df, wb_all, sheet_name):

    print(df.shape)
    dropF = df['dropFrameTimes']
    avgFps = df['avgFps']
    minFps = df['minFps']
    temp = df['temp']
    netLatencys = df['netLatencyTimes']

    # 分布的区间
    drop_l = [i for i in range(0, 60, 10)]
    temp_l = [36, 38, 40, 41, 42, 43, 44, 45]
    fps_l = [i for i in range(25, 30, 1)]
    if avgFps.max() > 30:
        fps_l = [i for i in range(30, 60, 5)]
    net_l = [i for i in range(1, 50, 10)]

    results = []
    # 计算分布的一个区间
    results.append(spread_scope(dropF, drop_l))
    results.append(spread_scope(avgFps, fps_l))
    results.append(spread_scope(minFps, fps_l))
    results.append(spread_scope(temp, temp_l))
    results.append(spread_scope(netLatencys, net_l))
    write_to_xlsx(results, wb_all, sheet_name)


def write_to_xlsx(dict_temp, wb, sheet_name):
    t_key = ["dropFrameTimes", "avgFps", "minFps", "temp", "netLatencyTimes"]
    sheet = wb.add_worksheet(sheet_name)
    i = 0
    index = 0
    for value in dict_temp:
        j = 0
        print(t_key[index])
        sheet.write(i, 0, str(t_key[index]))
        for k, v in value.items():
            sheet.write(i+1, j, k)
            sheet.write(i+2, j, v)
            j += 1
        i += 3
        index += 1


# 生成各个参数分布的表格
def generate_scope2(df, name, type):
    # 分布的区间
    # print("generate_scope2")
    wb = pd.ExcelWriter(type + "/" + name + ".xlsx")
    drop_l = [i for i in range(0, 60, 10)]
    temp_l = [36, 38, 40, 41, 42, 43, 44, 45]
    fps_l = [i for i in range(25, 30, 1)]
    # if avgFps.max() > 30:
    #     fps_l = [i for i in range(30, 60, 5)]
    net_l = [i for i in range(1, 50, 10)]
    results = {"dropFrameTimes": {}, "avgFps": {}, "minFps": {}, "temp": {}, "netLatencyTimes": {}}
    t_key = ["dropFrameTimes", "avgFps", "minFps", "temp", "netLatencyTimes"]

    # 不同的计算标准
    for key in df[type].unique():
        tmp = df[df[type] == key]
        dropF = tmp['dropFrameTimes']
        avgFps = tmp['avgFps']
        if avgFps.max() > 30:
            fps_l = [i for i in range(30, 60, 5)]
        minFps = tmp['minFps']
        temp = tmp['temp']
        netLatencys = tmp['netLatencyTimes']
        # 计算分布的一个区间
        results["dropFrameTimes"][key] = spread_scope(dropF, drop_l)
        results["avgFps"][key] = spread_scope(avgFps, fps_l)
        results["minFps"][key] = spread_scope(minFps, fps_l)
        results["temp"][key] = spread_scope(temp, temp_l)
        results["netLatencyTimes"][key] = spread_scope(netLatencys, net_l)
    for k in t_key:
        # print("=====")
        DataFrame(results[k]).to_excel(wb, sheet_name=k)
    wb.close()


# 分布的百分比
def spread_scope(temp_df, l0):
    len2 = len(l0)
    keys = generate_unit(l0)
    values = [0] * (len2 + 1)
    for k0 in temp_df:
        j = 0
        for k1 in l0:
            if k0 < k1:
                values[j] += 1
                break
            if j == len2 - 1:
                values[len2] += 1
            j += 1
    t = dict(zip(keys, values))
    count = sum(t.values()) if sum(t.values()) > 0 else 1
    # print(count)
    for k, v in t.items():
        t[k] = round(float(v / count) * 100, 1)
    return t


# 生成区间段的list
def generate_unit(unit_list):
    unit_list.sort()
    keys = []
    size = len(unit_list)
    i = 0
    while i < size:
        if i == 0:
            key = '<' + str(unit_list[i])
        else:
            key = str(unit_list[i - 1]) + '-' + str(unit_list[i])
        keys.append(key)
        i += 1
    keys.append('>' + str(unit_list[-1]))
    return keys


# 将目标文件保存到数据库中
def parse_file(path):
    with open(path, encoding='utf-8') as f:
        lines = f.readlines()
        for line in lines:
            try:
                arr = line.split("\t")
                # print(arr)
                results.get('imei').append(arr[0])
                results.get('model').append(arr[1])
                results.get('version').append(arr[2])
                results.get('event_date').append(arr[4])
                results.get('date').append(arr[4].split(' ')[0])
                results.get('time').append(arr[4].split(' ')[1])
                data = simplejson.loads(arr[12])
                results.get('dropFrameTimes').append(int(data.get('dropFrameTimes')))
                results.get('minFps').append(int(data.get('minFps')))
                results.get('killed').append(int(data.get('killed')))
                results.get('packageName').append(data.get('packageName'))
                results.get('netLatencyTimes').append(int(data.get('netLatencyTimes')))
                results.get('avgFps').append(int(data.get('avgFps')))
                results.get('temp').append(int(data.get('temp')))
                results.get('network').append(int(data.get('network')))
                # print(results)
            except Exception as e:
                print(e)
    df = DataFrame(results)
    df.to_sql("wangzhe", con=openDb(), index=False, if_exists="append")


def make_report(sql, type):
    p = type + "/"
    make_dir(p)
    wb_all = xlsxwriter.Workbook(type + "/" + type + "report.xlsx")
    writer_to = pd.ExcelWriter(p + type + "record.xlsx")
    df0 = pd.read_sql(sql, con=openDb())
    #  区分网络
    for x in range(2):
        df_network = df0[df0['network'] == x]
        df60 = df_network[df_network['avgFps'] > 30]
        df30 = df_network[df_network['avgFps'] <= 30]
        if x == 1:
            # name = "wifi"
            print("===== wifi =====", type)
            print("wifi_30", df30.shape[0])
            print("wifi_60", df60.shape[0])
            df30.to_excel(writer_to, sheet_name="wifi_30")
            df60.to_excel(writer_to, sheet_name="wifi_60")
            generate_scope_by_type(df30, df60, wb_all, type, "wifi")
        elif x == 0:
            # name = "4G"
            print("===== 4G =====", type)
            print("4G_30", df30.shape[0])
            print("4G_60", df60.shape[0])
            df30.to_excel(writer_to, sheet_name="4G_30")
            df60.to_excel(writer_to, sheet_name="4G_60")
            generate_scope_by_type(df30, df60, wb_all, type, "4G")
        else:
            pass
    wb_all.close()
    writer_to.close()


# 根据类型生成范围
def generate_scope_by_type(df30, df60, wb_all, type, networktype):
    if type == "all":
        generate_scope(df30, wb_all, networktype + "_30_spread")
        generate_scope(df60, wb_all, networktype + "_60_spread")
    else:
        generate_scope2(df30, networktype + "_30_spread", type)
        generate_scope2(df60, networktype + "_60_spread", type)


def make_dir(path):
    if not os.path.exists(path):
        os.makedirs(path)


# 时间对比的模板
def make_time_cmp_report():
    print("make_time_cmp_report ...")
    sql = ''' SELECT date, dropFrameTimes, minFps, avgFps, temp, netLatencyTimes, network
         FROM wangzhe WHERE killed = 0 '''
    make_report(sql, "date")


# 版本对比的模板
def make_version_cmp_report():
    print("make_version_cmp_report ...")
    sql = ''' SELECT version, dropFrameTimes, minFps, avgFps, temp, netLatencyTimes, network
         FROM wangzhe WHERE killed = 0 '''
    make_report(sql, "version")


# 整体对比的模板
def make_all_cmp_report():
    print("make_all_cmp_report ...")
    sql = ''' SELECT dropFrameTimes, minFps, avgFps, temp, netLatencyTimes, network
         FROM wangzhe WHERE killed = 0 '''
    make_report(sql, "all")


if __name__ == "__main__":
    # 解析某个文件并且入库
    # parse_file("data/export_1052_20170817.txt")
    start = time.time()
    print("begin === ", start)
    make_time_cmp_report()
    make_version_cmp_report()
    make_all_cmp_report()
    # pass
    print("end === %.1f s" % (time.time()-start))