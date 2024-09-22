import pyodbc
import time
import sys
import pandas as pd
pd.options.mode.chained_assignment = None
import datetime
import json
import os
#import random
import traceback

import urllib
from sqlalchemy import create_engine

startTime = datetime.datetime.now()
MLog = startTime.strftime('%m')
YLog = startTime.strftime('%Y')


# Select из базы данных
def select_query(query, login_sql, password_sql, server_sql, driver_sql, database, isList=False):
    time.sleep(0.1)  # Спим, чтобы не травмировать базу
    # conn = pyodbc.connect('Driver={' + driver_sql + '};'
    #                       'Server=' + server_sql + ';'
    #                       'Database=' + database + ';'
    #                       'UID=' + login_sql + ';'
    #                       'PWD=' + password_sql + ';'
    #                       'Trusted_Connection=no;')
    # df = pd.read_sql_query(query, conn)
    # conn.close()

    params = urllib.parse.quote_plus("DRIVER={" + driver_sql + "};"
                                     "SERVER=" + server_sql + ";"
                                     "DATABASE=" + database + ";"
                                     "UID=" + login_sql + ";"
                                     "PWD=" + password_sql + ";")
    engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)

    df = pd.read_sql_query(query, engine)

    if isList == True:
        list_df = df.values
        flat_list = [item for sublist in list_df for item in sublist]
        return flat_list  # Возвращаем лист
    else:
        return df  # Возвращаем Dataframe pandas

# Достаем лист, разделенный запятыми, из .txt файлов
def get_list_from_txt(txt_file, enc='utf-8'):
    try:
        with open(txt_file, encoding=enc) as f:
            t = f.read().split(',')
            list_fin = []
            for t0 in t:
                #t1 = t0.replace(' ', '').replace('\ufeff','')
                t1 = t0.replace(', ', '').replace('\ufeff', '')
                list_fin.append(t1)
    except:
        print('Сбор данных для доступа провалился.')
        traceback.print_exc()
        time.sleep(60)
        sys.exit()
    return list_fin

# Получение настроек
def GetSettings ():
    with open("Settings.json", "r") as read_file:
        data = json.load(read_file)
    return data

# Создание дирректорий для логов
def CreateLogDir ():
    if os.path.exists('./src/Logs/' + str(YLog)) == False:
        if os.path.exists('./src/Logs/') == False:
            if os.path.exists('./src/') == False:
                os.mkdir('./src/')
            os.mkdir('./src/Logs/')
        os.mkdir('./src/Logs/' + str(YLog))

# Запись в лог
def InsertLog (StrLog, NoPrint = 0):
    if NoPrint == 0:
        print(StrLog)
    startTime = datetime.datetime.now()
    t_log = startTime.strftime('%d.%m.%Y %H:%M:%S')
    log = open('./src/Logs/' + str(YLog) + '/Logs_' + str(MLog) + '.txt', 'a+', encoding="utf-8")
    log.write(t_log + '  # ' + StrLog + '\n')
    log.close()
