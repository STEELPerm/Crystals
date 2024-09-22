#import requests
import base64
import os
import sys
import datetime, time
import traceback
import shutil
import pandas as pd
#import xml.etree.ElementTree as ET

import xmltodict
#from flatten_json import flatten

import urllib
from sqlalchemy import create_engine
import json
from zeep import Client

import math


import api_utils


# INFO
"""
    Адрес развернутого сервера 'http://192.168.187.4:8090'
    
    ИМПОРТ данных из Crystals.
    Из описания: https://crystals.atlassian.net/wiki/spaces/INT/pages/1646806/SetRetail10+ERP+-+SetRetail10
    Методы веб-сервиса для экспорта чеков:         
    *** За заданный операционный день (getPurchasesByOperDay)
    Возвращаемый результат не содержит полные данные по бонусам и скидкам.
    Для полного просмотра бонусов и скидок используйте метод getFullPurchasesByParams.

    dateOperDay - date - Операционный день в формате YYYY-MM-DD
    
    # url_c = url_srv + '/SET-ERPIntegration/FiscalInfoExport?wsdl'
    # client = Client(url_c)
    # result = client.service.getPurchasesByOperDay('2024-09-03')
    # #print(result)
    # #print(result.decode("utf-8"))
    # file_xml = result.decode("utf-8")

    *** За заданный операционный день c вводом параметров (getPurchasesByOperDay)
    Возвращаемый результат не содержит полные данные по бонусам и скидкам.
    Для полного просмотра бонусов и скидок используйте метод getFullPurchasesByParams.

    Year -  integer - Год в формате YYYY

    Mobth - string
    Параметр mobth для вызова установки значений месяца должен использоваться именно в таком написании.
    Его наименование не совпадает со словом месяц (month) на английском языке!
    Месяц в текстовом формате:
    JANUARY
    FEBRUARY
    MARCH
    APRIL
    MAY
    JUNE
    JULY
    AUGUST
    SEPTEMBER
    OCTOBER
    NOVEMBER
    DECEMBE

    # url_c = url_srv + '/SET-ERPIntegration/FiscalInfoExport?wsdl'
    # client = Client(url_c)
    # result = client.service.getPurchasesByOperDay('2024')

    *** За заданный период (getPurchasesByPeriod)
    Возвращаемый результат не содержит полные данные по бонусам и скидкам.
    Для полного просмотра бонусов и скидок используйте метод getFullPurchasesByParams.

    fromDate - Начало диапазона в формате YYYY-MM-DD
    toDate - Конец диапазона в формате YYYY-MM-DD

    # url_c = url_srv + '/SET-ERPIntegration/FiscalInfoExport?wsdl'
    # client = Client(url_c)
    # result = client.service.getPurchasesByPeriod('2024-01-01', '2024-09-02')

    *** За заданный период по товару (getPurchasesByPeriodAndProduct)
    Возвращаемый результат не содержит полные данные по бонусам и скидкам.
    Для полного просмотра бонусов и скидок используйте метод getFullPurchasesByParams.

    fromDate - date - Начало диапазона в формате YYYY-MM-DD
    toDate - date -Конец диапазона в формате YYYY-MM-DD
    goodsCode - string - Код товара
    
    # url_c = url_srv + '/SET-ERPIntegration/FiscalInfoExport?wsdl'
    # client = Client(url_c)
    # result = client.service.getPurchasesByPeriodAndProduct('2024-01-01', '2024-09-02', '786')

    *** По заданным параметрам (getPurchasesByParams)
    Возвращаемый результат не содержит полные данные по бонусам и скидкам.
    Для полного просмотра бонусов и скидок используйте метод getFullPurchasesByParams.

    dateOperDay - date - Операционный день в формате YYYY-MM-DD
    shopNumber - integer - Номер магазина
    cashNumber - integer - Номер кассы
    shiftNumber - integer - Номер смены
    purchaseNumber - integer - Номер чека

    Параметры shopNumber, cashNumber, shiftNumber, purchaseNumber – являются необязательными.
    В зависимости от полноты указания параметров, в ответе будет возвращаться соответствующее количество чеков.

    Кейсы:
    dateOperDay - в отчёт попадают все чеки всех магазинов за операционный день dateOperDay.
    dateOperDay, shopNumber - в отчёт попадают все чеки за операционный день dateOperDay с магазина shopNumber.
    dateOperDay, shopNumber, cashNumber - в отчёт попадают все чеки за операционный день dateOperDay с магазина shopNumber с кассы cashNumber.
    dateOperDay, shopNumber, cashNumber, shiftNumber - в отчёт попадают все чеки смены shiftNumber за операционный день dateOperDay с магазина shopNumber с кассы cashNumber.
    dateOperDay, shopNumber, cashNumber, shiftNumber, purchaseNumber - в отчёт попадает только один конкретный чек под номером purchaseNumber из сменыshiftNumber за операционный день dateOperDay с магазина shopNumber с кассы cashNumber.

    # url_c = url_srv + '/SET-ERPIntegration/FiscalInfoExport?wsdl'
    # client = Client(url_c)
    # result = client.service.getPurchasesByParams('2024-01-01T15:00:00', 29591)

    *** Экспорт новых, не отправленных чеков за операционный день с вводом параметров (getNewPurchasesByParams)
    Выгружаются все новые чеки за указанный операционный день, либо удовлетворяющие заданным параметрам, если они указаны.

    dateOperDay - date - Операционный день в формате YYYY-MM-DD
    shopNumber - integer - Номер магазина
    cashNumber - integer - Номер кассы
    shiftNumber - integer - Номер смены
    purchaseNumber - integer - Номер чека

    # url_c = url_srv + '/SET-ERPIntegration/FiscalInfoExport?wsdl'
    # client = Client(url_c)
    # result = client.service.getNewPurchasesByParams('2024-01-01T15:00:00', 1)

    *** Экспорт новых, не отправленных чеков (getNewPurchasesByOperDay) --- !!!! Не тестировал
    В отчёте выгружаются только новые чеки (те которые ещё не забирали).
    Выгружаются все новые чеки за указанный операционный день, либо удовлетворяющие заданным параметрам, если они указаны.

    arrayOfParams - array - Массив параметров по следующему формату:
    [OperDay (DateTime, REQUIRED), shop(Long), cash(Long), shift(Long), number(Long)]

    # url_c = url_srv + '/SET-ERPIntegration/FiscalInfoExport?wsdl'
    # param = {'2024-01-01T12:00:00.000',
    #          1,
    #          1,
    #          1,
    #          1
    #          }
    # client = Client(url_c)
    # result = client.service.getNewPurchasesByOperDay(param)
    # ERROR: zeep.exceptions.Fault: java.lang.ClassCastException: com.sun.org.apache.xerces.internal.dom.ElementNSImpl cannot be cast to javax.xml.datatype.XMLGregorianCalendar

    *** Получение новых чеков, которые не отправлялись веб-сервисом (getNewFullPurchasesByOperDay) --- !!!! Не тестировал
    * Минимальный размер массива аргументов метода - 1 (потому что параметр "дата опердня" обязательный).
    * Если требуется пропустить, параметр "номер смены", тогда установите значение null, потому что за номером смены следует номер чека.
    * Если требуется номер чека, массиву допустимо быть длиной 4, потому что за параметром "номер чека" ничего не следует.

    0 - Date - Дата, за которую из операционного дня требуется получить новые чеки
    1 - Long - Номер магазина, от которого из операционного дня следует выбрать новые чеки
    2 - Long - Номер кассы, от которой из операционного дня следует выбрать новые чеки
    3 - Long - Номер смены, от которой из операционного дня следует выбрать новые чеки
    4 - Long - Номер чека, от которой из операционного дня следует выбрать новые чеки

    # url_c = url_srv + '/SET-ERPIntegration/FiscalInfoExport?wsdl'
    # client = Client(url_c)
    # result = client.service.getNewFullPurchasesByOperDay(['2024-01-01T12:00:00.000'])
    # # ERROR: zeep.exceptions.Fault: java.lang.ClassCastException: com.sun.org.apache.xerces.internal.dom.ElementNSImpl cannot be cast to javax.xml.datatype.XMLGregorianCalendar



    *** ПРОВЕРКА СХЕМ
    # import xmlschema
    # data_schema = xmlschema.XMLSchema('goods-catalog-schema.xsd')
    # data=data_schema.to_dict('goods-catalog.xml')
    # print(data_schema)
        
    # # 2 сопоставить xml схеме
    # import xmlschema
    # import json
    # from xml.etree.ElementTree import ElementTree
    # #my_xsd = '<?xml version="1.0"?> <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"> <xs:element name="note" type="xs:string"/> </xs:schema>'
    # schema = xmlschema.XMLSchema('goods-catalog-schema.xsd')
    # data = json.dumps({'note': 'this is a Note text'})
    # xml = xmlschema.from_json(data, schema=schema, preserve_root=True)
    # ElementTree(xml).write('goods-catalog.xml')    
    
    # # просмотр объекта схема
    # from xmlschema import XMLSchema
    # obj = XMLSchema.meta_schema.decode('goods-catalog-schema.xsd')
    # print(obj)

"""


# Создать словарь из xml
def xml_to_dict(element):
    result = {}
    for child in element:
        if len(child) > 0:
            value = xml_to_dict(child)
        else:
            value = child.text

        key = child.tag
        if key in result:
            if not isinstance(result[key], list):
                result[key] = [result[key]]
            result[key].append(value)
        else:
            result[key] = value

    return result


# OUT. Отправить файлы из расшаренной папки \\piter-sql\Export\Crystal в Crystals (getGoodsCatalog)
def send_files_to_crystals():
    # Отправляемые файлы должны находиться в корневой папке программы, в папке OutFiles
    dir = './OutFiles'
    if not os.path.exists(dir):
        api_utils.InsertLog('Ошибка, не обнаружена папка OutFiles')
        sys.exit()

    # Получение файлов из расшаренной папки \\piter-sql\Export\Crystal, куда выгружаются xml из базы
    files = (file for file in os.listdir(shared_folder) if os.path.isfile(os.path.join(shared_folder, file)))
    files_count = len(list(files))
    api_utils.InsertLog('Всего файлов в папке \\\piter-sql\Export\Crystal: ' + str(files_count))

    if files_count > 0:
        api_utils.InsertLog('Запуск перемещения xml файлов из расшаренной папки ' + shared_folder + ' в папку с программой')
        f = 0
        files = (file for file in os.listdir(shared_folder) if
                 os.path.isfile(os.path.join(shared_folder, file)))
        for file in files:
            if file.endswith('.xml'):
                file_from = os.path.join(shared_folder, file)
                file_to = dir + '/' + file
                if os.path.isfile(file_from):
                    shutil.move(file_from, file_to)
                    f += 1
        api_utils.InsertLog('Успешно. Файлов перемещено: ' + str(f))

    # Создание папки для хранения успешно отправленных файлов (архив). Например: Output\23072024
    dpath = datetime.datetime.now().strftime('%d%m%Y')
    if os.path.exists(dir + '/Output/' + dpath) == False:
        if os.path.exists(dir + '/Output/') == False:
            os.mkdir(dir + '/Output/')
        os.mkdir(dir + '/Output/' + dpath)

    # Отправить файлы в Crystals. Взять только файлы xml
    allfiles = (file for file in os.listdir(dir) if os.path.isfile(os.path.join(dir, file)))
    for file in allfiles:
        if file.endswith('.xml'):
            print(file)
            file_name = file
            file = dir + '/' + file
            print(file)
            error = 0

            # Отправка файла (getGoodsCatalog)
            # url = 'http://192.168.187.4:8090/SET-ERPIntegration/SET/WSGoodsCatalogImport'
            # url = 'http://192.168.187.4:8090/SET-ERPIntegration/SET/WSGoodsCatalogImport?wsdl'
            url = url_srv + "/SET-ERPIntegration/SET/WSGoodsCatalogImport?wsdl"

            # Открыть XML и перекодировать в base64
            with open(file, "rb") as file:
                file64 = base64.encodebytes(file.read()).decode("utf-8")

            #  ИМПОРТ данных в SetRetail10
            # Отправить файл на сервис (через zeep): True
            try:
                client = Client(url)
                result = client.service.getGoodsCatalog(file64)
                print(result)
                api_utils.InsertLog('Выгрузили ' + file_name + '. Ответ: ' + str(result))
            except:
                traceback.print_exc()
                error = 1
                startTime = datetime.datetime.now()
                t_log = startTime.strftime('%d.%m.%Y %H:%M:%S')
                log = open('./src/Logs/' + str(YLog) + '/Logs_' + str(MLog) + '.txt', 'a+', encoding="utf-8")
                log.write(t_log + ' ## Ошибка при отправке файла\n')
                traceback.print_exc(file=log)
                log.close()

            # Переместить успешно отправленный файл в архив.
            if error == 0:
                file_path = os.path.join(dir, file_name)
                if os.path.isfile(file_path):
                    os.replace(dir + '/' + file_name, dir + '/Output/' + dpath + '/' + file_name)

            # Отправка через SOAP
            # # СОЗДАТЬ SOAP
            # data = """<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:plug="http://plugins.products.ERPIntegration.crystals.ru/">
            # <soapenv:Header/>
            # <soapenv:Body>
            #  <plug:getGoodsCatalog>
            #  <goodsCatalogXML>""" + file64 + """</goodsCatalogXML>
            #  </plug:getGoodsCatalog>
            # </soapenv:Body>
            # </soapenv:Envelope>"""
            # print(data)
            # # Отправить SOAP
            # r = requests.post(url, data=data, headers=headers)
            # print(r)
            # print(r.text)


# IN. Получить продажи за период
def getPurchasesByPeriod(url_srv, date_begin, date_end):
    # Получить данные за заданный период (getPurchasesByPeriod)
    url_c = url_srv + '/SET-ERPIntegration/FiscalInfoExport?wsdl'
    client = Client(url_c)
    result = client.service.getPurchasesByPeriod(date_begin, date_end)
    # result = client.service.getPurchasesByPeriod('2024-09-03', '2024-09-03T23:59:00.000')
    file_xml = result.decode("utf-8")

    # Отдельно цены и позиции
    # df_purchase = pd.read_xml(file_xml, xpath="/purchases/purchase")
    # df_position = pd.read_xml(file_xml, xpath="/purchases/purchase/positions/position") # xpath='.//position'
    # df_purchase.to_excel("in_purchase.xlsx")
    # df_position.to_excel("in_position.xlsx")

    # Чеки. Преобразование XML в словарь  +++++
    dict_data = xmltodict.parse(file_xml)

    # Количество чеков
    perchases_count = dict_data.get('purchases')["@count"]

    api_utils.InsertLog('Всего продаж: ' + str(perchases_count))

    if perchases_count == '0':
        return

    # Преобразование словаря в JSON
    json_data = json.loads(json.dumps(dict_data, indent=4))
    print(json_data['purchases']['purchase'])
    df_purchase = pd.DataFrame(json_data['purchases']['purchase'])

    # Чтобы забрать ФИО кассира
    df_purchase['CASHIER_NAME'] = None

    # Оплаты в одной таблице (чтобы делать вставку в базу 1 раз)
    # df_payment_all = pd.read_xml(file_xml, xpath='(.//payments/*)')

    df_payment_all = pd.DataFrame(columns=['order', 'typeClass', 'amount', 'description',  # 4
                                           'pay.frcode', 'code', 'bank.id', 'ref.number', 'card.type', 'bank.type',
                                           'card.number',  # 11
                                           'terminal.number', 'cash.transaction.date', 'subclass', 'merchant.id'])  # 15

    # Позиции со всех чеков в одной таблице (чтобы делать вставку в базу 1 раз)
    # Сначала загрузим всё из xml (чтобы создались колонки)
    # А потом ниже удалим пустые (df_position_all.dropna(subset=['cash']))
    df_position_all = pd.read_xml(file_xml, xpath="/purchases/purchase/positions/position")

    # Колонки для позиций
    col_position = ['@order', '@departNumber', '@goodsCode', '@barCode',
                    '@count', '@cost', '@nds', '@ndsSum', '@discountValue', '@costWithDiscount',
                    '@amount', '@dateCommit', '@insertType'
                    ]

    # Дополнительные колонки для оплат
    add_column_payment = ['pay.frcode', 'code', 'bank.id', 'ref.number', 'card.type', 'bank.type', 'card.number',  # 7
                          'terminal.number', 'cash.transaction.date', 'subclass', 'merchant.id']  # 11
    df_payment_all[add_column_payment] = None, None, None, None, None, None, None, None, None, None, None  # 11

    # Соберём данные чеков, позиций и оплат в разные df
    for i, row in df_purchase.iterrows():
        # print(f"Index: {i}")
        # print(f"{row}\n")

        # Взять идентификаторы смены
        shop = df_purchase.iloc[i]['@shop']
        cash = df_purchase.iloc[i]['@cash']
        shift = df_purchase.iloc[i]['@shift']
        number = df_purchase.iloc[i]['@number']
        fiscalnum = df_purchase.iloc[i]['@fiscalnum']
        print(shop, cash, shift, number, fiscalnum)

        # Свойства - взять ФИО кассира
        CASHIER_NAME = None
        try:
            json_property = json.loads(json.dumps(row['plugin-property'], indent=4))
            df_property = pd.DataFrame(json_property)
            idx = df_property[df_property['@key'] == 'CASHIER_NAME'].index
            CASHIER_NAME = df_property.iloc[idx]['@value']
            df_purchase['CASHIER_NAME'].iloc[i] = CASHIER_NAME.at[idx[0]]
        except:
            pass

        # Позиции
        json_positions = json.loads(json.dumps(row['positions'], indent=4))
        df_positions = pd.DataFrame(json_positions['position'])
        # !!! Взять только определённые колонки. Поле plugin-property не нужно, т.к. бывает, что оно размножает строки
        df_positions = df_positions.loc[:, df_positions.columns.intersection(col_position)]

        df_positions['shop'] = shop
        df_positions['cash'] = cash
        df_positions['shift'] = shift
        df_positions['number'] = number
        df_positions['fiscalnum'] = fiscalnum
        # !!! Убрать дубликаты
        df_positions = df_positions.drop_duplicates()

        # Переименовать колонки у позиций
        df_positions.rename(columns={'@order': 'order', '@departNumber': 'departNumber',
                                     '@goodsCode': 'goodsCode', '@barCode': 'barCode',
                                     '@count': 'count', '@cost': 'cost', '@nds': 'nds', '@ndsSum': 'ndsSum',
                                     '@discountValue': 'discountValue',
                                     '@costWithDiscount': 'costWithDiscount',
                                     '@amount': 'amount', '@dateCommit': 'dateCommit',
                                     '@insertType': 'insertType'},
                            inplace=True)

        # Собрать в один df (чтобы потом 1 раз делать вставку в базу)
        df_position_all = pd.concat([df_position_all, df_positions])

        # Оплаты
        # json_payment = json.loads(json.dumps(row['payments'], indent=4))
        # df_payment = pd.DataFrame(json_payment['payment'])
        json_payment = json.loads(json.dumps(row['payments']['payment'], indent=4))
        df_payment = pd.DataFrame(json_payment)

        # Забрать свойства plugin-property, чтобы дальше их добавить колонками
        df_payment_plugin_property = df_payment['plugin-property']

        # Взять только первое значение, т.к. plugin-property идёт строками в payment и размножает записи в df
        df_payment = df_payment.groupby('@order').nth(0)  # .reset_index()


        df_payment['shop'] = shop
        df_payment['cash'] = cash
        df_payment['shift'] = shift
        df_payment['number'] = number
        df_payment['fiscalnum'] = fiscalnum

        # Забираем данные из дополнительных свойств (plugin-property) и записываем в нужные колонки df
        for j in df_payment_plugin_property:
            if isinstance(j, dict):
                print('key=' + j['@key'], '; value=' + str(j['@value']))
                if j['@key'] in add_column_payment:
                    print(j['@key'] + '  ***====***', j['@value'])
                    df_payment[j['@key']] = j['@value']

        del df_payment['plugin-property']

        # Переименовать колонки у оплат
        df_payment.rename(columns={'@order': 'order', '@typeClass': 'typeClass',
                                   '@amount': 'amount', '@description': 'description'},
                          inplace=True)

        # Собрать оплаты в один df (чтобы потом 1 раз делать вставку в базу)
        df_payment_all = pd.concat([df_payment_all, df_payment])

    # Чеки. Определить необходимые колонки
    col_Purchase = ['@tabNumber', '@userName', '@operationType', '@cashOperation',
                    '@operDay', '@shop', '@cash', '@shift', '@number', '@saletime', '@begintime',
                    '@amount', '@discountAmount', '@inn', '@qrcode',
                    '@fiscalDocNum', '@factorynum', '@fiscalnum', 'CASHIER_NAME'
                    ]

    # !!! Взять только определённые колонки.
    df_purchase = df_purchase.loc[:, df_purchase.columns.intersection(col_Purchase)]

    # Чеки. Переименвоать колонки
    df_purchase.rename(columns={'@tabNumber': 'tabNumber', '@userName': 'userName',
                                '@operationType': 'operationType', '@cashOperation': 'cashOperation',
                                '@operDay': 'operDay', '@shop': 'shop', '@cash': 'cash', '@shift': 'shift',
                                '@number': 'number', '@saletime': 'saletime', '@begintime': 'begintime',
                                '@amount': 'amount', '@discountAmount': 'discountAmount',
                                '@inn': 'inn', '@qrcode': 'qrcode', '@fiscalDocNum': 'fiscalDocNum',
                                '@factorynum': 'factorynum', '@fiscalnum': 'fiscalnum'},
                       inplace=True)

    # df_purchase.to_excel("in_df_purchase_NEW.xlsx")
    # sys.exit()

    # Позиции чеков. Удалить из позиций пустые строки, т.к было объединение
    df_position_all = df_position_all.dropna(subset=['cash'])
    # Удалить ненужные колонки (чтобы вставка в базу прошла корректно)
    del df_position_all['plugin-property']

    # Оплаты. Удалить из оплат пустые строки, т.к было объединение
    df_payment_all = df_payment_all.dropna(subset=['cash'])
    # df_payment_all['plugin-property'] = df_payment_all['plugin-property'].astype(str)

    # print(df_purchase)
    # print(df_position_all)
    # print(df_payment_all)
    # sys.exit()

    # Убрать из даты +03:00 (2024-09-03+03:00). Взять первые 10 символов из строки с датой
    df_purchase["operDay"] = df_purchase['operDay'].str[:10]

    # Вставка чеков
    params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                     "SERVER=" + server_sql + ";"
                                     "DATABASE=" + database + ";"
                                     "UID=" + login_sql + ";"
                                     "PWD=" + password_sql + ";")
    engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
    df_purchase.to_sql("Crystals_Purchase", engine, index=False, chunksize=10000, if_exists="append")

    # # Позиции. Удалить из позиций пустые строки, т.к было объединение
    # df_position_all = df_position_all.dropna(subset=['cash'])
    #
    # # Удалить ненужные колонки (чтобы вставка в базу прошла корректно)
    # del df_position_all['plugin-property']

    # Вставка позиций чеков
    params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                     "SERVER=" + server_sql + ";"
                                     "DATABASE=" + database + ";"
                                     "UID=" + login_sql + ";"
                                     "PWD=" + password_sql + ";")
    engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
    df_position_all.to_sql("Crystals_Purchase_Position", engine, index=False, chunksize=10000,
                           if_exists="append")

    # # Оплаты. Удалить из оплат пустые строки, т.к было объединение
    # df_payment_all = df_payment_all.dropna(subset=['cash'])
    # df_payment_all['plugin-property'] = df_payment_all['plugin-property'].astype(str)


    # Вставка оплат
    params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                     "SERVER=" + server_sql + ";"
                                     "DATABASE=" + database + ";"
                                     "UID=" + login_sql + ";"
                                     "PWD=" + password_sql + ";")
    engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
    df_payment_all.to_sql("Crystals_Purchase_Payment", engine, index=False, chunksize=10000,
                          if_exists="append")


# IN. Получить продажи за операционный день
def getPurchasesByOperDay(url_srv, operday):
    # Получить данные за операционный день (getPurchasesByPeriod) и сверить с базой (все ли чеки есть)
    url_c = url_srv + '/SET-ERPIntegration/FiscalInfoExport?wsdl'
    client = Client(url_c)
    result = client.service.getPurchasesByOperDay(operday)
    #print(result)
    file_xml = result.decode("utf-8")
    #print(file_xml)

    # ??? Можно использовать с подобъектом: По заданным параметрам (getPurchasesByParams)
    # url_c = url_srv + '/SET-ERPIntegration/FiscalInfoExport?wsdl'
    # client = Client(url_c)
    # result = client.service.getPurchasesByParams('2024-09-03', 29591)
    # file_xml = result.decode("utf-8")

    # Чеки. Преобразование XML в словарь
    dict_data = xmltodict.parse(file_xml)

    # Количество чеков
    perchases_count = dict_data.get('purchases')["@count"]

    api_utils.InsertLog('Всего продаж: ' + str(perchases_count))

    if perchases_count == '0':
        return

    # Преобразование словаря в JSON
    json_data = json.loads(json.dumps(dict_data, indent=4))
    print(json_data['purchases']['purchase'])
    df_purchase = pd.DataFrame(json_data['purchases']['purchase'])

    if df_purchase.empty:
        return

    # Чтобы забрать ФИО кассира
    df_purchase['CASHIER_NAME'] = None

    # Оплаты в одной таблице (чтобы делать вставку в базу 1 раз)
    # df_payment_all = pd.read_xml(file_xml, xpath='(.//payments/*)')

    df_payment_all = pd.DataFrame(columns=['order', 'typeClass', 'amount', 'description',  # 4
                                           'pay.frcode', 'code', 'bank.id', 'ref.number', 'card.type', 'bank.type',
                                           'card.number',  # 11
                                           'terminal.number', 'cash.transaction.date', 'subclass', 'merchant.id'])  # 15

    # Позиции со всех чеков в одной таблице (чтобы делать вставку в базу 1 раз)
    # Сначала загрузим всё из xml (чтобы создались колонки)
    # А потом ниже удалим пустые (df_position_all.dropna(subset=['cash']))
    df_position_all = pd.read_xml(file_xml, xpath="/purchases/purchase/positions/position")

    # Колонки для позиций
    col_position = ['@order', '@departNumber', '@goodsCode', '@barCode',
                    '@count', '@cost', '@nds', '@ndsSum', '@discountValue', '@costWithDiscount',
                    '@amount', '@dateCommit', '@insertType'
                    ]

    # Дополнительные колонки для оплат
    add_column_payment = ['pay.frcode', 'code', 'bank.id', 'ref.number', 'card.type', 'bank.type', 'card.number',  # 7
                          'terminal.number', 'cash.transaction.date', 'subclass', 'merchant.id']  # 11
    df_payment_all[add_column_payment] = None, None, None, None, None, None, None, None, None, None, None  # 11

    # Соберём данные чеков, позиций и оплат в разные df
    for i, row in df_purchase.iterrows():
        # print(f"Index: {i}")
        # print(f"{row}\n")

        # Взять идентификаторы смены
        shop = df_purchase.iloc[i]['@shop']
        cash = df_purchase.iloc[i]['@cash']
        shift = df_purchase.iloc[i]['@shift']
        number = df_purchase.iloc[i]['@number']
        fiscalnum = df_purchase.iloc[i]['@fiscalnum']
        print(shop, cash, shift, number, fiscalnum)

        # Свойства - взять ФИО кассира
        CASHIER_NAME = None
        try:
            json_property = json.loads(json.dumps(row['plugin-property'], indent=4))
            df_property = pd.DataFrame(json_property)
            idx = df_property[df_property['@key'] == 'CASHIER_NAME'].index
            CASHIER_NAME = df_property.iloc[idx]['@value']
            df_purchase['CASHIER_NAME'].iloc[i] = CASHIER_NAME.at[idx[0]]
        except:
            pass

        # Позиции
        json_positions = json.loads(json.dumps(row['positions'], indent=4))
        df_positions = pd.DataFrame(json_positions['position'])
        # !!! Взять только определённые колонки. Поле plugin-property не нужно, т.к. бывает, что оно размножает строки
        df_positions = df_positions.loc[:, df_positions.columns.intersection(col_position)]

        df_positions['shop'] = shop
        df_positions['cash'] = cash
        df_positions['shift'] = shift
        df_positions['number'] = number
        df_positions['fiscalnum'] = fiscalnum
        # !!! Убрать дубликаты
        df_positions = df_positions.drop_duplicates()

        # Переименвоать колонки у позиций
        df_positions.rename(columns={'@order': 'order', '@departNumber': 'departNumber',
                                     '@goodsCode': 'goodsCode', '@barCode': 'barCode',
                                     '@count': 'count', '@cost': 'cost', '@nds': 'nds', '@ndsSum': 'ndsSum',
                                     '@discountValue': 'discountValue',
                                     '@costWithDiscount': 'costWithDiscount',
                                     '@amount': 'amount', '@dateCommit': 'dateCommit',
                                     '@insertType': 'insertType'},
                            inplace=True)

        # Собрать в один df (чтобы потом 1 раз делать вставку в базу)
        df_position_all = pd.concat([df_position_all, df_positions])

        print('row_payments**********', row['payments']['payment'])
        # Оплаты
        # json_payment = json.loads(json.dumps(row['payments'], indent=4))
        # print('json_payment*******', json_payment)
        # df_payment = pd.DataFrame(json_payment['payment'])

        json_payment = json.loads(json.dumps(row['payments']['payment'], indent=4))
        df_payment = pd.DataFrame(json_payment)

        # Забрать свойства plugin-property, чтобы дальше их добавить колонками
        df_payment_plugin_property = df_payment['plugin-property']

        # Взять только первое значение, т.к. plugin-property идёт строками в payment и размножает записи в df
        df_payment = df_payment.groupby('@order').nth(0) #.reset_index()

        df_payment['shop'] = shop
        df_payment['cash'] = cash
        df_payment['shift'] = shift
        df_payment['number'] = number
        df_payment['fiscalnum'] = fiscalnum

        # print('df_payment*******',df_payment)
        # print('df_payment_FIRST*******', df_payment.groupby('@order').nth(0).reset_index())
        # print('df_payment_plugin-property*******', df_payment['plugin-property'])

        # Забираем данные из дополнительных свойств (plugin-property) и записываем в нужные колонки df
        for j in df_payment_plugin_property:
            if isinstance(j, dict):
                #print('key=' + j['@key'], '; value=' + str(j['@value']))
                if j['@key'] in add_column_payment:
                    print(j['@key'] + '  ***====***', j['@value'])
                    df_payment[j['@key']] = j['@value']

        del df_payment['plugin-property']


        # Переименовать колонки у оплат
        df_payment.rename(columns={'@order': 'order', '@typeClass': 'typeClass',
                                   '@amount': 'amount', '@description': 'description'},
                          inplace=True)

        # Собрать оплаты в один df (чтобы потом 1 раз делать вставку в базу)
        df_payment_all = pd.concat([df_payment_all, df_payment])

        #print('df_payment_all********',df_payment_all)


    #sys.exit()

    # Чеки. Определить необходимые колонки
    col_Purchase = ['@tabNumber', '@userName', '@operationType', '@cashOperation',
                    '@operDay', '@shop', '@cash', '@shift', '@number', '@saletime', '@begintime',
                    '@amount', '@discountAmount', '@inn', '@qrcode',
                    '@fiscalDocNum', '@factorynum', '@fiscalnum', 'CASHIER_NAME'
                    ]

    # !!! Взять только определённые колонки.
    df_purchase = df_purchase.loc[:, df_purchase.columns.intersection(col_Purchase)]

    # Чеки. Переименовать колонки
    df_purchase.rename(columns={'@tabNumber': 'tabNumber', '@userName': 'userName',
                                '@operationType': 'operationType', '@cashOperation': 'cashOperation',
                                '@operDay': 'operDay', '@shop': 'shop', '@cash': 'cash', '@shift': 'shift',
                                '@number': 'number', '@saletime': 'saletime', '@begintime': 'begintime',
                                '@amount': 'amount', '@discountAmount': 'discountAmount',
                                '@inn': 'inn', '@qrcode': 'qrcode', '@fiscalDocNum': 'fiscalDocNum',
                                '@factorynum': 'factorynum', '@fiscalnum': 'fiscalnum'},
                       inplace=True)

    # Позиции чеков. Удалить из позиций пустые строки, т.к было объединение
    df_position_all = df_position_all.dropna(subset=['cash'])
    # Удалить ненужные колонки (чтобы вставка в базу прошла корректно)
    del df_position_all['plugin-property']

    # Оплаты. Удалить из оплат пустые строки, т.к было объединение
    df_payment_all = df_payment_all.dropna(subset=['cash'])
    # df_payment_all['plugin-property'] = df_payment_all['plugin-property'].astype(str)

    # Забрать чеки из базы
    purchase_query = "select tabNumber, userName, operationType, cashOperation, operDay, " \
                     "shop, cash, shift, number, saletime, begintime, amount, discountAmount, " \
                     "inn, qrcode, fiscalDocNum, factorynum, fiscalnum, CASHIER_NAME " \
                     "from Crystals_Purchase " \
                     "where convert(datetime,replace(operday,'+03:00','')) = '" + str(operday) + "'"
    df_purchase_base = api_utils.select_query(purchase_query, login_sql, password_sql, server_sql, driver_sql, database)

    # Убрать из даты +03:00  (2024-09-03+03:00)
    # Взять первые 10 символов из строки с датой
    df_purchase["operDay"] = df_purchase['operDay'].str[:10]

    # Удалить из df чеки, которые уже есть в базе
    if not df_purchase_base.empty:
        mask = df_purchase.isin(df_purchase_base.to_dict(orient='list')).all(axis=1)
        df_purchase_new = df_purchase[~mask]
    else:
        df_purchase_new = df_purchase
    # df_purchase_new.to_excel("in_df_purchase_new_NEW5.xlsx")

    api_utils.InsertLog('Всего продаж за ' + str(operday) + ' из Crystals: ' + str(len(df_purchase)))
    api_utils.InsertLog('Всего продаж за ' + str(operday) + ' в базе: ' + str(len(df_purchase_base)))
    api_utils.InsertLog('Всего продаж за ' + str(operday) + ' необходимо загрузить в базу: ' + str(len(df_purchase_new)))

    #sys.exit()

    # Загрузить отсутствующие чеки в бд
    if not df_purchase_new.empty:
        #print(df_purchase_new)

        # Позиции. Оставить только те, которых нет в базе.
        df_position_new = df_position_all.loc[df_position_all["shop"].isin(df_purchase_new['shop']) &
                                              df_position_all["cash"].isin(df_purchase_new['cash']) &
                                              df_position_all["shift"].isin(df_purchase_new['shift']) &
                                              df_position_all["number"].isin(df_purchase_new['number']) &
                                              df_position_all["fiscalnum"].isin(df_purchase_new['fiscalnum'])]
        api_utils.InsertLog('Всего позиций необходимо загрузить в базу: ' + str(len(df_position_new)))

        # Оплаты. Оставить только те, которых нет в базе.
        df_payment_new = df_payment_all.loc[df_payment_all["shop"].isin(df_purchase_new['shop']) &
                                            df_payment_all["cash"].isin(df_purchase_new['cash']) &
                                            df_payment_all["shift"].isin(df_purchase_new['shift']) &
                                            df_payment_all["number"].isin(df_purchase_new['number']) &
                                            df_payment_all["fiscalnum"].isin(df_purchase_new['fiscalnum'])]
        api_utils.InsertLog('Всего Оплат необходимо загрузить в базу: ' + str(len(df_payment_new)))
        #sys.exit()

        # Вставка чеков
        params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                         "SERVER=" + server_sql + ";"
                                         "DATABASE=" + database + ";"
                                         "UID=" + login_sql + ";"
                                         "PWD=" + password_sql + ";")
        engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
        df_purchase_new.to_sql("Crystals_Purchase", engine, index=False, chunksize=10000, if_exists="append")
        api_utils.InsertLog('Вставка чеков: Успешно')

        # Вставка позиций чеков
        params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                         "SERVER=" + server_sql + ";"
                                         "DATABASE=" + database + ";"
                                         "UID=" + login_sql + ";"
                                         "PWD=" + password_sql + ";")
        engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
        df_position_new.to_sql("Crystals_Purchase_Position", engine, index=False, chunksize=10000,
                               if_exists="append")
        api_utils.InsertLog('Вставка позиций: Успешно')

        # Вставка оплат
        params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                         "SERVER=" + server_sql + ";"
                                         "DATABASE=" + database + ";"
                                         "UID=" + login_sql + ";"
                                         "PWD=" + password_sql + ";")
        engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
        df_payment_new.to_sql("Crystals_Purchase_Payment", engine, index=False, chunksize=10000,
                              if_exists="append")
        api_utils.InsertLog('Вставка оплат: Успешно')


# IN. Экспорт Z-отчетов из SetRetail10 в ERP (веб-сервис на стороне SetRetail10)
def getZReportsByOperDay (url_srv, operday):
    # За заданный операционный день (getZReportsByOperDay)
    # Описание: https://crystals.atlassian.net/wiki/spaces/INT/pages/1646877/Z-+SetRetail10+ERP+-+SetRetail10
    url_c = url_srv + '/SET-ERPIntegration/FiscalInfoExport?wsdl'
    client = Client(url_c)
    result = client.service.getZReportsByOperDay(operday)
    file_xml = result.decode("utf-8")

    # Сохранить в файл
    # with open('z_xml', 'w', encoding='utf-8') as f:
    #     f.write(file_xml)
    #     f.close()

    # Преобразование XML в словарь
    dict_data = xmltodict.parse(file_xml)

    print(dict_data)

    # Количество z-отчётов
    zreport_count = dict_data.get('reports')["@count"]

    api_utils.InsertLog('Всего z-отчётов: ' + str(zreport_count))

    if zreport_count == '0':
        return

    # Преобразование словаря в JSON
    json_data = json.loads(json.dumps(dict_data, indent=4))
    df_zreport = pd.DataFrame(json_data['reports']['zreport'])

    #print(df_zreport)
    #df_zreport.to_excel('df_zreport.xlsx')

    if df_zreport.empty:
        return

    # Добавить нужные колонки в df
    add_column = ['FFD_REG_NUMBER', 'FFD_PAYMENT_PLACE', 'FN_NOT_SENT_OISM_NOTIFICATIONS', 'FN_NOT_SENT_DOC_COUNT',  # 4
                'FISCAL_DOC_ID', 'FN_NUM', 'FN_EXPIRES_IN_DAYS', 'FFD_ADDRESS', 'FN_DOCS_IN_SHIFT',  # 9
                'FN_RECEIPTS_IN_SHIFT', 'FN_FIRST_NOT_SENT_DOC_DATE_TIME', 'FPD',  # 12
                'CashPaymentEntity_amountPurchase', 'CashPaymentEntity_amountReturn',  # 14
                'BankCardPaymentEntity_amountPurchase', 'BankCardPaymentEntity_amountReturn',  # 16
                'nds10_ndsSumSale', 'nds10_ndsSumReturn', 'nds10_sumPosition',  # 19
                # 'nds18_ndsSumSale', 'nds18_ndsSumReturn', 'nds18_sumPosition',  # НДС 18% убрал
                'nds20_ndsSumSale', 'nds20_ndsSumReturn', 'nds20_sumPosition']  # 22

    df_zreport[add_column] = None, None, None, None, None, None, None, None, None, None, None, \
                             None, None, None, None, None, None, None, None, None, None, None

    # Актуальные НДС
    tax = ('10', '20')

    # Соберём данные z-отчётов в один df
    for i, row in df_zreport.iterrows():
        print(f"Index: {i}")
        print(f"{row}\n")

        # Забираем данные из дополнительных свойств (plugin-property) и добавляем в df
        for j in row['plugin-property']:
            print('key=' + j['@key'], '; value=' + j['@value'])

            if j['@key'] in add_column:
                print(j['@key'] + '  ***====***', j['@value'])
                df_zreport[j['@key']][i] = j['@value']

        # Забираем данные платежей
        print('payments=====', row['payments'], type(row['payments']))
        if isinstance(row['payments'], dict):
            for k in row['payments']:
                for p in row['payments'][k]:

                    # Наличные
                    if p['@typeClass'] == 'CashPaymentEntity':
                        if '@amountPurchase' in p:
                            print('CashPaymentEntity_amountPurchase====', p['@amountPurchase'])
                            df_zreport['CashPaymentEntity_amountPurchase'][i] = p['@amountPurchase']
                        if '@amountReturn' in p:
                            print('CashPaymentEntity_amountReturn====', p['@amountReturn'])
                            df_zreport['CashPaymentEntity_amountReturn'][i] = p['@amountReturn']

                    # Оплата картой
                    if p['@typeClass'] == 'BankCardPaymentEntity':
                        if '@amountPurchase' in p:
                            print('BankCardPaymentEntity_amountPurchase====', p['@amountPurchase'])
                            df_zreport['BankCardPaymentEntity_amountPurchase'][i] = p['@amountPurchase']
                        if '@amountReturn' in p:
                            print('BankCardPaymentEntity_amountReturn====', p['@amountReturn'])
                            df_zreport['BankCardPaymentEntity_amountReturn'][i] = p['@amountReturn']

        # Забираем данные сумм НДС
        print('taxes=====', row['taxes'], type(row['taxes']))
        if isinstance(row['taxes'], dict):
            for l in row['taxes']:
                #print(l) # taxes - tax
                for t in row['taxes'][l]:
                    if '@nds' in t:
                        print('nds====', t['@nds'])
                        # Забирать только по актуальным НДС (tax = ('10', '18', '20'))
                        if t['@nds'] in tax:
                            if '@ndsSumSale' in t:
                                print(str(t['@nds']) + ', ndsSumSale====', t['@ndsSumSale'])
                                df_zreport['nds' + str(t['@nds']) + '_ndsSumSale'][i] = t['@ndsSumSale']
                            if '@ndsSumReturn' in t:
                                print(str(t['@nds']) + ', ndsSumReturn====', t['@ndsSumReturn'])
                                df_zreport['nds' + str(t['@nds']) + '_ndsSumReturn'][i] = t['@ndsSumReturn']
                            if '@sumPosition' in t:
                                print(str(t['@nds']) + ', sumPosition====', t['@sumPosition'])
                                df_zreport['nds' + str(t['@nds']) + '_sumPosition'][i] = t['@sumPosition']

    # Удалить ненужные колонки (чтобы вставка в базу прошла корректно)
    df_zreport.drop(['plugin-property', 'payments', 'taxes'], axis=1, inplace=True)

    api_utils.InsertLog('Всего за ' + str(operday) + ' z-отчётов в Crystals: ' + str(len(df_zreport)))

    if not df_zreport.empty:
        # Забрать Z-отчёты из базы
        zreport_query = "select convert(varchar(50),shiftNumber) as shiftNumber, " \
                        "convert(varchar(50),shopNumber) as shopNumber," \
                        "convert(varchar(50),docNumber) as docNumber," \
                        "convert(varchar(50),cashNumber) as cashNumber," \
                        "serialCashNumber " \
                        "from Crystals_ZReports " \
                        "where dateOperDay = '" + str(operday) + "'"
                        # "where convert(datetime,replace(dateOperDay,'+03:00','')) = '" + str(operday) + "'"
        df_zreport_base = api_utils.select_query(zreport_query, login_sql, password_sql, server_sql, driver_sql, database)

        api_utils.InsertLog('Всего за ' + str(operday) + ' z-отчётов в базе: ' + str(len(df_zreport_base)))

        # Удалить Z-отчёты, которые уже есть в базе
        if not df_zreport_base.empty:
            # Соединяем даныне из Crystal с данными из базы
            df_zreport_all = pd.merge(df_zreport, df_zreport_base,
                                      on=['shiftNumber', 'shopNumber', 'docNumber', 'cashNumber', 'serialCashNumber'],
                                      how="outer", indicator=True)
            # Убираем то, что есть в базе
            df_zreport_new = df_zreport_all.loc[df_zreport_all["_merge"] == "left_only"].drop("_merge", axis=1)

            # print(df_zreport_new)
        else:
            df_zreport_new = df_zreport
        # df_purchase_new.to_excel("in_df_purchase_new_NEW5.xlsx")

        api_utils.InsertLog('Всего за ' + str(operday) + ' z-отчётов необходимо загрузить в базу: ' + str(len(df_zreport_new)))
        #print(df_zreport_new)
        #sys.exit()

        # Вставка z-отчётов
        if not df_zreport_new.empty:

            # Убрать из даты +03:00  (2024-09-03+03:00)
            # df_zreport["dateOperDay"] = pd.to_datetime(df_zreport['dateOperDay'], format='%Y-%m-%d+03:00')
            # Взять первые 10 символов из строки с датой
            df_zreport_new["dateOperDay"] = df_zreport_new['dateOperDay'].str[:10]

            params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                             "SERVER=" + server_sql + ";"
                                             "DATABASE=" + database + ";"
                                             "UID=" + login_sql + ";"
                                             "PWD=" + password_sql + ";")
            engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
            df_zreport_new.to_sql("Crystals_ZReports", engine, index=False, chunksize=10000, if_exists="append")
            api_utils.InsertLog('Вставка z-отчётов: Успешно')

if __name__ == '__main__':
    try:
        # Создание дирректорий логов
        api_utils.CreateLogDir()

        startTime = datetime.datetime.now()
        api_utils.InsertLog('Start at: ' + str(startTime))

        MLog = startTime.strftime('%m')
        YLog = startTime.strftime('%Y')

        # Получение настроек
        try:
            settings = api_utils.GetSettings()
            login_sql = settings["login_sql"]
            password_sql = settings["password_sql"]
            server_sql = settings["server_sql"]
            driver_sql = settings["driver_sql"]
            database = settings["database"]
            mails = settings["mails_to_send"].split()
            shared_folder = settings["shared_folder"]
            url_srv = settings["url_srv"]
        except:
            api_utils.InsertLog('Ошибка при получении настроек')
            time.sleep(3)
            sys.exit()

        # !!!
        # OUT. Отпавить файлы из расшаренной папки \\piter-sql\Export\Crystal в Crystals
        # api_utils.InsertLog('Экспорт. Отправка файлов в Crystals')
        # send_files_to_crystals()
        # api_utils.InsertLog('Успешно')

        # !!!
        # IN. Получить данные о продажах из Crystals за период
        # date_begin = '2024-09-03'
        # date_end = '2024-09-03T23:59:00.000'
        # api_utils.InsertLog('Импорт. Получаем данные о продажах из Crystals за период: ' + str(date_begin) + ' - ' + str(date_end))
        # getPurchasesByPeriod(url_srv, date_begin, date_end)

        # !!!
        # IN. Получить данные о продажах из Crystals за операционный день (для сверки - всё ли есть в базе)
        OperDay = '2024-09-03'
        api_utils.InsertLog('Импорт. Получаем данные о продажах из Crystals за операционный день: ' + str(OperDay))
        getPurchasesByOperDay(url_srv, OperDay)

        # !!!
        # IN. Получить данные о z-отчётах из Crystals за операционный день
        # OperDay = '2024-09-03'
        # api_utils.InsertLog('Импорт. Получаем данные z-отчётов из Crystals за операционный день: ' + str(OperDay))
        # getZReportsByOperDay(url_srv, OperDay)

        # sys.exit()

    except:
        startTime = datetime.datetime.now()
        t_log = startTime.strftime('%d.%m.%Y %H:%M:%S')
        log = open('./src/Logs/' + str(YLog) + '/Logs_' + str(MLog) + '.txt', 'a+', encoding="utf-8")
        log.write(t_log + ' ## Ошибка в процедуре Parser_main.py\n')
        traceback.print_exc(file=log)
        log.close()
        traceback.print_exc()
        time.sleep(10)
        sys.exit()

