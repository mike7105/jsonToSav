"""Программа собирает все файлы json (результат распознавания чеков) и конвертирует в tab и SPSS файлы
Михаил Чесноков 2020"""
__version__ = 'Version:2.0'

import pandas as pd
from pandas import DataFrame, Series
import json
import os
import savReaderWriter as srw
import logging
import shutil
import datetime as dt

timestart = dt.datetime.now()
# список со всеми данными
dAll: list = []

# путь к файлам с json
pathJson = r"JSON_READY"
pathTxt = r"odf.txt"
pathSav = r"spss.sav"
pathJsonE = os.path.join(pathJson, "Empty")
pathJsonK = os.path.join(pathJson, "Key")

os.makedirs(pathJsonE, exist_ok=True)
os.makedirs(pathJsonK, exist_ok=True)

logging.basicConfig(level=logging.INFO, format=' %(asctime)s - %(levelname)s - %(message)s',
                    filename=os.path.abspath('log.txt'))


def getData(path):
    """
    Собирает данные из json в словарь
    :param str path: путь к файлу
    """
    dJson: dict = {}
    with open(path, "r", encoding="utf8") as f:
        file_content = os.path.getsize(path)  # f.read().strip()
        # Проверяем, не пустой ли файл
        if file_content > 0:
            dJson: dict = json.load(f)

    if len(dJson) > 0:
        dJson["filename"] = os.path.splitext(os.path.basename(path))[0]
        dJson1: dict = {k: v for k, v in dJson.items() if k != "products"}

        try:
            for d in dJson["products"]:
                dJson1.update(d)
                dAll.append(dJson1.copy())
        except KeyError:
            logging.error(f"KeyError: {path}")
            shutil.move(path, pathJsonK)
    else:
        logging.error(f"Empty: {path}")
        shutil.move(path, pathJsonE)


def getDataWide(path):
    """
    Собирает данные из json (расширенная структура) в словарь
    :param str path: путь к файлу
    """
    dJson: dict = {}
    with open(path, "r", encoding="utf8") as f:
        file_content = os.path.getsize(path)  # f.read().strip()
        # Проверяем, не пустой ли файл
        if file_content > 0:
            dJson: dict = json.load(f)

    if len(dJson) > 0:
        dJson["filename"] = os.path.splitext(os.path.basename(path))[0]
        dJson1: dict = {k: v for k, v in dJson.items() if k != "items"}
        dJson1["shopName"] = dJson1.pop("user")
        dJson1["shopAddress"] = dJson1.pop("retailPlaceAddress")
        dJson1["date"] = dJson1.pop("dateTime")
        dJson1["date"] = dt.datetime.fromtimestamp(dJson1["date"]).strftime('%d.%m.%y %H:%M')

        try:
            for d in dJson["items"]:
                dJson1.update(d)
                dJson1["modifiers"] = None if not dJson1["modifiers"] else dJson1["modifiers"]
                dJson1["stornoItems"] = None if not dJson1["stornoItems"] else dJson1["stornoItems"]
                dJson1["storno"] = 1 if dJson1["storno"] else 0
                dAll.append(dJson1.copy())
        except KeyError:
            logging.error(f"KeyError: {path}")
    else:
        logging.error(f"Empty: {path}")


# прохожусь по всем файлам и собираю json в единый словарь
with os.scandir(pathJson) as it:
    for entry in it:
        if entry.is_file(follow_symlinks=False) and entry.name.endswith(".json"):
            getData(entry.path)

# прохожусь по папке с другой структурой json
for it in os.listdir(pathJsonK):
    if it.endswith(".json"):
        getDataWide(os.path.join(pathJsonK, it))

# конвертирую в панду
df: DataFrame = DataFrame(dAll)


def memUsage(pandas_obj):
    """
    Подсчет занимаемого места в памяти файла панды
    :param DataFrame or Series pandas_obj: объект панды
    :return: строка с занимаемой памятью в KB
    :rtype: str
    """
    if isinstance(pandas_obj, DataFrame):
        usage_b = pandas_obj.memory_usage(deep=True).sum()
    else:  # предположим, что если это не датафрейм, то серия
        usage_b = pandas_obj.memory_usage(deep=True)
    # usage_mb = usage_b / 1024 ** 2 # преобразуем байты в мегабайты
    usage_kb = usage_b / 1024  # преобразуем байты в килобайты
    return f"{usage_kb:03.2f} KB"


logging.debug(f"df.dtypes \n{df.dtypes}")
logging.info(f"df {memUsage(df)}")

# оптимизирую df
odf = df.copy()
odf['quantity'] = odf['quantity'].astype('float32')
odf['totalSum'] = odf['totalSum'].astype('float32')
odf['price'] = odf['price'].astype('float32')
odf['sum'] = odf['sum'].astype('float32')
odf['cashTotalSum'] = odf['cashTotalSum'].astype('float32')
odf['discount'] = odf['discount'].astype('float32')
odf['discountSum'] = odf['discountSum'].astype('float32')
odf['ecashTotalSum'] = odf['ecashTotalSum'].astype('float32')
odf['fiscalDocumentNumber'] = odf['fiscalDocumentNumber'].astype('float32')
odf['fiscalSign'] = odf['fiscalSign'].astype('float32')
odf['kktNumber'] = odf['kktNumber'].astype('float32')
odf['markup'] = odf['markup'].astype('float32')
odf['markupSum'] = odf['markupSum'].astype('float32')
odf['nds0'] = odf['nds0'].astype('float32')
odf['nds10'] = odf['nds10'].astype('float32')
odf['nds18'] = odf['nds18'].astype('float32')
odf['ndsCalculated10'] = odf['ndsCalculated10'].astype('float32')
odf['ndsCalculated18'] = odf['ndsCalculated18'].astype('float32')
odf['ndsNo'] = odf['ndsNo'].astype('float32')
odf['operationType'] = odf['operationType'].astype('float32')
odf['requestNumber'] = odf['requestNumber'].astype('float32')
odf['shiftNumber'] = odf['shiftNumber'].astype('float32')
odf['taxationType'] = odf['taxationType'].astype('float32')
odf['fiscalDriveNumber'] = odf['fiscalDriveNumber'].astype('float32')
odf['kktRegId'] = odf['kktRegId'].astype('float32')
odf['userInn'] = odf['userInn'].astype('float32')
odf['storno'] = odf['storno'].astype('float16')

odf['date'] = pd.to_datetime(odf['date'], format='%d.%m.%y %H:%M')

for col in ['shopName', 'shopAddress', 'category', 'name', 'modifiers', 'operator', 'stornoItems']:
    logging.debug(f"col = {col}")
    num_unique_values = len(odf[col].unique())
    num_total_values = len(odf[col])
    if (num_unique_values / num_total_values) < 0.5 and num_unique_values > 1:
        logging.debug(f"col = {col}")
        odf[col] = odf[col].astype('category')
logging.debug(f"odf.dtypes \n{odf.dtypes}")

valLabs = {}
for col in odf.columns:
    if str(odf[col].dtype) == 'category':
        logging.debug(f"col = {col}")
        valLabs[col] = {i: x[:30] for i, x in enumerate(odf[col].cat.categories)}
        odf[col] = odf[col].cat.codes

# смотрим использование памяти
logging.info(f"df {memUsage(df)}")
logging.info(f"odf {memUsage(odf)}")

logging.debug(f"odf unique filenames: {len(odf['filename'].unique())}")

# сохраняем в tab
odf.to_csv(pathTxt, sep="\t", index=False)
logging.info(f"Save {os.path.abspath(pathTxt)}")

# собираем парметры для сохранения в SPSS
odf['date'] = odf['date'].astype('str')
logging.info(f"odf {memUsage(odf)}")

records = odf.values.tolist()

varTypes = {}
for col in odf.columns:
    if str(odf[col].dtype) == 'object':
        varTypes[col] = 1024
    elif 'date' in str(odf[col].dtype):
        varTypes[col] = 0
    else:
        varTypes[col] = 0
varTypes['date'] = 0
logging.debug(f"varTypes: {varTypes}")

colsSave = list(odf.columns)

# https://pythonhosted.org/savReaderWriter/generated_api_documentation.html#savwriter
with srw.SavWriter(pathSav, varNames=colsSave, varTypes=varTypes,
                   valueLabels=valLabs, ioUtf8=True, formats={'date': 'DATETIME17'}) as writer:
    for record in records:
        record[0] = writer.spssDateTime(record[0].encode(), '%Y-%m-%d %H:%M:%S')
        writer.writerow(record)

logging.info(f"Save {os.path.abspath(pathSav)}")

timeend = dt.datetime.now()

logging.info(f"\nstart time: {timestart}\nfinish time: {timeend}\nduration: {(timeend - timestart)}")
print(f"start time: {timestart}\nfinish time: {timeend}\nduration: {(timeend - timestart)}")
