import pandas as pd
import json
import os
import savReaderWriter as srw

# список со всеми данными
dAll = []

# путь к файлам с json
pathJson = r"W:\iMQ\Projects\_PROJECTS\Lenta\19-089577_Lenta Segmentation\4. DATA\Receipts from coding\JSON_READY"
pathTxt = r"W:\iMQ\Projects\_PROJECTS\Lenta\19-089577_Lenta Segmentation\4. DATA\Receipts from coding\odf.txt"
pathSav = r"W:\iMQ\Projects\_PROJECTS\Lenta\19-089577_Lenta Segmentation\4. DATA\Receipts from coding\spss.sav"


def getData(path: str):
    with open(path, "r", encoding="utf8") as f:
        file_content = os.path.getsize(path)  # f.read().strip()
        # Проверяем, не пустой ли файл
        if file_content > 0:
            dJson = json.load(f)
            dJson["filename"] = os.path.splitext(os.path.basename(path))[0]
            dJson1 = {k: v for k, v in dJson.items() if k != "products"}

            try:
                for d in dJson["products"]:
                    dJson1.update(d)
                    dAll.append(dJson1.copy())
            except KeyError:
                print("KeyError: " + path)
        else:
            print("Empty: " + path)


# прохожусь по всем файлам и собираю json в единый словарь
with os.scandir(pathJson) as it:
    for entry in it:
        if entry.is_file(follow_symlinks=False) and entry.name.endswith(".json"):
            getData(entry.path)

# конвертирую в панду
df = pd.DataFrame(dAll)


def memUsage(pandas_obj):
    if isinstance(pandas_obj, pd.DataFrame):
        usage_b = pandas_obj.memory_usage(deep=True).sum()
    else:  # предположим, что если это не датафрейм, то серия
        usage_b = pandas_obj.memory_usage(deep=True)
    # usage_mb = usage_b / 1024 ** 2 # преобразуем байты в мегабайты
    usage_kb = usage_b / 1024  # преобразуем байты в килобайты
    return "{:03.2f} KB".format(usage_kb)


print(df.dtypes)
print(memUsage(df))

# оптимизирую df
odf = df.copy()
odf['quantity'] = odf['quantity'].astype('uint8')
odf['totalSum'] = odf['totalSum'].astype('float32')
odf['price'] = odf['price'].astype('float32')
odf['sum'] = odf['sum'].astype('float32')
odf['date'] = pd.to_datetime(odf['date'], format='%d.%m.%y %H:%M')

for col in ['shopAddress', 'shopName', 'category', 'name']:
    num_unique_values = len(odf[col].unique())
    num_total_values = len(odf[col])
    if num_unique_values / num_total_values < 0.5:
        odf[col] = odf[col].astype('category')
print(odf.dtypes)

valLabs = {}
for col in odf.columns:
    if str(odf[col].dtype) == 'category':
        valLabs[col] = {i: x[:30] for i, x in enumerate(odf[col].cat.categories)}
        odf[col] = odf[col].cat.codes

# смотрим использование памяти
print(memUsage(df))
print(memUsage(odf))

print(odf['filename'].unique())
print(len(odf['filename'].unique()))

# сохраняем в tab
odf.to_csv(pathTxt, sep="\t", index=False)

# собираем парметры для сохранения в SPSS
odf['date'] = odf['date'].astype('str')

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
print(varTypes)

colsSave = list(odf.columns)

# https://pythonhosted.org/savReaderWriter/generated_api_documentation.html#savwriter
with srw.SavWriter(pathSav, varNames=colsSave, varTypes=varTypes,
                   valueLabels=valLabs, ioUtf8=True, formats={'date': 'DATETIME17'}) as writer:
    for record in records:
        record[0] = writer.spssDateTime(record[0].encode(), '%Y-%m-%d %H:%M:%S')
        writer.writerow(record)
