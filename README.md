# jsonToSav
Конвертация `json` файлов после распознавания чеков в `txt` и `sav`

Нужно задать следующие параметры:

путь к файлам `json`<br>
`pathJson` = r"JSON_READY"

имена выходных файлов<br>
`pathTxt` = r"odf.txt"<br>
`pathSav` = r"spss.sav"

###### Структура `json` файла
<pre>{
  "date": "14.01.20 17:39",
  "shopName": "Красное  Белое",
  "shopAddress": "Новосибирская ул. 26 • Пермь",
  "totalSum": 239.99,
  "products": [
    {
      "category": "Алкоголь",
      "name": "#Коньяк Армянс.прозр.0,25",
      "quantity": 1.0,
      "price": 239.99,
      "sum": 239.99
    }
  ]
}</pre>

###### Структура расширенного `json` файла
<pre>{
  "cashTotalSum": 6490,
  "dateTime": 1580148120,
  "discount": null,
  "discountSum": null,
  "ecashTotalSum": 0,
  "fiscalDocumentNumber": 3939,
  "fiscalDriveNumber": "1231231231321321321",
  "fiscalSign": 0000113111,
  "items": [
    {
      "modifiers": null,
      "name": "Что-то",
      "nds0": null,
      "nds10": null,
      "nds18": null,
      "ndsCalculated10": null,
      "ndsCalculated18": null,
      "ndsNo": null,
      "price": 6490,
      "quantity": 1.0,
      "sum": 6490,
      "storno": false
    }
  ],
  "kktNumber": null,
  "kktRegId": "00012312312123",
  "markup": null,
  "markupSum": null,
  "modifiers": null,
  "nds0": null,
  "nds10": null,
  "nds18": null,
  "ndsCalculated10": null,
  "ndsCalculated18": null,
  "ndsNo": 6490,
  "operationType": 1,
  "operator": "КАССИР 02",
  "requestNumber": 35,
  "retailPlaceAddress": null,
  "shiftNumber": 153,
  "stornoItems": null,
  "taxationType": 8,
  "totalSum": 6490,
  "user": null,
  "userInn": "00000000"
}</pre>