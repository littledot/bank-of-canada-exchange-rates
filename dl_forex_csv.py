import requests
import shutil
import functools
import json
from datetime import date, timedelta
import os
import csv
from itertools import islice

# https://www.bankofcanada.ca/rates/exchange/daily-exchange-rates-lookup/

dailyUrlFmt = "https://www.bankofcanada.ca/valet/observations/FXAUDCAD,FXBRLCAD,FXCNYCAD,FXEURCAD,FXHKDCAD,FXINRCAD,FXIDRCAD,FXJPYCAD,FXMYRCAD,FXMXNCAD,FXNZDCAD,FXNOKCAD,FXPENCAD,FXRUBCAD,FXSARCAD,FXSGDCAD,FXZARCAD,FXKRWCAD,FXSEKCAD,FXCHFCAD,FXTWDCAD,FXTHBCAD,FXTRYCAD,FXGBPCAD,FXUSDCAD,FXVNDCAD/csv?start_date={start}&end_date={end}"

dailyUrlFmt2 = "https://www.bankofcanada.ca/valet/observations/FXEURCAD,FXUSDCAD/csv?start_date={start}&end_date={end}"

yearUrl = "https://www.bankofcanada.ca/valet/observations/group/FX_RATES_ANNUAL/json?start_date=2017-01-01"


def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)


def downloadFile(url, path):
    with requests.get(url, stream=True) as r:
        with open(path, "wb") as f:
            r.raw.read = functools.partial(r.raw.read, decode_content=True)  # decodes gzip
            shutil.copyfileobj(r.raw, f)


def downloadForex(start_year, end_year, path):
    url = dailyUrlFmt2.format(
        start=f"{start_year}-12-25",
        end=f"{end_year}-01-01",
    )
    downloadFile(url, path)
    return path


def cleanDailyData(names):
    with open(names["raw"], "r") as inFile, open(names["clean"], "w") as outFile:
        reader = csv.reader(inFile)
        writer = csv.writer(outFile)

        start = False
        for row in reader:
            if len(row)>0 and row[0] == "date":
                start = True
            if start:
                writer.writerow(row)


def fillDate(names):
    with open(names["clean"], "r") as inFile, open(names["full"], "w") as outFile:
        reader = csv.DictReader(inFile)
        writer = csv.DictWriter(outFile, reader.fieldnames)
        writer.writeheader()

        lastRow = next(reader)

        for row in reader:
            rowDate = date.fromisoformat(row["date"])
            lastDate = date.fromisoformat(lastRow["date"])
            while lastDate < rowDate:
                writer.writerow(lastRow)

                lastDate += timedelta(days=1)
                lastRow["date"] = lastDate

            lastRow = row

        writer.writerow(lastRow)


def getFileNames(name):
    return {
        "raw": f"data/raw/{name}.raw.csv",
        "clean": f"data/out/{name}.csv",
        "full": f"data/out/{name}.full.csv",
    }


def main():
    # for i in range(2017, 2022):
    names = getFileNames("boc_daily")
    downloadForex(2016, 2023, names["raw"])
    cleanDailyData(names)
    fillDate(names)

    # names = getFileNames("boc_annual")
    # downloadFile(yearUrl, names["raw"])
    # cleanAnnualData(names)


if __name__ == "__main__":
    main()
