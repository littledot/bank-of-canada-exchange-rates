import requests
import shutil
import functools
import json
from datetime import date, timedelta
import os

# https://www.bankofcanada.ca/rates/exchange/daily-exchange-rates-lookup/

dailyUrlFmt = "https://www.bankofcanada.ca/valet/observations/FXAUDCAD,FXBRLCAD,FXCNYCAD,FXEURCAD,FXHKDCAD,FXINRCAD,FXIDRCAD,FXJPYCAD,FXMYRCAD,FXMXNCAD,FXNZDCAD,FXNOKCAD,FXPENCAD,FXRUBCAD,FXSARCAD,FXSGDCAD,FXZARCAD,FXKRWCAD,FXSEKCAD,FXCHFCAD,FXTWDCAD,FXTHBCAD,FXTRYCAD,FXGBPCAD,FXUSDCAD,FXVNDCAD/json?start_date={start}&end_date={end}"

yearUrl = "https://www.bankofcanada.ca/valet/observations/group/FX_RATES_ANNUAL/json?start_date=2017-01-01"


def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)


def downloadFile(url, path):
    with requests.get(url, stream=True) as r:
        with open(path, "wb") as f:
            r.raw.read = functools.partial(r.raw.read, decode_content=True)  # decodes gzip
            shutil.copyfileobj(r.raw, f)


def downloadForex(year, path):
    url = dailyUrlFmt.format(
        start=f"{year-1}-12-25",
        end=f"{year+1}-01-01",
    )
    downloadFile(url, path)
    return path


def saveJson(obj, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)

    with open(f"{path}.json", "w") as outFile:
        json.dump(obj, outFile)

    with open(f"{path}.pretty.json", "w") as outFile:
        json.dump(obj, outFile, indent=4)


def cleanDailyData(names):
    with open(names["raw"], "r") as inFile:
        inJson = json.load(inFile)

        outObs = {}
        outJson = {
            "terms": inJson["terms"],
            "seriesDetail": inJson["seriesDetail"],
            "observations": outObs,
        }

        for currency in inJson["seriesDetail"].keys():
            outCurObs = {}
            outObs[currency] = outCurObs

            for inObs in inJson["observations"]:
                dateStr = inObs["d"]
                outCurObs[dateStr] = inObs.get(currency, {}).get("v")

        saveJson(outJson, names["clean"])


def cleanAnnualData(names):
    with open(names["raw"], "r") as inFile:
        inJson = json.load(inFile)

        allObs = {}
        outJson = {
            "terms": inJson["terms"],
            "seriesDetail": inJson["seriesDetail"],
            "observations": allObs,
        }

        for inObs in inJson["observations"]:
            date = inObs.pop("d")
            outObs = {}
            allObs[date] = outObs

            for currency, rate in inObs.items():
                outObs[currency] = rate["v"]

        saveJson(outJson, names["clean"])


def fillDate(names, year):
    with open(names["clean"] + ".json", "r") as inFile:
        inJson = json.load(inFile)

        outObs = {}
        outJson = {
            "terms": inJson["terms"],
            "seriesDetail": inJson["seriesDetail"],
            "observations": outObs,
        }

        for currency, inCurObs in inJson["observations"].items():
            inDateStrs = list(inCurObs.keys())
            outCurObs = {}
            outObs[currency] = outCurObs

            startDate = date.fromisoformat(inDateStrs[0])
            endDate = date(year + 1, 1, 1)
            lastObs = inCurObs[inDateStrs[0]]
            for curDate in daterange(startDate, endDate):
                curDateStr = curDate.isoformat()
                if curDateStr in inCurObs:
                    outCurObs[curDateStr] = inCurObs[curDateStr]
                else:
                    outCurObs[curDateStr] = lastObs
                    print(f"filled {curDateStr}")
                lastObs = outCurObs[curDateStr]

        saveJson(outJson, names["full"])


def getFileNames(name):
    return {
        "raw": f"data/raw/{name}.raw.json",
        "clean": f"data/out/{name}",
        "full": f"data/out/{name}.full",
    }


def main():
    for i in range(2017, 2022):
        names = getFileNames("boc_" + str(i))
        downloadForex(i, names["raw"])
        cleanDailyData(names)
        fillDate(names, i)

    names = getFileNames("boc_annual")
    downloadFile(yearUrl, names["raw"])
    cleanAnnualData(names)


if __name__ == "__main__":
    main()
