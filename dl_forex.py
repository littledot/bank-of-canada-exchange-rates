import requests
import shutil
import functools
import json

# https://www.bankofcanada.ca/rates/exchange/daily-exchange-rates-lookup/

dailyUrlFmt = "https://www.bankofcanada.ca/valet/observations/FXAUDCAD,FXBRLCAD,FXCNYCAD,FXEURCAD,FXHKDCAD,FXINRCAD,FXIDRCAD,FXJPYCAD,FXMYRCAD,FXMXNCAD,FXNZDCAD,FXNOKCAD,FXPENCAD,FXRUBCAD,FXSARCAD,FXSGDCAD,FXZARCAD,FXKRWCAD,FXSEKCAD,FXCHFCAD,FXTWDCAD,FXTHBCAD,FXTRYCAD,FXGBPCAD,FXUSDCAD,FXVNDCAD/json?start_date={start}&end_date={end}"

yearUrl = "https://www.bankofcanada.ca/valet/observations/group/FX_RATES_ANNUAL/json?start_date=2017-01-01"


def downloadFile(url, path):
    with requests.get(url, stream=True) as r:
        with open(path, "wb") as f:
            r.raw.read = functools.partial(r.raw.read, decode_content=True)  # decodes gzip
            shutil.copyfileobj(r.raw, f)


def downloadForex(year, path):
    url = dailyUrlFmt.format(start="{}-12-25".format(year - 1), end="{}-01-01".format(year + 1))
    downloadFile(url, path)
    return path


def cleanData(names):
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

        with open(names["clean"], "w") as outFile:
            json.dump(outJson, outFile)

        with open(names["pretty"], "w") as outFile:
            json.dump(outJson, outFile, indent=4)


def getFileNames(name):
    return {
        "raw": name + ".raw.json",
        "clean": name + ".json",
        "pretty": name + ".pretty.json",
    }


def main():
    for i in range(2017, 2022):
        names = getFileNames("data/boc_" + str(i))
        downloadForex(i, names["raw"])
        cleanData(names)

    names = getFileNames("data/boc_annual")
    downloadFile(yearUrl, names["raw"])
    cleanData(names)


if __name__ == "__main__":
    main()
