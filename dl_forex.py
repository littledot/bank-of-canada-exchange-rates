import requests
import shutil
import functools

# https://www.bankofcanada.ca/rates/exchange/daily-exchange-rates-lookup/

# api: https://www.bankofcanada.ca/valet/observations/FXAUDCAD,FXBRLCAD,FXCNYCAD,FXEURCAD,FXHKDCAD,FXINRCAD,FXIDRCAD,FXJPYCAD,FXMYRCAD,FXMXNCAD,FXNZDCAD,FXNOKCAD,FXPENCAD,FXRUBCAD,FXSARCAD,FXSGDCAD,FXZARCAD,FXKRWCAD,FXSEKCAD,FXCHFCAD,FXTWDCAD,FXTHBCAD,FXTRYCAD,FXGBPCAD,FXUSDCAD,FXVNDCAD/json?start_date=2022-03-30&end_date=2022-04-07

urlFmt = "https://www.bankofcanada.ca/valet/observations/FXAUDCAD,FXBRLCAD,FXCNYCAD,FXEURCAD,FXHKDCAD,FXINRCAD,FXIDRCAD,FXJPYCAD,FXMYRCAD,FXMXNCAD,FXNZDCAD,FXNOKCAD,FXPENCAD,FXRUBCAD,FXSARCAD,FXSGDCAD,FXZARCAD,FXKRWCAD,FXSEKCAD,FXCHFCAD,FXTWDCAD,FXTHBCAD,FXTRYCAD,FXGBPCAD,FXUSDCAD,FXVNDCAD/json?start_date={start}&end_date={end}"


def download_file(url, dest):
    with requests.get(url, stream=True) as r:
        with open(dest, "wb") as f:
            r.raw.read = functools.partial(r.raw.read, decode_content=True)
            shutil.copyfileobj(r.raw, f)


def download_forex(year):
    url = urlFmt.format(start="{}-12-25".format(year - 1), end="{}-01-01".format(year + 1))
    download_file(url, "cad_{}.json".format(year))


def main():
    for i in range(2020, 2022):
        download_forex(i)


if __name__ == "__main__":
    main()
