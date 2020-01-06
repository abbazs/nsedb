"""
 The MIT License (MIT)
 
 Copyright (c) 2019 abbazs
 
 Permission is hereby granted, free of charge, to any person obtaining a copy
 of this software and associated documentation files (the "Software"), to deal
 in the Software without restriction, including without limitation the rights
 to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 copies of the Software, and to permit persons to whom the Software is
 furnished to do so, subject to the following conditions:
 
 The above copyright notice and this permission notice shall be included in all
 copies or substantial portions of the Software.
 
 THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 SOFTWARE.
"""
import json
import os
import sys
import time
import urllib
import zipfile
from datetime import date, datetime, timedelta
from io import BytesIO, StringIO

import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
from dateutil import parser
from pandas.io import sql
from sqlalchemy import create_engine

from log import print_exception


class nsedb(object):
    """Creates and updates database of index Nifty and BankNifty since 1994"""

    headers = {"Host": "www.nseindia.com"}
    headers["User-Agent"] = "Opera/9.80 (Windows NT 6.0) Presto/2.12.388 Version/12.14"
    headers["Accept"] = "*/*"
    headers["Accept-Language"] = "en-US,en;q=0.5"
    headers["Accept-Encoding"] = "gzip, deflate, br"
    headers[
        "Referer"
    ] = "https://www1.nseindia.com/products/content/equities/equities/archieve_eq.htm"
    headers["Connection"] = "keep-alive"

    fno_col_typ = {
        "STRIKE_PR": np.float,
        "OPEN": np.float,
        "HIGH": np.float,
        "LOW": np.float,
        "CLOSE": np.float,
        "SETTLE_PR": np.float,
        "CONTRACTS": np.float,
        "VAL_INLAKH": np.float,
        "OPEN_INT": np.float,
        "CHG_IN_OI": np.float,
    }

    fno_cols = [
        "TIMESTAMP",
        "INSTRUMENT",
        "SYMBOL",
        "EXPIRY_DT",
        "STRIKE_PR",
        "OPTION_TYP",
        "OPEN",
        "HIGH",
        "LOW",
        "CLOSE",
        "SETTLE_PR",
        "CONTRACTS",
        "VAL_INLAKH",
        "OPEN_INT",
        "CHG_IN_OI",
    ]

    eq_cols = [
        "TIMESTAMP",
        "SYMBOL",
        "OPEN",
        "HIGH",
        "LOW",
        "CLOSE",
        "LAST",
        "PREVCLOSE",
        "TOTTRDQTY",
        "TOTTRDVAL",
    ]

    idx_col_rename = {"SHARESTRADED": "VOLUME"}

    idx_cols = ["OPEN", "HIGH", "LOW", "CLOSE", "SHARESTRADED"]
    idx_final_cols = ["TIMESTAMP", "SYMBOL", "OPEN", "HIGH", "LOW", "CLOSE", "VOLUME"]
    vix_cols = ["OPEN", "HIGH", "LOW", "CLOSE", "PCLOSE", "CHANGE", "%CHANGE"]
    vix_col_rename = {"%CHANGE": "PERCENTAGE_CHANGE"}
    # nsedb
    engine = create_engine("postgresql://postgres@localhost:5432/nsedb")

    def __init__(self):
        pass

    @staticmethod
    def get_dates(start):
        end = date.today()
        df = pd.DataFrame(
            pd.date_range(start=start, end=end, freq="360D"), columns=["START"]
        )
        df = df.assign(END=df["START"].shift(-1) - pd.DateOffset(days=1))
        df["END"].iloc[-1] = datetime.fromordinal(end.toordinal())
        return df

    @staticmethod
    def get_next_update_start_date(table):
        dfd = pd.read_sql_table(
            table_name=table, con=nsedb.engine, columns=["TIMESTAMP"]
        )
        dfd = dfd.sort_values("TIMESTAMP", ascending=False).head(1)
        dates = pd.bdate_range(
            start=dfd["TIMESTAMP"].iloc[0], end=date.today(), closed="right"
        )
        try:
            ddt = dates[-1] - dates[0]
            if ddt.days > 360:
                print(f"{ddt.days} > 360 needs futher implementation")
            else:
                dts = pd.DataFrame(index=[0, 1])
                dts = dts.assign(START=dates[0])
                dts = dts.assign(END=dates[-1])
            return dts
        except:
            return None

    @staticmethod
    def check_table_exists(name):
        check = False
        if name in nsedb.engine.table_names():
            print(f"Historic data is already updated for {name} table.")
            check = True
        return check

    @staticmethod
    def get_csv_data(urlg, fix_cols):
        pg = requests.get(urlg, headers=nsedb.headers)
        content = pg.content
        if "No Records" not in content.decode():
            bsf = BeautifulSoup(content, "html5lib")
            csvc = bsf.find(name="div", attrs={"id": "csvContentDiv"})
            csvs = StringIO(csvc.text.replace(":", "\n"))
            df = pd.read_csv(csvs, error_bad_lines=False)
            cols = {}
            for x in df.columns:
                if "Date" in x:
                    cols.update({x: "TIMESTAMP"})
                elif "Prev" in x:
                    cols.update({x: "PCLOSE"})
                else:
                    cols.update({x: x.replace(" ", "").upper()})
            df = df.rename(columns=cols)
            df[fix_cols] = df[fix_cols].apply(
                lambda x: pd.to_numeric(x, errors="coerce").fillna(0)
            )
            df[fix_cols] = df[fix_cols].astype(np.float)
            df["TIMESTAMP"] = pd.to_datetime(df["TIMESTAMP"], dayfirst=True)
            return df
        else:
            return None

    @staticmethod
    def updateIndexData(dates, index="NIFTY%2050", symbol="NIFTY"):
        try:
            url = "https://www1.nseindia.com/products/dynaContent/equities/indices/historicalindices.jsp?indexType={index}&fromDate={start}&toDate={end}"
            dfs = []
            for x in dates.iterrows():
                urlg = url.format(
                    index=index,
                    start=x[1][0].strftime("%d-%m-%Y"),
                    end=x[1][1].strftime("%d-%m-%Y"),
                )
                print(urlg)
                dfi = nsedb.get_csv_data(urlg, nsedb.idx_cols)
                if dfi is not None:
                    dfs.append(dfi)

            if len(dfs) > 1:
                dfo = pd.concat(dfs)
            elif len(dfs) == 1:
                dfo = dfs[0]
            else:
                dfo = None

            if dfo is not None:
                dfo = dfo.rename(columns=nsedb.idx_col_rename)
                dfo["SYMBOL"] = symbol
            return dfo
        except Exception as e:
            print(urlg)
            print_exception(e)
            return None

    @staticmethod
    def update_index_for_dates(dates):
        try:
            dfn = nsedb.updateIndexData(dates, "NIFTY%2050", "NIFTY")
            dfbn = nsedb.updateIndexData(dates, "NIFTY%20BANK", "BANKNIFTY")
            if (dfn is not None) and (dfbn is not None):
                df = pd.concat([dfn, dfbn])
                df = df[nsedb.idx_final_cols]
                df.to_sql(
                    name="idx", con=nsedb.engine, if_exists="append",
                )
                print(f"Done index")
            else:
                print("Nothing to update for index...")
        except Exception as e:
            print_exception(e)

    @staticmethod
    def getHistoricalNiftyAndBankNifty():
        try:
            if nsedb.check_table_exists("idx"):
                res = sql.execute("DROP TABLE IF EXISTS idx", nsedb.engine)

            dates = nsedb.get_dates(start="1994-1-1")
            nsedb.update_index_for_dates(dates)
        except Exception as e:
            print_exception(e)

    @staticmethod
    def updateIndex_upto_date():
        try:
            dates = nsedb.get_next_update_start_date("idx")
            if dates is not None:
                nsedb.update_index_for_dates(dates)
            else:
                print("Nothing to update for index...")
        except Exception as e:
            print_exception(e)

    @staticmethod
    def get_vix(dates):
        try:
            # "https://www.nseindia.com/products/dynaContent/equities/indices/hist_vix_data.jsp?&fromDate=08-Oct-2018&toDate=31-Oct-2018"
            url = "https://www1.nseindia.com/products/dynaContent/equities/indices/hist_vix_data.jsp?&fromDate={start}&toDate={end}"
            dfs = []
            for x in dates.iterrows():
                urlg = url.format(
                    start=x[1][0].strftime("%d-%b-%Y"), end=x[1][1].strftime("%d-%b-%Y")
                )
                print(urlg)
                dfi = nsedb.get_csv_data(urlg, nsedb.vix_cols)
                if dfi is not None:
                    dfs.append(dfi)
            if len(dfs) > 1:
                dfo = pd.concat(dfs)
            elif len(dfs) == 1:
                dfo = dfs[0]
            else:
                dfo = None
            return dfo
        except Exception as e:
            print(url)
            print(urlg)
            print_exception(e)
            return None

    @staticmethod
    def getHistoricalVix():
        try:
            if not nsedb.check_table_exists("vix"):
                dates = nsedb.get_dates(start="2007-1-1")
                dfn = nsedb.get_vix(dates)
                dfn = dfn.rename(columns=nsedb.vix_col_rename)
                dfn.to_sql(
                    name="vix", con=nsedb.engine, if_exists="append",
                )
                print(f"Done vix")
        except Exception as e:
            print_exception(e)

    @staticmethod
    def updateVix_upto_Update():
        try:
            dates = nsedb.get_next_update_start_date("vix")
            if dates is not None:
                dfn = nsedb.get_vix(dates)
                if dfn is not None:
                    dfn = dfn.rename(columns=nsedb.vix_col_rename)
                    dfn.to_sql(
                        name="vix", con=nsedb.engine, if_exists="append",
                    )
                    print(f"Done vix")
                else:
                    print("Nothing to update for vix...")
            else:
                print("Nothing to update for vix...")
        except Exception as e:
            print_exception(e)

    @staticmethod
    def get_bhav_csv_data(dt, typ):
        print(f"Started {dt:%d-%b-%Y} ... ", end="")
        urls = None
        try:
            tp = typ.lower()

            urld = {
                "name": "",
                "type": "archives",
                "category": "",
                "section": "equity",
            }

            if tp == "eq":
                urld["name"] = "CM - Bhavcopy(csv)"
                urld["category"] = "capital-market"
            else:
                urld["name"] = "F&O - Bhavcopy(csv)"
                urld["category"] = "derivatives"

            urlp = urllib.parse.quote(f"[{json.dumps(urld, separators=(',',':'))}]".encode("utf-8"), safe="()")
            urls = (
                "https://beta.nseindia.com/api/reports?archives="
                f"{urlp}"
                f"&date={dt:%d-%b-%Y}"
            )
            urlap = requests.get(urls, headers=nsedb.headers)
            if urlap.reason == "OK":
                byt = BytesIO(urlap.content)
                zipfl = zipfile.ZipFile(byt)
                zinfo = zipfl.infolist()
                zipdata0 = zipfl.read(zinfo[0])
                zipfl1 = zipfile.ZipFile(BytesIO(zipdata0))
                zinfo1 = zipfl1.infolist()
                zipdata = zipfl1.read(zinfo1[0])
                zipstring = StringIO(zipdata.decode())
                try:
                    if tp == "fo":
                        pds = ["TIMESTAMP", "EXPIRY_DT"]
                    else:
                        pds = ["TIMESTAMP"]

                    dfp = pd.read_csv(
                        zipstring,
                        dtype=nsedb.fno_col_typ,
                        parse_dates=pds,
                        error_bad_lines=False,
                    )

                    if tp == "fo":
                        # Sometimes options type column is named as OPTIONTYPE instead of OPTION_TYP
                        if "OPTIONTYPE" in dfp.columns:
                            dfp = dfp.rename(columns={"OPTIONTYPE": "OPTION_TYP"})
                        return dfp[nsedb.fno_cols]
                    else:
                        dfp = dfp[dfp["SERIES"] == "EQ"]
                        return dfp[nsedb.eq_cols]
                except Exception as e:
                    msg = f"Error processing {dt:%d-%b-%Y}"
                    print_exception(msg)
                    print_exception(e)
                    return None
            else:
                print(f"Failed at url {dt:%d-%b-%Y}")
                print(urls)
                return None
        except Exception as e:
            print_exception(e)
            print(f"Error getting data for date {dt:%d-%b-%Y}")
            print(f"{urls}")

    @staticmethod
    def updateFNOBhavData_upto_date():
        try:
            dfd = pd.read_sql(
                sql=(
                    'SELECT DISTINCT "TIMESTAMP" '
                    "FROM fno_nifty "
                    'ORDER BY "TIMESTAMP" DESC '
                    "LIMIT 1"
                ),
                con=nsedb.engine,
            )
            df = pd.bdate_range(
                start=dfd["TIMESTAMP"].iloc[0], end=date.today(), closed="right"
            )
            if len(df) > 0:
                nsedb.updateFNOBhavData_for_given_dates(df)
                print("Done updating FNO...")
            else:
                print("Nothing to update for FNO...")
        except Exception as e:
            print_exception(e)

    @staticmethod
    def updateEQBhavData_upto_date():
        try:
            dfd = pd.read_sql(
                sql=(
                    'SELECT DISTINCT "TIMESTAMP" '
                    "FROM spot "
                    'ORDER BY "TIMESTAMP" DESC '
                    "LIMIT 1"
                ),
                con=nsedb.engine,
            )
            df = pd.bdate_range(
                start=dfd["TIMESTAMP"].iloc[0], end=date.today(), closed="right"
            )
            if len(df) > 0:
                nsedb.updateEQBhavData_for_given_dates(df)
                print("Done updating EQ...")
            else:
                print("Nothing to update for EQ...")
        except Exception as e:
            print_exception(e)

    @staticmethod
    def updateHistoricFNOBhavData(end_date):
        """
        Do not call this function as this function tries to update the db since 2000-6-12
        """
        try:
            df = pd.bdate_range(start="2000-6-12", end=end_date).sort_values(
                ascending=False
            )
            nsedb.updateFNOBhavData_for_given_dates(df)
        except Exception as e:
            print_exception(e)

    @staticmethod
    def updateEQBhavData_for_given_dates(dates):
        df = dates
        for d in df:
            nsedb.updateEQBhavData_for_given_date(d)

    @staticmethod
    def updateFNOBhavData_for_given_dates(dates):
        df = dates
        for d in df:
            nsedb.updateFNOBhavData_for_given_date(d)

    @staticmethod
    def updateFNOBhavData_for_given_date(d, force_update=False):
        dt = None

        try:
            if isinstance(d, datetime):
                dt = datetime.combine(d, datetime.min.time())
            elif isinstance(d, str):
                dt = parser.parse(d)
        except Exception as e:
            print(f"Error processing input date {d}")
            print_exception(e)

        try:
            dfd = pd.read_sql(
                sql=f'SELECT "TIMESTAMP" FROM fno_nifty WHERE "SYMBOL"=\'NIFTY\' AND "INSTRUMENT"=\'FUTIDX\' AND "TIMESTAMP"=\'{dt}\'',
                con=nsedb.engine,
            )
            if len(dfd) == 0:
                dfd = None
            elif force_update:
                raise Exception("Force exception is not yet implemented.")
        except:
            dfd = None

        if dfd is None:
            dfc = nsedb.get_bhav_csv_data(dt, "fo")
            if dfc is not None:
                try:
                    dfc = dfc.reset_index(drop=True)
                    dfs = dfc[~(dfc["SYMBOL"] == "NIFTY")]
                    dfs.to_sql(
                        name="fno", con=nsedb.engine, if_exists="append",
                    )
                    dfn = dfc[(dfc["SYMBOL"] == "NIFTY")]
                    dfn.to_sql(
                        name="fno_nifty", con=nsedb.engine, if_exists="append",
                    )
                    dfb = dfc[(dfc["SYMBOL"] == "BANKNIFTY")]
                    dfb.to_sql(
                        name="fno_banknifty", con=nsedb.engine, if_exists="append",
                    )
                    print(f"Done fno {dt:%d%b%Y}")
                except Exception as e:
                    print(f"Error saving data to database {dt:%d%b%Y}")
                    dfc.to_excel(f"{dt:%d-%b-%Y}.xlsx")
                    print_exception(f"Error in processing {dt:%d-%b-%Y}")
                    print_exception(e)
        else:
            print(f"Data already updated for given date {dt:%d-%b-%Y}")

    @staticmethod
    def updateEQBhavData_for_given_date(d, force_update=False):
        dt = None

        try:
            if isinstance(d, datetime):
                dt = datetime.combine(d, datetime.min.time())
            elif isinstance(d, str):
                dt = parser.parse(d)
        except Exception as e:
            print(f"Error processing input date {d}")
            print_exception(e)

        try:
            dfd = pd.read_sql(
                sql=f'SELECT "TIMESTAMP" FROM spot WHERE "TIMESTAMP"=\'{dt}\'',
                con=nsedb.engine,
            )
            if len(dfd) == 0:
                dfd = None
            elif force_update:
                raise Exception("Force exception is not yet implemented.")
        except:
            dfd = None

        if dfd is None:
            dfc = nsedb.get_bhav_csv_data(dt, "eq")
            if dfc is not None:
                try:
                    dfc = dfc.reset_index(drop=True)
                    dfc.to_sql(
                        name="spot", con=nsedb.engine, if_exists="append",
                    )
                    print(f"Done EQ {dt:%d%b%Y}")
                except Exception as e:
                    print(f"Error saving data to database {dt:%d%b%Y}")
                    dfc.to_excel(f"{dt:%d-%b-%Y}.xlsx")
                    print_exception(f"Error in processing {dt:%d-%b-%Y}")
                    print_exception(e)
        else:
            print(f"Data already updated for given date {dt:%d-%b-%Y}")

    @staticmethod
    def updateFNOBhavData_between_dates(start, end):
        dates = pd.bdate_range(start=start, end=end, closed="right")
        nsedb.updateFNOBhavData_for_given_dates(dates)

    @staticmethod
    def updateEQBhavData_between_dates(start, end):
        dates = pd.bdate_range(start=start, end=end, closed="right")
        nsedb.updateEQBhavData_for_given_dates(dates)


if __name__ == "__main__":
    nsedb.updateEQBhavData_upto_date()
    nsedb.updateFNOBhavData_upto_date()
    nsedb.updateIndex_upto_date()
    nsedb.updateVix_upto_Update()
