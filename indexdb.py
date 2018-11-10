import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
import requests
import zipfile
from io import BytesIO
from io import StringIO
import time
from sqlalchemy import create_engine
import sys, os
from log import print_exception
from bs4 import BeautifulSoup

class indexdb(object):
    '''Creates and updates database of index Nifty and BankNifty since 1994'''
    headers = {'Host':'www.nseindia.com'}
    headers['User-Agent'] = 'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:56.0) Gecko/20100101 Firefox/56.0'
    headers['Accept'] = '*/*'
    headers['Accept-Language']='en-US,en;q=0.5'
    headers['Accept-Encoding']='gzip, deflate, br'
    headers['Referer']='https://www.nseindia.com/products/content/equities/equities/archieve_eq.htm'
    headers['Connection']='keep-alive'

    fno_col_typ = {
    'STRIKE_PR':np.float, 
    'OPEN':np.float, 
    'HIGH':np.float, 
    'LOW':np.float, 
    'CLOSE':np.float, 
    'SETTLE_PR':np.float, 
    'CONTRACTS':np.float, 
    'VAL_INLAKH':np.float, 
    'OPEN_INT':np.float, 
    'CHG_IN_OI':np.float,
    }

    idx_col_typ = {
    'OPEN':np.float,
    'HIGH':np.float,
    'LOW':np.float,
    'CLOSE':np.float,
    'SHARESTRADED':np.float,
    'TURNOVER(RS.CR)':np.float
    }

    vix_col_typ = {
    'OPEN':np.float,
    'HIGH':np.float,
    'LOW':np.float,
    'CLOSE':np.float,
    'PCLOSE':np.float,
    'CHANGE':np.float,
    '%CHANGE':np.float
    }

    fno_cols = ['TIMESTAMP',
                'INSTRUMENT',
                'SYMBOL',
                'EXPIRY_DT',
                'STRIKE_PR',
                'OPTION_TYP',
                'OPEN',
                'HIGH',
                'LOW',
                'CLOSE',
                'SETTLE_PR',
                'CONTRACTS',
                'VAL_INLAKH',
                'OPEN_INT',
                'CHG_IN_OI']

    idx_col_rename = {
    'SHARESTRADED':'VOLUME',
    'TURNOVER(RS.CR)':'TURNOVER'
    }

    stk_col_type = {'OPEN':np.float,
                    'HIGH':np.float,
                    'LOW':np.float,
                    'CLOSE':np.float,
                    'LAST':np.float,
                    'PREVCLOSE':np.float,
                    'TOTTRDQTY':np.float,
                    'TOTTRDVAL':np.float,
                    'TOTALTRADES':np.float}

    idx_cols = ['OPEN', 'HIGH', 'LOW', 'CLOSE', 'SHARESTRADED', 'TURNOVER(RS.CR)']
    vix_cols = ['OPEN', 'HIGH', 'LOW', 'CLOSE', 'PCLOSE', 'CHANGE', '%CHANGE']
    stk_cols = ['SYMBOL', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'LAST', 'PREVCLOSE', 'TOTTRDQTY', 'TOTTRDVAL', 'TIMESTAMP', 'TOTALTRADES']

    def __init__(self):
        pass
    
    @staticmethod
    def get_dates(start):
        end = date.today()
        df = pd.DataFrame(pd.date_range(start=start, end=end, freq='360D'), columns=['START'])
        df = df.assign(END=df['START'].shift(-1) - pd.DateOffset(days=1))  
        df['END'].iloc[-1] = datetime.fromordinal(end.toordinal())
        return df

    @staticmethod
    def check_table_exists(name):
        check = False
        with pd.HDFStore('indexdb.hdf') as store:
            names = [g._v_name for g in store.groups()]
            if name in names:
                print(f'Historic data is already updated for {name} table.')
                check = True
        return check
            
    @staticmethod
    def get_csv_data(urlg, fix_cols):
        pg = requests.get(urlg, headers=indexdb.headers)
        content = pg.content
        if 'No Records' not in content.decode():
            bsf = BeautifulSoup(content, 'html5lib')
            csvc=bsf.find(name='div', attrs={'id':'csvContentDiv'})
            csvs = StringIO(csvc.text.replace(':', '\n'))
            df = pd.read_csv(csvs, error_bad_lines=False)
            cols = {}
            for x in df.columns:
                if 'Date' in x:
                    cols.update({x:'TIMESTAMP'})
                elif 'Prev' in x:
                    cols.update({x:'PCLOSE'})
                else:
                    cols.update({x:x.replace(' ', '').upper()})
            df = df.rename(columns=cols)
            df[fix_cols] = df[fix_cols].apply(lambda x: pd.to_numeric(x, errors='coerce').fillna(0))
            df[fix_cols] = df[fix_cols].astype(np.float)
            df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'], dayfirst=True)
            return df
        else:
            return None

    @staticmethod
    def updateIndexData(dates, index='NIFTY%2050', symbol='NIFTY'):
        try:
            url = 'https://www.nseindia.com/products/dynaContent/equities/indices/historicalindices.jsp?indexType={index}&fromDate={start}&toDate={end}'
            dfs=[]
            for x in dates.iterrows():
                urlg = url.format(index=index, start=x[1][0].strftime('%d-%m-%Y'), end=x[1][1].strftime('%d-%m-%Y'))
                dfi = indexdb.get_csv_data(urlg, indexdb.idx_cols)
                if dfi is not None:
                    dfs.append(dfi)
            
            if len(dfs) > 1:
                dfo = pd.concat(dfs)
            elif len(dfs) == 1:
                dfo = dfs[0]
            else:
                dfo = None

            if dfo is not None:
                dfo = dfo.rename(columns=indexdb.idx_col_rename)
                dfo['SYMBOL']=symbol
            
            return dfo
        except Exception as e:
            print(url)
            print(urlg)
            print_exception(e)
            return None

    @staticmethod
    def getHistoricalNiftyAndBankNifty():
        try:
            if not indexdb.check_table_exists('idx'):
                dates = indexdb.get_dates(start='1994-1-1')
                dfn = indexdb.updateIndexData(dates, "NIFTY%2050", 'NIFTY')
                dfbn = indexdb.updateIndexData(dates, "NIFTY%20BANK", 'BANKNIFTY')
                df = pd.concat([dfn, dfbn])
                df.to_hdf('indexdb.hdf', 'idx', mode='a', append=True, format='table', data_columns=True)
                print(f'Done idx')
            return df
        except Exception as e:
            print_exception(e)

    @staticmethod
    def updateIndex_upto_date():
        try:
            dfd = pd.read_hdf('indexdb.hdf', 'idx', columns=['TIMESTAMP'])
            dfd = dfd.sort_values('TIMESTAMP', ascending=False).head(1) + timedelta(days=1)
            dates = indexdb.get_dates(start=dfd['TIMESTAMP'].iloc[0])
            dfn = indexdb.updateIndexData(dates, "NIFTY%2050", 'NIFTY')
            dfbn = indexdb.updateIndexData(dates, "NIFTY%20BANK", 'BANKNIFTY')
            if ((dfn is not None) and (dfbn is not None)):
                df = pd.concat([dfn, dfbn])
                df.to_hdf('indexdb.hdf', 'idx', mode='a', append=True, format='table', data_columns=True)
                print(f'Done idx')
            else:
                print('Nothing to update for index')
        except Exception as e:
            print_exception(e)

    @staticmethod
    def get_vix(dates):
        try:
            URL='https://www.nseindia.com/products/dynaContent/equities/indices/hist_vix_data.jsp?&fromDate=08-Oct-2018&toDate=31-Oct-2018'
            url='https://www.nseindia.com/products/dynaContent/equities/indices/hist_vix_data.jsp?&fromDate={start}&toDate={end}'
            dfs = []
            for x in dates.iterrows():
                urlg = url.format(start=x[1][0].strftime('%d-%b-%Y'), end=x[1][1].strftime('%d-%b-%Y'))
                dfi = indexdb.get_csv_data(urlg, indexdb.vix_cols)
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
            if not indexdb.check_table_exists('vid'):
                dates = indexdb.get_dates(start='1994-1-1')
                dfn = indexdb.get_vix(dates)
                dfn.to_hdf('indexdb.hdf', 'vix', mode='a', append=True, format='table', data_columns=True)
                print(f'Done vix')
        except Exception as e:
            print_exception(e)

    @staticmethod
    def updateVix_upto_Update():
        try:
            dfd = pd.read_hdf('indexdb.hdf', 'vix', columns=['TIMESTAMP'])
            dfd = dfd.sort_values('TIMESTAMP', ascending=False).head(1) + timedelta(days=1)
            dates = indexdb.get_dates(start=dfd['TIMESTAMP'].iloc[0])
            dfn = indexdb.get_vix(dates)
            if dfn is not None:
                dfn.to_hdf('indexdb.hdf', 'vix', mode='a', append=True, format='table', data_columns=True)
                print(f'Done vix')
            else:
                print('Nothing to update for vix')
        except Exception as e:
            print_exception(e)

    @staticmethod
    def get_fno_csv_data(dt):
        action_url = 'https://www.nseindia.com/ArchieveSearch?h_filetype=fobhav&date={date}&section=FO'
        download_url = 'https://www.nseindia.com/content/historical/DERIVATIVES/{year}/{month}/fo{date}bhav.csv.zip'
        url = action_url.format(date=dt.strftime('%d-%m-%Y'))
        urlp = requests.get(url, headers=indexdb.headers)
        if not "No file found" in urlp.content.decode():
            urls = download_url.format(year=dt.year, month= dt.strftime('%b').upper(), date=dt.strftime('%d%b%Y').upper())
            urlap = requests.get(urls, headers=indexdb.headers)
            if urlap.reason == 'OK':
                byt = BytesIO(urlap.content)
                zipfl = zipfile.ZipFile(byt)
                zinfo = zipfl.infolist()
                zipdata = zipfl.read(zinfo[0])
                zipstring = StringIO(zipdata.decode())
                try:
                    dfp = pd.read_csv(zipstring, dtype=indexdb.fno_col_typ, parse_dates=['TIMESTAMP', 'EXPIRY_DT'], error_bad_lines=False)
                    # Sometimes options type column is named as OPTIONTYPE instead of OPTION_TYP
                    if 'OPTIONTYPE' in dfp.columns:
                        dfp = dfp.rename(columns={'OPTIONTYPE':'OPTION_TYP'})
                    return dfp[indexdb.fno_cols]
                except Exception as e:
                    msg = f'Error processing {dt:%d-%b-%Y}'
                    print_exception(msg)
                    print_exception(e)
                    return None
            else:
                print(f'Failed at second url {dt:%d-%b-%Y}')
                print(url)
                print(urls)
                return None
        else:
            print(f'File not found {dt:%d-%b-%Y}')
            print(url)
            return None

    @staticmethod
    def updateFNOBhavData_upto_date():
        try:
            dfd = pd.read_hdf('indexdb.hdf', 'fno', where="SYMBOL==NIFTY & INSTRUMENT==FUTIDX", columns=['TIMESTAMP'])
            dfd = dfd.sort_values('TIMESTAMP', ascending=False).head(1)
            df = pd.bdate_range(start=dfd['TIMESTAMP'].iloc[0], end=date.today(), closed='right')
            indexdb.updateFNOBhavData_for_given_dates(df)
        except Exception as e:
            print_exception(e)
    
    @staticmethod
    def updateHistoricFNOBhavData(end_date):
        '''
        Do not call this function as this function tries to update the db since 2000-6-12
        '''
        try:
            if not indexdb.check_table_exists('fno'):
                end = end_date
                df = pd.bdate_range(start='2000-6-12', end=end).sort_values(ascending=False)
                indexdb.updateFNOBhavData_for_given_dates(df)
        except Exception as e:
            print_exception(e)

    @staticmethod
    def updateFNOBhavData_for_given_dates(dates):
        df = dates
        for d in df:
            dfd = pd.read_hdf('indexdb.hdf', 'fno', where="SYMBOL==NIFTY & INSTRUMENT==FUTIDX & TIMESTAMP==d", columns=['TIMESTAMP'])
            if ((dfd is None) or (len(dfd) == 0)):
                dfc = indexdb.get_fno_csv_data(d)
                if dfc is not None:
                    try:
                        dfc = dfc.reset_index(drop=True)
                        dfc.to_hdf('indexdb.hdf', 'fno', mode='a', append=True, format='table', data_columns=True)
                        print(f'Done fno {d:%d%b%Y}')
                    except Exception as e:
                        print(f'Error saving data to hdf {d:%d%b%Y}')
                        dfc.to_excel(f'{d:%d-%b-%Y}.xlsx')
                        print_exception(f'Error in processing {d:%d-%b-%Y}')
                        print_exception(e)
            else:
                print(f"Data already updated for given date {d:%d-%b-%Y}")

    @staticmethod
    def get_stk_csv_data(dt):
        action_url = 'https://www.nseindia.com/ArchieveSearch?h_filetype=eqbhav&date={date}&section=EQ'
        download_url = 'https://www.nseindia.com/content/historical/EQUITIES/{year}/{month}/cm{date}bhav.csv.zip'
        #user_url = 'https://www.nseindia.com/products/content/equities/equities/archieve_eq.htm'
        dtt = dt.strftime('%d-%m-%Y')
        url = action_url.format(date=dtt)
        lastPage1 = requests.get(url, headers=indexdb.headers)
        if not "No file found" in lastPage1.content.decode():
            urls = download_url.format(year=dt.year, month= dt.strftime('%b').upper(), date=dt.strftime('%d%b%Y').upper())
            lastPage2 = requests.get(urls, headers=indexdb.headers)
            if lastPage2.reason == 'OK':
                byt = BytesIO(lastPage2.content)
                zipfl = zipfile.ZipFile(byt)
                zinfo = zipfl.infolist()
                zipdata = zipfl.read(zinfo[0])
                zipstring = StringIO(zipdata.decode())
                try:
                    dfp = pd.read_csv(zipstring, dtype=indexdb.stk_col_type, parse_dates=['TIMESTAMP'], error_bad_lines=False)
                    dfp = dfp[dfp['SERIES'] == 'EQ']
                    if 'TOTALTRADES' not in dfp.columns:
                        dfp = dfp.assign(TOTALTRADES=0.0)
                    dfp = dfp[indexdb.stk_cols]
                    return dfp
                except Exception as e:
                    msg = f'Error processing {dt:%d-%b-%Y}'
                    print_exception(msg)
                    print_exception(e)
                    return None
            else:
                print(f'Failed at second url {dt:%d-%b-%Y}')
                print(url)
                print(urls)
                return None
        else:
            print(f'File not found {dt:%d-%b-%Y}')
            print(url)
            return None
    
    @staticmethod
    def updateHistoricSTKBhavData(end_date):
        '''
        Do not call this function as this function tries to update the db since 1994-1-1
        '''
        try:
            if not indexdb.check_table_exists('stk'):
                end = end_date
                df = pd.bdate_range(start='1994-1-1', end=end).sort_values(ascending=False)
                indexdb.updateSTKBhavData_for_given_dates(df)
        except Exception as e:
            print_exception(e)

    @staticmethod
    def updateSTKBhavData_for_given_dates(dates):
        df = dates
        for d in df:
            dfc = None
            try:
                dfd = pd.read_hdf('indexdb.hdf', 'stk', where="TIMESTAMP==d", columns=['TIMESTAMP'])
                if ((dfd is None) or (len(dfd) == 0)):
                    dfc = indexdb.get_stk_csv_data(d)
                else:
                    print(f"Data already updated for given date {d:%d-%b-%Y}")
            except Exception as e:
                if "No object named stk in the file" in str(e):
                    dfc = indexdb.get_stk_csv_data(d)
                else:
                    print_exception(e)
            
            if dfc is not None:
                try:
                    dfc = dfc.reset_index(drop=True)
                    dfc.to_hdf('indexdb.hdf', 'stk', mode='a', append=True, format='table', data_columns=True)
                    print(f'Done stk {d:%d%b%Y}')
                except Exception as e:
                    print(f'Error saving data to stk hdf {d:%d%b%Y}')
                    dfc.to_excel(f'{d:%d-%b-%Y}.xlsx')
                    print_exception(f'Error in processing {d:%d-%b-%Y}')
                    print_exception(e)


    @staticmethod
    def updateSTKBhavData_upto_date():
        try:
            dfd = pd.read_hdf('indexdb.hdf', 'stk', columns=['TIMESTAMP'])
            dfd = dfd.sort_values('TIMESTAMP', ascending=False).head(1)
            df = pd.bdate_range(start=dfd['TIMESTAMP'].iloc[0], end=date.today(), closed='right')
            indexdb.updateSTKBhavData_for_given_dates(df)
        except Exception as e:
            print_exception(e)

if __name__ == '__main__':
    indexdb.updateFNOBhavData_upto_date()
    indexdb.updateIndex_upto_date()
    indexdb.updateVix_upto_Update()
    indexdb.updateSTKBhavData_upto_date()