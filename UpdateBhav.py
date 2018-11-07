import pandas as pd
import numpy as np
from datetime import datetime
import requests
import zipfile
from io import BytesIO
from io import StringIO
import time
from sqlalchemy import create_engine
import sys, os

class UpdateBhav(object):
    def __init__(self):
        self.db = create_engine('sqlite:///bhav.db')
        self.mainDataFrame = None
        self.dateRange = None
        self.lastDF = None
        self.lastText = None
        self.lastUrl1 = None
        self.lastPage1 = None
        self.lastUrl2 = None
        self.lastPage2 = None
        self.STOCKS = 'stocks'
        self.FNO = 'fno'
        self.IINDEX = 'iindex'
        self.headers = {'Host':'www.nseindia.com'}
        self.headers['User-Agent'] = 'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:56.0) Gecko/20100101 Firefox/56.0'
        self.headers['Accept'] = '*/*'
        self.headers['Accept-Language']='en-US,en;q=0.5'
        self.headers['Accept-Encoding']='gzip, deflate, br'
        self.headers['Referer']='https://www.nseindia.com/products/content/equities/equities/archieve_eq.htm'
        self.headers['Connection']='keep-alive'