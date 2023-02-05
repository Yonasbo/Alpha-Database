import pandas as pd
import os
import time
from datetime import datetime
import pytz
import asyncio
import numpy as np
import yfinance
from cvxpy import *
import numpy_ext as npext
import matplotlib.pyplot as plt
import statsmodels.api as sm
import json
from dateutil import parser

def load_data_fx(daily_range,tickers,just_C=True):
    fx_datas = {}
    for i in tickers:
        data = yfinance.Ticker(i + "=X").history(start=daily_range[0],end=daily_range[-1],interval='1wk')
        if just_C:
            fx_datas[i]=data.drop(['Open','High','Low','Volume','Dividends','Stock Splits'],axis=1)
        else:
            fx_datas[i]=data
    return fx_datas

def load_data_eq(daily_range,tickers,just_C=True):
    eq_datas = {}
    for i in tickers:
        data = yfinance.Ticker(i).history(start=daily_range[0],end=daily_range[-1],interval='1wk')
        if just_C:
            eq_datas[i]=data.drop(['Open','High','Low','Volume','Dividends','Stock Splits'],axis=1)
        else:
            eq_datas[i]=data
    return eq_datas