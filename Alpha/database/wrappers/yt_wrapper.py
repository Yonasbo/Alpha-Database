import datetime
import yfinance as yf


def get_ohlcv(ticker='' ,period_start = None,period_end = datetime.datetime.today(), period_days = 3650):
    
    period_start= period_start if period_start else period_end-datetime.timedelta(days=period_days)
    data = yf.Ticker(ticker).history(start=period_start,end=period_end,interval='1d').reset_index().rename(columns={'Date':'datetime'})
    data['datetime'] = data['datetime'].dt.tz_localize(None) 
    return data
