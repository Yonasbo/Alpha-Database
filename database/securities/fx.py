
import datetime
import numpy as np
import pandas as pd

import db_logs


import wrappers.yt_wrapper as yt_wrapper

class FX():

    def __init__(self, data_clients={}, db_service=None):
        self.data_clients = data_clients
        self.fred_client = data_clients['fred_client']
        self.db_service = db_service

    def _get_series_identifiers_and_metadata(self, ticker, exchange='FOREX', source='yfinance'):
        series_metadata ={
            'ticker': ticker,
            'exchange': exchange,
            'source': source
        }
        series_identifier = {**{'type':'ticker_series'}, **series_metadata}
        return series_metadata , series_identifier

    def get_ohlcv(self, ticker, exchange='FOREX', period_end=datetime.datetime.today(), period_start=None,
                period_days=3650, read_db=True,insert_db=True):
        series_df = None
        try:
            
            #id_codes = self.get_identification_codes(ticker=ticker, exchange=exchange)
            series_metadata, series_identifier = self._get_series_identifiers_and_metadata(
                                                                                        ticker=ticker,
                                                                                        exchange=exchange,
                                                                                        source='yfinance')
            if read_db:
                period_start = period_start if period_start else period_end - datetime.timedelta(days=period_days)
                
                exists, series_df = self.db_service.read_timeseries(
                                                                dtype='forex',
                                                                dformat='spot',
                                                                dfreq='1d',
                                                                series_metadata=series_metadata,
                                                                series_identifier = series_identifier,
                                                                metalogs = ticker,
                                                                period_start = period_start,
                                                                period_end = period_end)
                
            if not read_db or not exists:
                
                series_df = yt_wrapper.get_ohlcv(
                            ticker = ticker+"=X",
                            period_start = period_start,
                            period_end = period_end,
                            period_days = period_days
                )                          
                if not insert_db:
                    db_logs.DBLogs().info('successful get_ohlcv but not inserted {}'.format(ticker))
                elif insert_db and len(series_df)>0:
                    self.db_service.insert_timeseries_df(
                                                        dtype='forex',
                                                        dformat='spot',
                                                        dfreq='1d',
                                                        df= series_df,
                                                        series_identifier = series_identifier,
                                                        series_metadata=series_metadata,
                                                        metalogs = ticker)
                    db_logs.DBLogs().info('successful get_ohlcv with db write {}'.format(ticker))
                elif insert_db and len(series_df)==0:
                    db_logs.DBLogs().info('successful get_ohlcv but skipped with len 0 insert {}'.format(ticker))
                else:
                    pass
        except Exception:
            db_logs.DBLogs().critical('get_ohlcv FAILED {}'.format(ticker))
        return series_df