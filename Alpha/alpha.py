import numpy as np 
import pandas as pd
from numba import jit
import backtest

from copy import deepcopy
from database import general_util
from database.general_util import timeme

class Alpha():

    def __init__(self, instruments, dfs, configs):
        self.instruments = instruments
        self.dfs = deepcopy(dfs)
        self.configs = configs
        self.portfolio_df = None

    def get_trade_datetime_range(self):
        return (self.configs['start'], self.configs['end'])


    def compute_metas(self, index):


        @jit(nopython=True)
        def numba_any(x):
            return int(np.any(x))
        
        vols , rets , actives , closes , fxconvs = [] , [] , [] , [] , []


        for inst in self.instruments:
            df = pd.DataFrame(index=index)
            self.dfs[inst]['vol'] = (-1 + self.dfs[inst]['close']/self.dfs[inst].shift(1)['close']).rolling(30).std()
            #print(self.dfs[inst])
            self.dfs[inst] = df.join(self.dfs[inst])
            #print(self.dfs[inst])
            self.dfs[inst] = self.dfs[inst].fillna(method='ffill').fillna(method='bfill')
            self.dfs[inst]['ret']= -1 + self.dfs[inst]['close']/self.dfs[inst].shift(1)['close']
            self.dfs[inst]['sampled'] = self.dfs[inst]['close']!=self.dfs[inst].shift(1)['close']
            self.dfs[inst]['active'] = self.dfs[inst]['sampled'].rolling(5).apply(numba_any ,engine ="numba", raw = True).fillna(0)
            
            
            
            vols.append(self.dfs[inst]['vol'])
            rets.append(self.dfs[inst]['ret'])
            actives.append(self.dfs[inst]['active'])
            closes.append(self.dfs[inst]['close'])
    
        for inst in self.instruments:
            if inst[-3:] == 'USD':
                fxconvs.append(pd.Series(index=index,data=np.ones(len(index))))
            elif inst[-3:] + 'USD%USD' in self.dfs:
                fxconvs.append(self.dfs [inst[-3:] + "USD%USD"]["close"])
            elif 'USD' + inst[-3:] + '%' + inst[-3:] in self.dfs:
                fxconvs.append(1/ self.dfs['USD' + inst[-3:] + '%' + inst[-3:]]['close'])
            else:
                print('NO SOlution', inst,inst[-3:])
                exit()
        self.voldf = pd.concat(vols,axis=1)
        self.voldf.columns = self.instruments
        self.retdf = pd.concat(rets , axis =1)
        self.retdf.columns = self.instruments
        self.activedf = pd.concat(actives , axis =1)
        self.activedf.columns = self.instruments
        closedf = pd.concat(closes , axis =1)
        closedf.columns = self.instruments
        fxconvsdf = pd.concat(fxconvs , axis =1)
        fxconvsdf.columns = self.instruments
        self.baseclosedf = fxconvsdf * closedf
        pass
        


    def compute_forecasts(self,index,date):
        pass

    def post_risk_management(self, index , date , eligibles , nominal_tot , positions , weights):
        return nominal_tot, positions,weights

    def init_portfolio_settings(self, trade_range):
        self.portfolio_df = pd.DataFrame(index=trade_range).reset_index().rename(columns={'index':'datetime'})
        
        return 10000,0.001,1,self.portfolio_df
    
    def compute_eligibles(self, date):
        eligibles = [inst for inst in self.instruments if self.dfs[inst].at[date,'eligible']]
        non_eligibles = [inst for inst in self.instruments if not self.dfs[inst].at[date,'eligible']]
        return eligibles, non_eligibles
    
    def get_strat_scalar(self, lookback, portfolio_vol, idx, default,ewmas,ewstrats):
        #print(idx)
        ann_realized_vol = np.sqrt(ewmas[-1]*252)
        return portfolio_vol/ann_realized_vol*ewstrats[-1]

    def set_positions(self, capital, portfolio_i, forecasts,
                    eligibles, num_trading, portfolio_vol, strat_scalar,
                    invrisk_row, baseclose_row):
        vol_target = 1.0/num_trading*capital*portfolio_vol/np.sqrt(253)
        positions = eligibles*strat_scalar*vol_target*forecasts*invrisk_row/baseclose_row
        positions = np.nan_to_num(positions,nan=0,posinf=0,neginf=0)
        nominal_tot = np.linalg.norm(positions*baseclose_row,ord=1)
        return positions, nominal_tot

    def set_weights(self, nominal_tot, positions, baseclose_row):
        nominals = positions * baseclose_row
        weights = np.nan_to_num(nominals/nominal_tot,nan=0,posinf=0,neginf=0)
        return weights

    @timeme
    def run_simulation(self, verbose=False):

        portfolio_vol = 0.4
        trade_datetime_range = pd.date_range(start=self.get_trade_datetime_range()[0],
                                            end=self.get_trade_datetime_range()[1],freq='D')

        self.compute_metas(index=trade_datetime_range)

        capital,ewma, ewstrat,self.portfolio_df = self.init_portfolio_settings(trade_range=trade_datetime_range)

        date_prev = None
        baseclose_prev = None
        capitals = [capital]
        ewmas = [ewma]
        ewstrats = [ewstrat]
        nominalss = []
        leverages = []
        strat_scalars = []
        units_held = []
        weights_held = []

        for (portfolio_i,portfolio_row),(ret_i,ret_row),(baseclose_i,baseclose_row),\
            (eligibles_i,eligibles_row),(invrisk_i,invrisk_row) in \
            zip(self.portfolio_df.iterrows(),self.retdf.iterrows(),self.baseclosedf.iterrows(),\
            self.eligiblesdf.iterrows(),self.invriskdf.iterrows()):
            portfolio_row = portfolio_row.values
            ret_row = ret_row.values
            baseclose_row = baseclose_row.values
            eligibles_row = eligibles_row.values
            invrisk_row = invrisk_row.values

            strat_scalar = 2
            #print(portfolio_i)
            if portfolio_i != 0:
                strat_scalar = self.get_strat_scalar(lookback=30, portfolio_vol=portfolio_vol,
                                                    idx=portfolio_i, default=strat_scalars[-1],
                                                    ewmas=ewmas,ewstrats=ewstrats)
                capitals, nominal_ret,ewmas = backtest.get_pnl_stats(
                    portfolio_df=self.portfolio_df,
                    last_weights= weights_held[-1],
                    last_units=units_held[-1],
                    idx=portfolio_i,
                    baseclose_row=baseclose_prev,
                    ret_row=ret_row,
                    capitals=capitals,
                    leverages=leverages,
                    ewmas=ewmas
                )
                
                ewstrats.append(0.06*strat_scalar + .94*ewstrats[-1] if nominal_ret!=0 else ewstrats[-1])

            strat_scalars.append(strat_scalar)

            forecasts , num_trading = self.compute_forecasts(
                portfolio_i=portfolio_i,
                date=ret_i,
                eligibles_row=eligibles_row
            )
            positions , nominal_tot = self.set_positions(
                capital=capitals[-1],
                portfolio_i= portfolio_i,
                forecasts=forecasts,
                eligibles=eligibles_row,
                num_trading=num_trading,
                portfolio_vol=portfolio_vol,
                strat_scalar=strat_scalars[-1],
                invrisk_row=invrisk_row,
                baseclose_row=baseclose_row
            )
            
            weights = self.set_weights(nominal_tot, positions, baseclose_row)
            
            nominal_tot, positions, weights = self.post_risk_management(
                index=portfolio_i,
                date=ret_i,
                eligibles=eligibles_row,
                nominal_tot=nominal_tot,
                positions=positions,
                weights=weights
            )
            
            date_prev = portfolio_i
            baseclose_prev = baseclose_row
            nominalss.append(nominal_tot)
            leverages.append(nominal_tot/capitals[-1])
            units_held.append(positions)
            weights_held.append(weights)

        if verbose:
            capital_ser = pd.Series(data=capitals,index=trade_datetime_range,name='capital')
            stratscal_ser = pd.Series(data=strat_scalars,index=trade_datetime_range,name='strat_scalar')
            nominals_ser = pd.Series(data=nominalss,index=trade_datetime_range,name='nominal_tot')
            leverages_ser = pd.Series(data=leverages,index=trade_datetime_range,name='leverage')
            units = pd.DataFrame(data=units_held, index=trade_datetime_range, 
                                        columns=[inst+' units' for inst in self.instruments])
            weights = pd.DataFrame(data=weights_held, index=trade_datetime_range, 
                                        columns=[inst+' w' for inst in self.instruments])
            
            self.portfolio_df = pd.concat([
                units,
                weights,
                stratscal_ser,
                nominals_ser,
                leverages_ser,
                capital_ser
            ], axis=1)
            print(self.portfolio_df)
        else:
            print(capitals)
        return self.portfolio_df
                    

