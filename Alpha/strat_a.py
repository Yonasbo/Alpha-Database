from alpha import Alpha
import numpy as np
import pandas as pd

class Strat_A(Alpha):
    def __init__(self, instruments,dfs, configs):
        super().__init__(instruments,dfs,configs)

    def compute_metas(self, index):
        super().compute_metas(index)
        alphas, eligibles, trenders = [],[],[]
        for inst in self.instruments:
            #print(inst, 'cm')
            self.dfs[inst]['smaf'] = self.dfs[inst]['close'].rolling(10).mean()
            self.dfs[inst]['smam'] = self.dfs[inst]['close'].rolling(30).mean()
            self.dfs[inst]['smas'] = self.dfs[inst]['close'].rolling(100).mean()
            self.dfs[inst]['smass'] = self.dfs[inst]['close'].rolling(300).mean()

            self.dfs[inst]['alphas'] = 0.0 + \
                (self.dfs[inst]['smaf']> self.dfs[inst]['smam'])+ \
                (self.dfs[inst]['smaf']> self.dfs[inst]['smas'])+ \
                (self.dfs[inst]['smaf']> self.dfs[inst]['smass'])+ \
                (self.dfs[inst]['smam']> self.dfs[inst]['smas'])+ \
                (self.dfs[inst]['smam']> self.dfs[inst]['smass'])

            self.dfs[inst]['eligible']= \
                (~np.isnan(self.dfs[inst]['smass'])) \
                & self.dfs[inst]['active']\
                & (self.dfs[inst]['vol']>0)\
                & (self.dfs[inst]['close']>0)
            
            alphas.append(self.dfs[inst]['alphas'])
            eligibles.append(self.dfs[inst]['eligible'])
            trenders.append(self.dfs[inst]['smaf']>self.dfs[inst]['smam'])
        
        self.invriskdf = np.log(1/self.voldf)/np.log(1.3)
        self.alphadf = pd.concat(alphas,axis=1)
        self.alphadf.columns = self.instruments
        self.eligiblesdf = pd.concat(eligibles,axis=1)
        self.eligiblesdf.columns = self.instruments
        self.trendersdf = pd.concat(trenders, axis=1)
        self.trendersdf.columns = self.instruments
        self.eligiblesdf.astype('int8')

    def compute_forecasts(self, portfolio_i, date, eligibles_row):
        return self.alphadf.loc[date], np.sum(eligibles_row)

    def post_risk_management(self, index, date, eligibles, nominal_tot, positions, weights):
        #print(date, eligibles, nominal_tot, positions, weights)
        num_trend = np.dot(0 +eligibles,0+self.trendersdf.loc[date].values)
        if np.sum(eligibles)>0 and num_trend/np.sum(eligibles)>0.6:
            return 0, np.zeros(len(positions)),np.zeros(len(weights))
        return nominal_tot, positions, weights
