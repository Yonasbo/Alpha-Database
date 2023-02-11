import numpy as np

def get_pnl_stats(portfolio_df, last_weights, last_units, idx, baseclose_row, ret_row,capitals,leverages,ewmas):
    
    ret_row = np.nan_to_num(ret_row,nan=0,posinf=0,neginf=0)
    day_pnl = np.sum(last_units*baseclose_row*ret_row)
    nominal_ret = np.dot(last_weights,ret_row)
    capital_ret = nominal_ret*leverages[-1]
    capitals.append(capitals[-1]+day_pnl)
    
    ewmas.append(0.06*(capital_ret**2)+0.94 * ewmas[-1] if nominal_ret!= 0 else ewmas[-1])
    return capitals, nominal_ret, ewmas


def unit_base_value(instrument, dfs, date):
    if instrument[-3:] == 'USD':
        return dfs[instrument].at[date, 'close']
    if instrument[-3:] + 'USD%USD' in dfs:
        return dfs[instrument].at[date, 'close']* dfs[instrument[-3:]+'USD%USD'].at[date,'close']
    elif 'USD' + instrument[-3:] + '%' + instrument[-3:] in dfs:
        return dfs[instrument].at[date,'close'] * 1/ dfs['USD' + instrument[-3:] + '%' + instrument[-3:]].at[date,'close']
    else:
        print('NO SOlution')
        exit()

