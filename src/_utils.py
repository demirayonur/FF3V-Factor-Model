import pandas as pd
import numpy as np

def compute_nyse_breakpoints(_df):
    breakpoints = _df[_df['exchange'] == 'NYSE'].groupby('date').apply(
        lambda x: pd.Series({
            'size_median': x['mktcap'].median(),
            'vol_30': x['volatility'].quantile(0.3),
            'vol_70': x['volatility'].quantile(0.7)
        })
    )
    return breakpoints.reset_index()


def value_weighted_returns(_df):
    portfolio_returns = _df.groupby(['date', 'portfolio']).apply(
        lambda x: np.average(x['ret_excess'], weights=x['mktcap'])
    ).reset_index()

    portfolio_returns.columns = ['date', 'portfolio', 'vw_return']
    return portfolio_returns
