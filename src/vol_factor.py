from _utils import compute_nyse_breakpoints, value_weighted_returns
from pathlib import Path

import pandas as pd
import numpy as np
import os


class VolFactor:

    """This class is responsible for generating and writing the volatility factor, shortly vol_factor

    Example Code:


    >>> start_date = '1963-01-01'
    >>> final_date = '2023-12-31'

    >>> f = VolFactor(start_date, final_date)
    >>> f.create_factor()
    >>> f.to_csv()
    """

    def __init__(self, start_date: str, final_date: str):

        self.start_date = start_date
        self.final_date = final_date
        self.df = None

    def create_factor(self):

        # make sure that you run the 'sql_to_csv.py' script under ./data folder

        current_path = Path(os.getcwd()).resolve()
        parent_path = current_path.parent
        data_folder = parent_path / "data"
        data_name = data_folder / f"{self.start_date}__{self.final_date}.csv"
        monthly_data = pd.read_csv(data_name)

        nyse_breakpoints = compute_nyse_breakpoints(monthly_data)
        monthly_data = monthly_data.merge(nyse_breakpoints, on='date', how='left')

        monthly_data['size_cat'] = np.where(monthly_data['mktcap'] <= monthly_data['size_median'], 'S', 'B')

        monthly_data['vol_cat'] = np.where(monthly_data['volatility'] <= monthly_data['vol_30'], 'L',
                                 np.where(monthly_data['volatility'] >= monthly_data['vol_70'], 'H', 'M'))

        monthly_data['portfolio'] = monthly_data['size_cat'] + "/" + monthly_data['vol_cat']

        portfolio_returns = value_weighted_returns(monthly_data)

        s_h = portfolio_returns[portfolio_returns['portfolio'] == 'S/H'][['date', 'vw_return']].rename(
            columns={'vw_return': 'S_H'})

        s_l = portfolio_returns[portfolio_returns['portfolio'] == 'S/L'][['date', 'vw_return']].rename(
            columns={'vw_return': 'S_L'})

        b_h = portfolio_returns[portfolio_returns['portfolio'] == 'B/H'][['date', 'vw_return']].rename(
            columns={'vw_return': 'B_H'})

        b_l = portfolio_returns[portfolio_returns['portfolio'] == 'B/L'][['date', 'vw_return']].rename(
            columns={'vw_return': 'B_L'})

        self.df = s_h.merge(s_l, on='date', how='inner').merge(b_h, on='date', how='inner').merge(b_l, on='date', how='inner')

        self.df['vol'] = 0.5 * ((self.df['S_H'] - self.df['S_L']) + (self.df['B_H'] - self.df['B_L']))

        self.df = self.df[['date', 'vol']]

    def to_csv(self):
        """Write as a csv file under ./data folder"""
        current_path = Path(os.getcwd()).resolve()
        parent_path = current_path.parent
        data_folder = parent_path / "data"

        self.df.to_csv(data_folder / "vol_factor.csv", index=False)
