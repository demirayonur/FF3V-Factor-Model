"""
CRSP Data Processing Class

This module defines a `CRSP` class for retrieving, processing, and analyzing stock market data
from the Center for Research in Security Prices (CRSP) using a WRDS (Wharton Research Data Services) database connection.

The class provides functions to fetch CRSP data, classify stocks by size, compute momentum and volatility,
and merge the CRSP data with Compustat data.

Dependencies:
    - pandas
    - numpy
    - sqlalchemy
    - sqlite3
    - _utils (custom utility functions)
    - fama_french_factors (custom module for Fama-French factor retrieval)

Classes:
    CRSP: Class for retrieving, processing, and analyzing CRSP stock data.

Usage:
    ```python
    from sqlalchemy import create_engine
    wrds_engine = create_engine("postgresql://username:password@wrds.wharton.upenn.edu:5432/wrds")
    crsp = CRSP(wrds=wrds_engine)
    crsp.set_data(start_date="2010-01-01", final_date="2020-12-31")
    ```
"""

import pandas as pd
import numpy as np
import sqlalchemy
import sqlite3

from datetime import datetime
from typing import Union
from _utils import (
    get_ccm_linking_table_query, change_crsp_exchange_codes,
    change_crsp_industry_codes, get_daily_crsp_query,
    convert_to_datetime, get_crsp_query
)
from fama_french_factors import FamaFrench


class CRSP:
    """
        A class to retrieve and process CRSP stock market data using a WRDS connection.

        Attributes:
            wrds (sqlalchemy.engine.base.Engine): WRDS database connection engine.
            df (pd.DataFrame): DataFrame containing processed CRSP data. (initially null)
    """

    def __init__(self,
                 wrds: sqlalchemy.engine.base.Engine):

        """
            Initializes the CRSP class with a WRDS database connection.

            Args:
                wrds (sqlalchemy.engine.base.Engine): A valid WRDS database connection.

            Raises:
                ValueError: If no WRDS connection is provided.
        """

        if wrds is None:
            raise ValueError('You should first establish WRDS connection')
        self.wrds = wrds
        self.df = None

    def set_data(self,
                 start_date: Union[datetime, str],
                 final_date: Union[datetime, str]) -> None:
        """
            Fetches CRSP data, classifies stocks by size, computes momentum and volatility, and links to Compustat.

            Args:
                start_date (Union[datetime, str]): The start date of the dataset.
                final_date (Union[datetime, str]): The end date of the dataset.
        """

        self.set_raw_data(start_date, final_date)
        self.create_market_cap_column()
        self.create_excess_return_column(start_date, final_date)
        self.create_momentum_column()
        self.create_volatility_column(start_date, final_date)
        self.classify_for_size()
        self.get_compustat_merge_links()

        desired_columns = ['permno', 'gvkey', 'exchange', 'industry', 'date', 'size_category', 'mktcap', 'ret_excess', 'momentum', 'volatility']
        self.df = self.df[desired_columns]

    def set_raw_data(self,
                 start_date: Union[datetime, str],
                 final_date: Union[datetime, str]) -> None:
        """
            Fetches and processes monthly CRSP data, including market cap calculation and excess return computation.

            Args:
                start_date (Union[datetime, str]): Start date for data retrieval.
                final_date (Union[datetime, str]): End date for data retrieval.
        """

        # Convert both dates
        start_date = convert_to_datetime(start_date, "start_date")
        final_date = convert_to_datetime(final_date, "final_date")

        # Ensure final_date is not earlier than start_date
        if final_date < start_date:
            raise ValueError("Final_date cannot be earlier than start_date.")

        query = get_crsp_query(start_date=start_date, final_date=final_date)
        self.df = pd.read_sql_query(sql=query, con=self.wrds, dtype={"permno": int, "siccd": int}, parse_dates={"date"})
        self.df = self.df.assign(shrout=lambda x: x["shrout"] * 1000)  # Convert shares to actual numbers

        change_crsp_exchange_codes(self.df)
        change_crsp_industry_codes(self.df)

    def create_market_cap_column(self):
        """Creates Market Cap Column."""

        self.df = self.df.assign(mktcap=lambda x: x["shrout"] * x["altprc"] / 1000000)
        self.df = self.df.assign(mktcap=lambda x: x["mktcap"].replace(0, np.nan))  # 0 market cap is a null value!

    def create_excess_return_column(self,
                                    start_date: Union[datetime, str],
                                    final_date: Union[datetime, str]
                                    ):
        """"Creates excess return column."""

        ff = FamaFrench(ff_version=3, data_freq='M')
        factors_ff3_monthly = ff.get_data(start_date=start_date, final_date=final_date)
        self.df = self.df.merge(factors_ff3_monthly, how="left", on="date")
        self.df = self.df.assign(ret_excess=lambda x: x["ret"] - x["rf"]).drop(columns=["rf"])
        self.df = self.df.dropna(subset=["ret_excess", "mktcap"])

    def classify_for_size(self):
        """Categorizes stocks into Large, Small, and Micro-cap based on NYSE market cap percentiles."""

        nyse_stocks = self.df[self.df['exchange'] == 'NYSE']
        large_cap_threshold = nyse_stocks['mktcap'].quantile(0.70)  # 70th percentile
        small_cap_threshold = nyse_stocks['mktcap'].quantile(0.30)  # 30th percentile

        def classify_size(mktcap):
            if mktcap >= large_cap_threshold:
                return "Large"
            elif mktcap >= small_cap_threshold:
                return "Small"
            else:
                return "Micro"

        self.df['size_category'] = self.df['mktcap'].apply(classify_size)

    def create_momentum_column(self):
        """Computes momentum for each stock based on past 11 months' excess returns (excluding the last month)"""

        self.df = self.df.sort_values(by=['permno', 'date']).reset_index(drop=True)

        def compute_momentum(group):
            momentum_values = np.full(len(group), np.nan)
            for i in range(12, len(group)):
                past_returns = group['ret_excess'].iloc[i - 12:i - 1]
                momentum_values[i] = np.prod(1 + past_returns.values) - 1
            group['momentum'] = momentum_values
            return group

        self.df = self.df.groupby('permno', group_keys=False).apply(compute_momentum)

    def get_compustat_merge_links(self):
        """Merges CRSP data with Compustat using CCM linking table."""

        ccm_linking_table_query = get_ccm_linking_table_query()

        ccm_linking_table = pd.read_sql_query(sql=ccm_linking_table_query, con=self.wrds,
                                              dtype={"permno": int, "gvkey": str}, parse_dates={"linkdt", "linkenddt"})

        ccm_links = (self.df.merge(ccm_linking_table, how="inner", on="permno")
                            .query("~gvkey.isnull() & (date >= linkdt) & (date <= linkenddt)")
                            .get(["permno", "gvkey", "date"])
                     )

        self.df = self.df.merge(ccm_links, how="left", on=["permno", "date"])

    def create_volatility_column(self,
                                 start_date: Union[datetime, str],
                                 final_date: Union[datetime, str]):
        """
            Computes rolling volatility using daily excess returns over a 60-day window.

            Args:
                start_date (Union[datetime, str]): Start date for data retrieval.
                final_date (Union[datetime, str]): End date for data retrieval.
        """
        def get_daily_crsp_data() -> pd.DataFrame:
            ff = FamaFrench(ff_version=3, data_freq='D')
            factors_ff3_daily = ff.get_data(start_date=start_date, final_date=final_date)

            permnos = pd.read_sql(sql="SELECT DISTINCT permno FROM crsp.stksecurityinfohist", con=self.wrds, dtype={"permno": int})
            permnos = list(permnos["permno"].astype(str))
            batch_size = 500
            batches = np.ceil(len(permnos) / batch_size).astype(int)

            df_list = []
            for j in range(1, batches + 1):
                permno_batch = permnos[((j - 1) * batch_size):(min(j * batch_size, len(permnos)))]
                permno_batch_formatted = (", ".join(f"'{permno}'" for permno in permno_batch))
                permno_string = f"({permno_batch_formatted})"
                query = get_daily_crsp_query(start_date, final_date, permno_string)

                crsp_daily_sub = pd.read_sql_query(sql=query, con=self.wrds, dtype={"permno": int}, parse_dates={"date"})
                crsp_daily_sub = crsp_daily_sub.dropna()

                if not crsp_daily_sub.empty:
                    crsp_daily_sub = crsp_daily_sub.merge(factors_ff3_daily[["date", "rf"]], on="date", how="left")
                    crsp_daily_sub = crsp_daily_sub.assign(ret_excess=lambda x: ((x["ret"] - x["rf"]).clip(lower=-1)))
                    crsp_daily_sub = crsp_daily_sub.get(["permno", "date", "ret_excess"])
                    df_list.append(crsp_daily_sub)

                print(f"Batch {j} out of {batches} done ({(j / batches) * 100:.2f}%)")

            crsp_daily_final = pd.concat(df_list, ignore_index=True)

            return crsp_daily_final

        df_daily = get_daily_crsp_data()

        self.df = self.df.sort_values(by=['permno', 'date']).reset_index(drop=True)
        df_daily = df_daily.sort_values(by=['permno', 'date']).reset_index(drop=True)

        def compute_vol(sub_df):
            sub_df["volatility"] = sub_df["ret_excess"].shift(1).rolling(window=60, min_periods=20).std()
            return sub_df


        df_daily = df_daily.groupby("permno", group_keys=False).apply(compute_vol)
        self.df = self.df.merge(df_daily[["permno", "date", "volatility"]], on=["permno", "date"], how="left")

    def write_to_sql(self,
                     db_con: sqlite3.Connection):
        """Writes the latest form of the dataframe to the given database as a table"""

        self.df.to_sql(name="crsp", con=db_con, if_exists="replace", index=False)