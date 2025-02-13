"""
Compustat Data Processing Class

This module defines a `Compustat` class for retrieving, processing, and analyzing financial statement data
from the Compustat database using a WRDS (Wharton Research Data Services) database connection.

The class provides functions to fetch Compustat data, compute book equity, operating profitability,
and investment metrics.

Dependencies:
    - pandas
    - numpy
    - sqlalchemy
    - sqlite3
    - _utils (custom utility functions)

Classes:
    Compustat: Class for retrieving, processing, and analyzing Compustat financial statement data.

Usage:
    ```python
    from sqlalchemy import create_engine
    wrds_engine = create_engine(f"postgresql://{your_username}:{your_password}@wrds.wharton.upenn.edu:5432/wrds")
    compustat = Compustat(wrds=wrds_engine)
    compustat.set_data(start_date="2010-01-01", final_date="2020-12-31")
    compustat.write_to_sql(your_db)
    ```
"""

import pandas as pd
import numpy as np
import sqlalchemy
import sqlite3
from datetime import datetime
from typing import Union
from _utils import get_annual_compustat_query, convert_to_datetime


class Compustat:
    """
        A class to retrieve and process Compustat financial statement data using a WRDS connection.

        Attributes:
            wrds (sqlalchemy.engine.base.Engine): WRDS database connection engine.
            df (pd.DataFrame): DataFrame containing processed Compustat data.
    """

    def __init__(self, wrds: sqlalchemy.engine.base.Engine):
        """
            Initializes the Compustat class with a WRDS database connection.

            Args:
                wrds (sqlalchemy.engine.base.Engine): A valid WRDS database connection.

            Raises:
                ValueError: If no WRDS connection is provided.
        """

        if wrds is None:
            raise ValueError('You should first establish WRDS connection')
        self.wrds = wrds
        self.df = None

    def set_data(self, start_date: Union[datetime, str], final_date: Union[datetime, str]):
        """
        Fetches Compustat data and computes financial metrics including book equity,
        operating profitability, and investment.

        Args:
            start_date (Union[datetime, str]): The start date of the dataset.
            final_date (Union[datetime, str]): The end date of the dataset.

        Raises:
            ValueError: If `final_date` is earlier than `start_date`.
        """

        start_date = convert_to_datetime(start_date, "start_date")
        final_date = convert_to_datetime(final_date, "final_date")
        if final_date < start_date:
            raise ValueError("final_date cannot be earlier than start_date.")

        self.set_raw_data(start_date ,final_date)
        self.add_be_and_op_columns()
        self.add_inv_column()

        desired_columns = ['gvkey', 'datadate', 'year', 'be', 'op', 'inv']
        self.df = self.df[desired_columns]

    def set_raw_data(self, start_date: datetime, final_date: datetime):
        """
            Fetches annual Compustat data for the specified date range.

            Args:
                start_date (datetime): Start date for data retrieval.
                final_date (datetime): End date for data retrieval.
        """
        query = get_annual_compustat_query(start_date=start_date, final_date=final_date)
        self.df = pd.read_sql_query(sql=query, con=self.wrds, dtype={"gvkey": str}, parse_dates={"datadate"})

    def add_be_and_op_columns(self):
        """
            Computes book equity (BE) and operating profitability (OP) for each firm-year observation.

            - `BE` (Book Equity) is calculated based on shareholders' equity adjusted for
              preferred stock and deferred taxes.
            - `OP` (Operating Profitability) is computed as revenues minus cost of goods sold,
              selling and administrative expenses, and interest expense, scaled by BE.

            The method retains only the last available information for each firm-year.
        """

        self.df = (self.df.assign(be=lambda x: (x["seq"].combine_first(x["ceq"] + x["pstk"])
                                                .combine_first(x["at"] - x["lt"]) +
                                                x["txditc"].combine_first(x["txdb"] + x["itcb"]).fillna(0) -
                                                x["pstkrv"].combine_first(x["pstkl"])
                                                .combine_first(x["pstk"]).fillna(0)))
                   .assign(be=lambda x: x["be"].apply(lambda y: np.nan if y <= 0 else y))
                   .assign(
            op=lambda x: ((x["sale"] - x["cogs"].fillna(0) - x["xsga"].fillna(0) - x["xint"].fillna(0)) / x["be"]))
                   )

        # Keep only the last available information for each firm-year group (by using the tail(1) for each group)
        self.df = (self.df.assign(year=lambda x: pd.DatetimeIndex(x["datadate"]).year)
                          .sort_values("datadate")
                          .groupby(["gvkey", "year"])
                          .tail(1)
                          .reset_index()
                  )

    def add_inv_column(self):
        """
            Computes investment (INV) for each firm-year as the percentage change in total assets (`at`).

            Investment is calculated as:
                `INV = (AT_t / AT_{t-1}) - 1`

            Where:
                - `AT_t` is total assets in the current year.
                - `AT_{t-1}` is total assets in the previous year.

            If `AT_{t-1}` is non-positive, the investment is set to NaN.
        """

        _df = self.df.get(["gvkey", "year", "at"]).assign(year=lambda x: x["year"] + 1).rename(columns={"at": "at_lag"})
        self.df = (self.df.merge(_df, how="left", on=["gvkey", "year"])
                          .assign(inv=lambda x: x["at"] / x["at_lag"] - 1)
                          .assign(inv=lambda x: np.where(x["at_lag"] <= 0, np.nan, x["inv"]))
                  )

    def write_to_sql(self, db_con: sqlite3.Connection):
        """
            Writes the processed Compustat data to an SQLite database.

            Args:
                db_con (sqlite3.Connection): SQLite database connection where the data will be stored.

            Raises:
                ValueError: If `df` is None or empty, indicating that there is no data to write.
        """

        if self.df is None or self.df.empty:
            raise ValueError("No data available to write to SQL. Ensure that `set_data` has been executed.")

        self.df.to_sql(name="compustat", con=db_con, if_exists="replace", index=False)
