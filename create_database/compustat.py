from _utils import get_annual_compustat_query
from _utils import convert_to_datetime
from datetime import datetime
from typing import Union

import pandas as pd
import numpy as np
import sqlalchemy


class Compustat:

    """
        A class for fetching and processing Compustat data from WRDS.

        Attributes:
            wrds (sqlalchemy.engine.base.Engine): A connection to the WRDS database.
            data_freq (str): The frequency of the data ('A' for annual, 'Q' for quarterly).
    """

    def __init__(self,
                 wrds: sqlalchemy.engine.base.Engine,
                 data_freq: str):
        """
            Initializes the Compustat class.

            Args:
                wrds (sqlalchemy.engine.base.Engine): A SQLAlchemy connection to the WRDS database.
                data_freq (str): Frequency of the data, either 'A' (annual) or 'Q' (quarterly).

            Raises:
                ValueError: If no WRDS connection is provided.
                ValueError: If the specified data frequency is not 'A' or 'Q'.
        """
        if wrds is None:
            raise ValueError('You should first establish WRDS connection')
        self.wrds = wrds

        if data_freq not in ['A', 'Q']:
            raise ValueError(f'Only annual (A) and quarterly (Q) create_database are supported!')
        self.data_freq = data_freq

    def get_data(self,
                 start_date: Union[datetime, str],
                 final_date: Union[datetime, str],
                 output_log: bool = False) -> pd.DataFrame:

        """
            Fetches and processes Compustat data for the specified date range.

            Args:
                start_date (Union[datetime, str]): The start date of the data request.
                final_date (Union[datetime, str]): The end date of the data request.
                output_log (bool, optional): If True, prints progress logs. Defaults to False.

            Returns:
                pd.DataFrame: A DataFrame containing processed Compustat data with key financial variables.

            Raises:
                ValueError: If `final_date` is earlier than `start_date`.
        """

        # Convert both dates
        start_date = convert_to_datetime(start_date, "start_date")
        final_date = convert_to_datetime(final_date, "final_date")

        # Ensure final_date is not earlier than start_date
        if final_date < start_date:
            raise ValueError("final_date cannot be earlier than start_date.")

        if output_log:
            if self.data_freq == 'A':
                print(f'Fetching annual compustat data from {start_date} to {final_date}')
            else:
                print(f'Fetching quarterly compustat data from {start_date} to {final_date}')

        if self.data_freq == 'A':
            query = get_annual_compustat_query(start_date=start_date, final_date=final_date)
            df = pd.read_sql_query(sql=query, con=self.wrds, dtype={"gvkey": str}, parse_dates={"datadate"})
            if output_log:
                print(f'Annual data from {start_date} to {final_date} have been fetched!')

            # add book value and operating profitability
            df = (df.assign(be=lambda x:(x["seq"].combine_first(x["ceq"] + x["pstk"])
                                        .combine_first(x["at"] - x["lt"]) +
                                        x["txditc"].combine_first(x["txdb"] + x["itcb"]).fillna(0) -
                                        x["pstkrv"].combine_first(x["pstkl"])
                                        .combine_first(x["pstk"]).fillna(0)))
                    .assign(be=lambda x: x["be"].apply(lambda y: np.nan if y <= 0 else y))
                    .assign(op=lambda x:((x["sale"] - x["cogs"].fillna(0) - x["xsga"].fillna(0) - x["xint"].fillna(0)) / x["be"]))
                )
            if output_log:
                print('book value (be) and operating profitability (op) columns have been created!')

            # Keep only the last available information for each firm-year group (by using the tail(1) for each group)
            df = (df.assign(year=lambda x: pd.DatetimeIndex(x["datadate"]).year)
                    .sort_values("datadate")
                    .groupby(["gvkey", "year"])
                    .tail(1)
                    .reset_index()
                  )
            if output_log:
                print('firm-year redundancies have been eliminated!')

            # Compute asset growth (inv)
            _df = df.get(["gvkey", "year", "at"]).assign(year=lambda x: x["year"] + 1).rename(columns={"at": "at_lag"})
            df = (df.merge(_df, how="left", on=["gvkey", "year"])
                    .assign(inv=lambda x: x["at"] / x["at_lag"] - 1)
                    .assign(inv=lambda x: np.where(x["at_lag"] <= 0, np.nan, x["inv"]))
                 )
            if output_log:
                print('asset growth (inv) column have been created!')

            # Get only desired columns
            desired_columns = ['gvkey', 'datadate', 'year', 'be', 'op', 'inv']
            df = df[desired_columns]

            return df

        elif self.data_freq == 'Q':
            pass
