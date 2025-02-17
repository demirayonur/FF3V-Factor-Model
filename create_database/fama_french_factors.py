from _params import famafrench_identifiers_dict
from _utils import convert_to_datetime
from datetime import datetime
from typing import Union

import pandas_datareader as pdr
import pandas as pd
import sqlite3
import warnings
warnings.filterwarnings("ignore")


class FamaFrench:
    """
        # ------------------------------
        # Fama-French Three-Factor Data
        # ------------------------------

        We have the following columns:

        Column       Meaning               Description
        ------       -------               -----------
        mkt_excess   market excess return  the return of the market portfolio minus the risk-free rate
        smb          small minus big       the return diff b/w small-cap stocks and large-cap stocks
        hml          high minus low        the return diff b/w high book-to-market stocks and low book-to-market stocks
        rf           risk-free rate        the return on a risk-free asset, typically a one-month treasury bill


        # -----------------------------
        # Fama-French Five-Factor Data
        # -----------------------------

        We have additional two variables:

        Column       Meaning                 Description
        ------       -------                 -----------
        rmw          robust minus weak       the return spread b/w high-profitability firms and low-profitability firms
        cma          conservative minus      the return spread b/w firms with conservative investment policies and
                     aggressive              firms with aggressive investment policies
    """

    def __init__(self,
                 ff_version: int,
                 data_freq: str):
        """
           Initializes the FamaFrench class.

           This class retrieves and processes Fama-French factor create_database based on the specified
           factor model version and create_database frequency.

           Parameters:
           -----------
           ff_version : int
               The version of the Fama-French factor model. Must be either:
               - `3` for the Fama-French Three-Factor Model.
               - `5` for the Fama-French Five-Factor Model.

           data_freq : str
               The frequency of the create_database. Must be either:
               - `'D'` for daily create_database.
               - `'M'` for monthly create_database.

           Raises:
           -------
           TypeError
               If `ff_version` is not an integer.
           ValueError
               If `ff_version` is not 3 or 5.
               If `data_freq` is not `'D'` or `'M'`.

           Example:
           --------
           >>> fff_obj = FamaFrench(ff_version=3, data_freq='M')
        """

        if not isinstance(ff_version, int):
            raise TypeError("ff_version must be an integer.")

        if ff_version not in [3, 5]:
            raise ValueError(f'Only FamaFrench-3 and FamaFrench-5 is valid!')

        self.ff_version: int = ff_version

        if data_freq not in ['D', 'M']:
            raise ValueError(f'Only daily (D) and monthly (M) create_database are supported!')

        self.data_freq: str = data_freq
        self.df = None

    def set_data(self, start_date: Union[datetime, str], final_date: Union[datetime, str]):
        """
            Retrieves Fama-French factor create_database for a specified date range and factor model version.

            This function fetches either daily or monthly Fama-French factor create_database from the
            Kenneth R. French create_database library using `pandas_datareader`. The create_database is pre-processed
            to ensure consistency, including date conversion, percentage normalization, and
            column renaming.

            Parameters:
            -----------
            start_date : Union[datetime, str]
                The start date for retrieving the Fama-French create_database. Can be a `datetime` object
                or a string in a recognizable date format (e.g., 'YYYY-MM-DD').

            final_date : Union[datetime, str]
                The end date for retrieving the Fama-French create_database. Can be a `datetime` object
                or a string in a recognizable date format (e.g., 'YYYY-MM-DD').
                Must be greater than or equal to `start_date`.

            Raises:
            -------
            ValueError
                - If `final_date` is earlier than `start_date`.

            Example:
            --------
            >>> ff = FamaFrench(ff_version=3, data_freq='M')
            >>> ff.set_data(start_date='2020-01-01', final_date='2022-12-31')

            Notes:
            ------
            - The raw percentage values from the create_database source are converted to decimal format.
            - The column `"mkt-rf"` from the original dataset is renamed to `"market_excess_return"`.
            - The `"date"` column is converted to `datetime` format for ease of use.
        """

        start_date = convert_to_datetime(start_date, "start_date")
        final_date = convert_to_datetime(final_date, "final_date")
        if final_date < start_date:
            raise ValueError("final_date cannot be earlier than start_date.")

        raw_data = pdr.DataReader(
            name=famafrench_identifiers_dict[self.ff_version, self.data_freq],
            data_source="famafrench",
            start=start_date,
            end=final_date)[0]

        self.df = (raw_data
                   .divide(100)
                   .reset_index(names="date")
                   .assign(date=lambda x: pd.to_datetime(x["date"].astype(str)))
                   .rename(str.lower, axis="columns")
                   .rename(columns={"mkt-rf": "market_excess_return"})
                   )

    def get_data(self) -> pd.DataFrame:
        """Return the FF data"""

        if self.df is None or self.df.empty:
            raise ValueError("Fama-French data not available.")
        return self.df

    def write_to_sql(self, db_con: sqlite3.Connection):
        """
            Writes the processed Fama-French data to an SQLite database.

            Args:
                db_con (sqlite3.Connection): SQLite database connection where the data will be stored.

            Raises:
                ValueError: If `df` is None or empty, indicating that there is no data to write.
        """

        if self.df is None or self.df.empty:
            raise ValueError("No data available to write to SQL. Ensure that `set_data` has been executed.")

        format_name = f"fama_french_{self.ff_version}_{self.data_freq}"
        self.df.to_sql(name=format_name, con=db_con, if_exists="replace", index=False)
