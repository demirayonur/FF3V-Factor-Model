from _utils import convert_to_datetime
from datetime import datetime
from typing import Union

import pandas as pd
import sqlite3
import ssl


class QFactors:

    def __init__(self,
                 start_date: Union[str, datetime],
                 final_date: Union[str, datetime]):
        """
               Initializes the QFactors class with a specified date range.

               Converts input dates into `datetime` objects and ensures validity
               before fetching CPI create_database.

               Parameters:
               -----------
               start_date : Union[str, datetime]
                   The start date for retrieving CPI create_database. It can be either a string in
                   'YYYY-MM-DD' format or a `datetime` object.

               final_date : Union[str, datetime]
                   The end date for retrieving CPI create_database. It can be either a string in
                   'YYYY-MM-DD' format or a `datetime` object.
                   Must be greater than or equal to `start_date`.

               Raises:
               -------
               ValueError
                   If `final_date` is earlier than `start_date`.
        """

        self.start_date = convert_to_datetime(start_date, "start_date")
        self.final_date = convert_to_datetime(final_date, "final_date")
        if self.final_date < self.start_date:
            raise ValueError("final_date cannot be earlier than start_date.")
        self.df = None

    def set_data(self):

        ssl._create_default_https_context = ssl._create_unverified_context
        _link = "https://global-q.org/uploads/1/2/2/6/122679606/q5_factors_monthly_2023.csv"
        self.df = pd.read_csv(_link)
        self.df = self.df.assign(date=lambda x: (pd.to_datetime(x["year"].astype(str) + "-" + x["month"].astype(str) + "-01")))
        self.df = self.df.drop(columns=["R_F", "R_MKT", "year"])
        self.df = self.df.rename(columns=lambda x: x.replace("R_", "").lower())
        self.df = self.df.query(f"date >= '{self.start_date}' and date <= '{self.final_date}'")
        self.df = self.df.assign(**{col: lambda x: x[col] / 100 for col in ["me", "ia", "roe", "eg"]})

        ssl._create_default_https_context = ssl.create_default_context

    def write_to_sql(self, db_con: sqlite3.Connection):
        """
            Writes the processed Q-factors data to an SQLite database.

            Args:
                db_con (sqlite3.Connection): SQLite database connection where the data will be stored.

            Raises:
                ValueError: If `df` is None or empty, indicating that there is no data to write.
        """

        if self.df is None or self.df.empty:
            raise ValueError("No data available to write to SQL. Ensure that `set_data` has been executed.")

        self.df.to_sql(name="q_factors", con=db_con, if_exists="replace", index=False)