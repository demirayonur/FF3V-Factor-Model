from _utils import convert_to_datetime
from datetime import datetime
from typing import Union

import pandas as pd
import numpy as np
import sqlite3
import ssl


class MacroPredictors:

    def __init__(self,
                 start_date: Union[str, datetime],
                 final_date: Union[str, datetime]):
        """
               Initializes the MacroPredictors class with a specified date range.

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
        """read and sets the data"""
        sheet_id = "1bM7vCWd3WOt95Sf9qjLPZjoiafgF_8EG"
        sheet_name = "macro_predictors.xlsx"
        macro_predictors_link = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"

        ssl._create_default_https_context = ssl._create_unverified_context

        self.df = (
            pd.read_csv(macro_predictors_link, thousands=",")
            .assign(
                date=lambda x: pd.to_datetime(x["yyyymm"], format="%Y%m"),
                dp=lambda x: np.log(x["D12"]) - np.log(x["Index"]),
                dy=lambda x: np.log(x["D12"]) - np.log(x["Index"].shift(1)),
                ep=lambda x: np.log(x["E12"]) - np.log(x["Index"]),
                de=lambda x: np.log(x["D12"]) - np.log(x["E12"]),
                tms=lambda x: x["lty"] - x["tbl"],
                dfy=lambda x: x["BAA"] - x["AAA"]
            )
            .rename(columns={"b/m": "bm"})
            .get(["date", "dp", "dy", "ep", "de", "svar", "bm",
                  "ntis", "tbl", "lty", "ltr", "tms", "dfy", "infl"])
            .query("date >= @self.start_date and date <= @self.final_date")
            .dropna()
        )

        ssl._create_default_https_context = ssl.create_default_context


    def write_to_sql(self, db_con: sqlite3.Connection):
        """
            Writes the processed Macro Predictors data to an SQLite database.

            Args:
                db_con (sqlite3.Connection): SQLite database connection where the data will be stored.

            Raises:
                ValueError: If `df` is None or empty, indicating that there is no data to write.
        """

        if self.df is None or self.df.empty:
            raise ValueError("No data available to write to SQL. Ensure that `set_data` has been executed.")

        self.df.to_sql(name="macro_predictors", con=db_con, if_exists="replace", index=False)
