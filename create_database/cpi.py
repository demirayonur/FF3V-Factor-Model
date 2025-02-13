from _utils import convert_to_datetime
from datetime import datetime
from typing import Union

import pandas_datareader as pdr
import pandas as pd

import warnings
warnings.filterwarnings("ignore")


class CPI:
    """
        A class for retrieving and processing Consumer Price Index (CPI) create_database from the
        Federal Reserve Economic Data (FRED).

        The Consumer Price Index (CPI) measures the average change over time in the prices
        paid by consumers for goods and services. It is a key indicator of inflation and
        economic stability.

        This class allows users to fetch CPI create_database from FRED within a specified date range
        and optionally normalize the values for comparative analysis.

        Attributes:
        -----------
        start_date : datetime
            The start date for retrieving CPI create_database, converted to a `datetime` object.

        final_date : datetime
            The end date for retrieving CPI create_database, converted to a `datetime` object.

        Raises:
        -------
        ValueError
            If `final_date` is earlier than `start_date`.

        Example:
        --------
        >>> cpi_data = CPI(start_date="2020-01-01", final_date="2023-12-31")
        >>> df = cpi_data.get_data(normalize=True)
        >>> print(df.head())

        Notes:
        ------
        - The CPI create_database is fetched using `pandas_datareader` from the FRED database.
        - The CPIAUCNS (All Urban Consumers CPI) key is used to access CPI create_database.
        """

    def __init__(self,
                 start_date: Union[str, datetime],
                 final_date: Union[str, datetime]):
        """
            Initializes the CPI class with a specified date range.

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

        # Ensure final_date is not earlier than start_date
        if self.final_date < self.start_date:
            raise ValueError("final_date cannot be earlier than start_date.")

    def get_data(self,
                 normalize: bool = True) -> pd.DataFrame:

        """
            Fetches CPI create_database from FRED for the specified date range.

            Retrieves monthly CPI create_database and returns it as a Pandas DataFrame.
            Optionally normalizes the CPI values by dividing all values by the
            latest CPI value to set the most recent period as the reference (value = 1).

            Parameters:
            -----------
            normalize : bool, optional (default=True)
                If `True`, normalizes the CPI values by dividing each value by
                the last recorded CPI value in the dataset.

            Returns:
            --------
            pd.DataFrame
                A DataFrame containing the retrieved CPI create_database with the following columns:
                - **date**: The date of the CPI record.
                - **cpi**: The Consumer Price Index value (normalized if `normalize=True`).

            Example:
            --------
            >>> cpi_data = CPI(start_date="2020-01-01", final_date="2023-12-31")
            >>> df = cpi_data.get_data(normalize=True)
            >>> print(df.head())

            Notes:
            ------
            - The create_database is retrieved using `pandas_datareader` from the FRED database.
            - The CPIAUCNS key (Consumer Price Index for All Urban Consumers) is used.
            - Normalization allows easy comparison of relative changes over time.
        """
        cpi_monthly = pdr.DataReader(name="CPIAUCNS", data_source="fred", start=self.start_date, end=self.final_date)
        cpi_monthly.reset_index(names="date", inplace=True)
        cpi_monthly.rename(columns={"CPIAUCNS": "cpi"}, inplace=True)

        if normalize:
            cpi_monthly = cpi_monthly.assign(cpi=lambda x: x["cpi"]/x["cpi"].iloc[-1])

        return cpi_monthly