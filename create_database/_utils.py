from datetime import datetime
from typing import Union

import pandas as pd


def convert_to_datetime(date_value: Union[str, datetime],
                        param_name: str) -> datetime:
    """
    Converts a date input into a `datetime` object, ensuring proper format and validation.

    This utility function checks whether the provided date value is already a `datetime` object
    or a string in the format `'YYYY-MM-DD'`. If it is a valid string, it converts it to a
    `datetime` object. If the format is incorrect or the input type is invalid, it raises an
    appropriate error.

    Parameters:
    -----------
    date_value : Union[datetime, str]
        The date value to be validated and converted. It can be either:
        - A `datetime` object (returned as is).
        - A string formatted as `'YYYY-MM-DD'` (converted to `datetime`).

    param_name : str
        The name of the parameter being converted. Used in error messages to provide
        context if validation fails.

    Returns:
    --------
    datetime
        A `datetime` object corresponding to the input date.

    Raises:
    -------
    ValueError
        If `date_value` is a string but not in the `'YYYY-MM-DD'` format.
    TypeError
        If `date_value` is neither a `datetime` object nor a string.

    Example:
    --------
    >>> convert_to_datetime("2024-02-13", "start_date")
    datetime.datetime(2024, 2, 13, 0, 0)

    >>> convert_to_datetime(datetime(2024, 2, 13), "end_date")
    datetime.datetime(2024, 2, 13, 0, 0)

    >>> convert_to_datetime("13-02-2024", "start_date")  # Raises ValueError
    ValueError: Invalid start_date format. Use 'YYYY-MM-DD'.

    Notes:
    ------
    - This function is particularly useful for ensuring consistent date formatting before
      performing date-based operations.
    - It prevents potential errors caused by incorrect or inconsistent date representations.
    """

    if isinstance(date_value, datetime):
        return date_value  # Already a datetime, return as is
    elif isinstance(date_value, str):
        try:
            return datetime.strptime(date_value, "%Y-%m-%d")  # Convert string to datetime
        except ValueError:
            raise ValueError(f"Invalid {param_name} format. Use 'YYYY-MM-DD'.")
    else:
        raise TypeError(f"{param_name} must be either a datetime object or a string in 'YYYY-MM-DD' format.")


def get_annual_compustat_query(start_date: datetime,
                               final_date: datetime) -> str:
    """
        Generates an SQL query to retrieve annual Compustat data from WRDS.

        Args:
            start_date (datetime): The start date for filtering the data.
            final_date (datetime): The end date for filtering the data.

        Returns:
            str: An SQL query string to fetch annual Compustat data.

        Notes:
            - The query selects key financial variables from the `comp.funda` table.
            - Filters applied:
                - `indfmt = 'INDL'`: Industrial format data.
                - `datafmt = 'STD'`: Standardized data.
                - `consol = 'C'`: Consolidated financials.
                - `curcd = 'USD'`: Data in U.S. dollars.
                - `datadate BETWEEN start_date AND final_date`: Restricts data to the specified date range.
    """

    compustat_query = (
        "SELECT gvkey, datadate, seq, ceq, at, lt, txditc, txdb, itcb,  pstkrv, pstkl, pstk, sale, cogs, xint, xsga "
        "FROM comp.funda "
        "WHERE indfmt = 'INDL' "
            "AND datafmt = 'STD' "
            "AND consol = 'C' "
            "AND curcd = 'USD' "
            f"AND datadate BETWEEN '{start_date}' AND '{final_date}'"
    )
    return compustat_query


def get_crsp_query(start_date: datetime,
                   final_date: datetime) -> str:

    crsp_monthly_query = (
        "SELECT msf.permno, date_trunc('month', msf.mthcaldt)::date AS date, msf.mthret AS ret, "
                "msf.shrout, msf.mthprc AS altprc, ssih.primaryexch, ssih.siccd "
        "FROM crsp.msf_v2 AS msf "
        "INNER JOIN crsp.stksecurityinfohist AS ssih "
        "ON msf.permno = ssih.permno AND ssih.secinfostartdt <= msf.mthcaldt AND msf.mthcaldt <= ssih.secinfoenddt "
        f"WHERE msf.mthcaldt BETWEEN '{start_date}' AND '{final_date}' "
            "AND ssih.sharetype = 'NS' "
            "AND ssih.securitytype = 'EQTY' "
            "AND ssih.securitysubtype = 'COM' "
            "AND ssih.usincflg = 'Y' "
            "AND ssih.issuertype in ('ACOR', 'CORP') "
            "AND ssih.primaryexch in ('N', 'A', 'Q') "
            "AND ssih.conditionaltype in ('RW', 'NW') "
            "AND ssih.tradingstatusflg = 'A'"
    )
    return crsp_monthly_query

def get_daily_crsp_query(start_date: datetime,
                         final_date: datetime,
                         permno_string: str) -> str:
    crsp_daily_sub_query = (
        "SELECT dsf.permno, dlycaldt AS date, dlyret AS ret "
        "FROM crsp.dsf_v2 AS dsf "
        "INNER JOIN crsp.stksecurityinfohist AS ssih "
        "ON dsf.permno = ssih.permno AND "
        "ssih.secinfostartdt <= dsf.dlycaldt AND "
        "dsf.dlycaldt <= ssih.secinfoenddt "
        f"WHERE dsf.permno IN {permno_string} "
        f"AND dlycaldt BETWEEN '{start_date}' AND '{final_date}' "
        "AND ssih.sharetype = 'NS' "
        "AND ssih.securitytype = 'EQTY' "
        "AND ssih.securitysubtype = 'COM' "
        "AND ssih.usincflg = 'Y' "
        "AND ssih.issuertype in ('ACOR', 'CORP') "
        "AND ssih.primaryexch in ('N', 'A', 'Q') "
        "AND ssih.conditionaltype in ('RW', 'NW') "
        "AND ssih.tradingstatusflg = 'A'"
    )
    return crsp_daily_sub_query

def get_ccm_linking_table_query():
    ccm_linking_table_query = (
        "SELECT lpermno AS permno, gvkey, linkdt, "
        "COALESCE(linkenddt, CURRENT_DATE) AS linkenddt "
        "FROM crsp.ccmxpf_linktable "
        "WHERE linktype IN ('LU', 'LC') "
        "AND linkprim IN ('P', 'C')"
    )
    return ccm_linking_table_query

def change_crsp_exchange_codes(crsp_df: pd.DataFrame) -> None:
    # inplace
    def assign_exchange(primaryexch):
        if primaryexch == "N":
            return "NYSE"
        elif primaryexch == "A":
            return "AMEX"
        elif primaryexch == "Q":
            return "NASDAQ"
        else:
            return "Other"

    crsp_df["exchange"] = crsp_df["primaryexch"].apply(assign_exchange)

def change_crsp_industry_codes(crsp_df: pd.DataFrame) -> None:
    # inplace
    def assign_industry(siccd):
        if 1 <= siccd <= 999:
            return "Agriculture"
        elif 1000 <= siccd <= 1499:
            return "Mining"
        elif 1500 <= siccd <= 1799:
            return "Construction"
        elif 2000 <= siccd <= 3999:
            return "Manufacturing"
        elif 4000 <= siccd <= 4899:
            return "Transportation"
        elif 4900 <= siccd <= 4999:
            return "Utilities"
        elif 5000 <= siccd <= 5199:
            return "Wholesale"
        elif 5200 <= siccd <= 5999:
            return "Retail"
        elif 6000 <= siccd <= 6799:
            return "Finance"
        elif 7000 <= siccd <= 8999:
            return "Services"
        elif 9000 <= siccd <= 9999:
            return "Public"
        else:
            return "Missing"

    crsp_df["industry"] = crsp_df["siccd"].apply(assign_industry)


