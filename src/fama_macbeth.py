from pathlib import Path

import statsmodels.formula.api as smf
import pandas as pd
import numpy as np
import sqlite3
import os

import warnings
warnings.filterwarnings('ignore')


class FamaMacbeth:

    def __init__(self,
                 start_date: str,
                 final_date: str,
                 compustat_month_lag:int = 6,
                 drop_tail_percentile:float = None,
                 desired_size: str = None,):

        current_path = Path(__file__).resolve()
        parent_path = current_path.parent.parent
        data_folder = parent_path / "data"
        database_name = data_folder / f"{start_date}__{final_date}.sqlite"
        self.database_connection = sqlite3.connect(database=database_name)
        self.compustat_month_lag = compustat_month_lag
        self.data = None
        self.drop_tail_percentile = drop_tail_percentile
        if self.drop_tail_percentile is not None and self.drop_tail_percentile >= 0.25:
            raise ValueError('Drop tail percentile must be lower than 0.25')
        self.desired_size = desired_size

    def prepare_data(self):

        compustat_data = pd.read_sql_query(sql="SELECT * FROM compustat",
                                           con=self.database_connection,
                                           parse_dates={"datadate"})

        crsp_data = pd.read_sql_query(sql="SELECT * FROM crsp", con=self.database_connection, parse_dates={"date"})

        characteristics = (compustat_data
                           .assign(date=lambda x: x["datadate"].dt.to_period("M").dt.to_timestamp())
                           .merge(crsp_data, how="left", on=["gvkey", "date"], )
                           .assign(
                                    log_bm=lambda x: np.log(x["be"] / x["mktcap"]).where(x["be"] > 0, np.nan),
                                    log_mktcap=lambda x: np.log(x["mktcap"]),
                                    sorting_date=lambda x: x["date"] + pd.DateOffset(months=self.compustat_month_lag)
                                  ))
        characteristics = characteristics.get(["gvkey", "log_bm", "log_mktcap", "sorting_date", "op", "inv"])

        data_fama_macbeth = (crsp_data.merge(characteristics, how="left", left_on=["gvkey", "date"], right_on=["gvkey", "sorting_date"])
                                      .sort_values(["date", "permno"])
                                      .groupby("permno")
                                      .apply(lambda x: x.assign(log_bm=x["log_bm"].fillna(method="ffill"),
                                                                op=x["op"].fillna(method="ffill"),
                                                                inv=x["inv"].fillna(method="ffill"),
                                                                log_mktcap=x["log_mktcap"].fillna(method="ffill"))))
        data_fama_macbeth = data_fama_macbeth.reset_index(drop=True)

        data_fama_macbeth_lagged = (data_fama_macbeth
                                    .assign(date=lambda x: x["date"] - pd.DateOffset(months=1))
                                    .get(["permno", "date", "ret_excess"])
                                    .rename(columns={"ret_excess": "ret_excess_lead"})
                                    )

        data_fama_macbeth = (data_fama_macbeth.merge(data_fama_macbeth_lagged, how="left", on=["permno", "date"])
                                              .get(["permno", "date", "size_category", "ret_excess_lead", "log_mktcap", "mktcap", "log_bm", "op", "inv", "ret_excess", "momentum", "volatility"])
                                              .dropna())

        if self.drop_tail_percentile is not None:
            date_counts = data_fama_macbeth['date'].value_counts()
            percentile_ = date_counts.quantile(self.drop_tail_percentile)
            valid_dates = date_counts[date_counts >= percentile_].index
            data_fama_macbeth = data_fama_macbeth[data_fama_macbeth['date'].isin(valid_dates)]

        if self.desired_size is not None:
            data_fama_macbeth = data_fama_macbeth[data_fama_macbeth['size_category'] == self.desired_size]

        self.data = data_fama_macbeth

    def run(self, is_ols=True) -> pd.DataFrame:

        # cross-sectional regression
        """"""
        if is_ols:
            formula_string = "ret_excess_lead ~ log_mktcap + log_bm + op + inv + ret_excess + momentum  + volatility"
            risk_premiums = (self.data.groupby("date").apply(lambda x: smf.ols(formula=formula_string, data=x).fit().params)
                               .reset_index())
        else:  # wls
            formula_string = "ret_excess_lead ~ log_mktcap + log_bm + op + inv + ret_excess + momentum + volatility"
            risk_premiums = (self.data.groupby("date")
                             .apply(lambda x: smf.wls(formula=formula_string, data=x, weights=x["mktcap"]).fit().params)
                             .reset_index())

        # time series aggregation
        price_of_risk = (risk_premiums
                         .melt(id_vars="date", var_name="factor", value_name="estimate")
                         .groupby("factor")["estimate"]
                         .apply(lambda x: pd.Series({"risk_premium": 100 * x.mean()}))
                         .reset_index()
                         .pivot(index="factor", columns="level_1", values="estimate")
                         .reset_index())

        # Newey and West (1987) Standard Errors
        price_of_risk_newey_west = (risk_premiums
                                    .melt(id_vars="date", var_name="factor", value_name="estimate")
                                    .groupby("factor")
                                    .apply(lambda x: (x["estimate"].mean() / smf.ols("estimate ~ 1", x).fit(cov_type="HAC", cov_kwds={"maxlags": 6}).bse))
                                    .reset_index()
                                    .rename(columns={"Intercept": "t_stat_newey_west"})
                                    )

        price_of_risk = price_of_risk.merge(price_of_risk_newey_west, on="factor").round(3)

        return price_of_risk
