from create_database.macro_predictors import MacroPredictors
from create_database.q_factors import QFactors
from fama_french_factors import FamaFrench
from sqlalchemy import create_engine
from compustat import Compustat
from dotenv import load_dotenv
from pathlib import Path
from crsp import CRSP
from cpi import CPI
import sqlite3
import os


def run(start_date: str, final_date: str):

    # Database connection
    # -------------------
    current_path = Path(__file__).resolve()
    parent_path = current_path.parent.parent
    data_folder = parent_path / "data"
    if not data_folder.exists():
        data_folder.mkdir()
    database_name = data_folder / f"{start_date}__{final_date}.sqlite"
    database_connection = sqlite3.connect(database=database_name)

    # Monthly Fama French 3
    # ---------------------
    ff_3_m = FamaFrench(ff_version=3, data_freq='M')
    ff_3_m.set_data(start_date=start_date, final_date=final_date)
    ff_3_m.write_to_sql(db_con=database_connection)

    # Monthly Fama French 5
    # ---------------------
    ff_5_m = FamaFrench(ff_version=5, data_freq='M')
    ff_5_m.set_data(start_date=start_date, final_date=final_date)
    ff_5_m.write_to_sql(db_con=database_connection)

    # CPI Data
    # --------
    cpi_reader = CPI(start_date=start_date, final_date=final_date)
    cpi_reader.set_data(normalize=True)
    cpi_reader.write_to_sql(db_con=database_connection)

    # WRDS Connection
    # ---------------
    load_dotenv()
    wrds_connection = ("postgresql+psycopg2://"
        f"{os.getenv('WRDS_USERNAME')}:{os.getenv('WRDS_PASSWORD')}"
        "@wrds-pgdata.wharton.upenn.edu:9737/wrds"
    )
    wrds = create_engine(wrds_connection, pool_pre_ping=True)

    # Compustat
    # ---------
    compustat_reader = Compustat(wrds)
    compustat_reader.set_data(start_date=start_date, final_date=final_date)
    compustat_reader.write_to_sql(db_con=database_connection)

    # CRSP
    # -----
    crsp_reader = CRSP(wrds)
    crsp_reader.set_data(start_date=start_date, final_date=final_date)
    crsp_reader.write_to_sql(db_con=database_connection)

    # Q-Factors
    # ---------
    q_reader = QFactors(start_date=start_date, final_date=final_date)
    q_reader.set_data()
    q_reader.write_to_sql(db_con=database_connection)

    # Macroeconomic Predictors
    # ------------------------
    macro = MacroPredictors(start_date=start_date, final_date=final_date)
    macro.set_data()
    macro.write_to_sql(db_con=database_connection)



if __name__ == '__main__':
    start_date = "2021-01-01"
    final_date = "2023-12-31"
    run(start_date, final_date)
