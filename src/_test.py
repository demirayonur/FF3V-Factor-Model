
from data_fama_french_factors import FamaFrench

ff = FamaFrench(ff_version=3, data_freq='M')
df = ff.get_data(start_date='2020-01-01', final_date='2022-12-31')
print(df.head())
