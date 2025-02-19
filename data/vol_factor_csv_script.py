import os, sys

parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(os.path.join(parent_dir, "src"))

from vol_factor import VolFactor

start_date = '1963-01-01'
final_date = '2023-12-31'

f = VolFactor(start_date, final_date)
f.create_factor()
f.to_csv()