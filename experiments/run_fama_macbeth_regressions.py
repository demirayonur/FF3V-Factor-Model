import sys
import os

parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(os.path.join(parent_dir, "src"))
from fama_macbeth import FamaMacbeth

def run(start_date = '1963-01-01', final_date = '2023-12-31', drop_tail_percentile = None, desired_size = None):

    fm_regressor = FamaMacbeth(start_date, final_date, 6, drop_tail_percentile, desired_size)
    fm_regressor.prepare_data()
    results = fm_regressor.run(is_ols=False)
    return results

if __name__ == '__main__':

    # All Data
    res = run()
    print(res)

    print('\n------------------\n')

    # Micro-Caps
    res = run(desired_size='Micro')
    print(res)

    print('\n------------------\n')
    # Small-Caps
    res = run(desired_size='Small')
    print(res)

    print('\n------------------\n')
    # Large-Caps
    res = run(desired_size='Large')
    print(res)
