import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import wget
import os
import time
import random
import warnings
from glob import glob
import analysis_module

import time

# Start the timer
start_time = time.time()

city_dict = {"Calgary": 50430}

# city_dict = {"Calgary": 50430,
#              "Montreal": 30165,
#             "Vancouver": 51442,
#             "Winnipeg": 51097}

result = []
list_of_years = [2022]

# create query dictionary for each city/year
for city, city_code in city_dict.items():
    for year in list_of_years:
        result.append({"City": city, "City_Code": city_code, "year": year})

# download the data
data_downloaded_list = []
for entry in result:
    data_downloaded_list.append(analysis_module.download_data(entry))


# prep the columns of the dataframe
df_list_col_prepped = [analysis_module.prep_df(this_file) for this_file in data_downloaded_list]


# do some analysis

new_df = pd.concat([analysis_module.calculate_monthly_avgs(this_df) for this_df in df_list_col_prepped]).reset_index(drop = True)


# plot data

for label, group in new_df.groupby(['station_name','year']):

    analysis_module.plot_data(label, group)

# End the timer
end_time = time.time()

# Calculate elapsed time
elapsed_time = end_time - start_time

# Print the results
print(f"Elapsed time: {elapsed_time:.6f} seconds")