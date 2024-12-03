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
import re
import pickle

from ruffus import *
import time

# Start the timer
start_time = time.time()



# input list of citys
city_dict = {"Calgary": 50430}
# city_dict = {"Calgary": 50430,
#              "Montreal": 30165,
#             "Vancouver": 51442,
#             "Winnipeg": 51097}

# input list of years
list_of_years = [2022]

result = []
# Loop through the dictionary keys and list of years
for city, city_code in city_dict.items():
    for year in list_of_years:
        result.append({"City": city, "City_Code": city_code, "year": year})


# function that takes every config in result and passes it to
# the "file creator" (AKA @files decorator) in order to download
def dl_params():
    for config in result:

        infile = None

        city, year = config['City'], config['year']

        # establish the first ruffus outfile that will be created from scratch

        outfile = f'{city}_{year}.downloaded'

        yield(infile, outfile, config)



@files(dl_params) # @files will create a file from no-files made
def download_data(infile, outfile, config):

    downloaded_file_name = analysis_module.download_data(config)

    output_df = pd.DataFrame([{'downloaded_file_name': downloaded_file_name}])

    print(infile, outfile, config, output_df.columns, downloaded_file_name)

    pickle.dump(output_df, open(outfile,'wb'))

# if __name__ == '__main__':

#     pipeline_run([download_data], verbose = 2)


@transform(download_data, suffix('downloaded'), 'fixed_columns.pickle')
def fix_columns(infile, outfile):

    infile_df = pickle.load(open(infile, 'rb'))

    csv_file_name = infile_df.iloc[0].downloaded_file_name

    prepped_df = analysis_module.prep_df(csv_file_name)

    print(infile, outfile, )

    pickle.dump(prepped_df, open(outfile, 'wb'))

# if __name__ == '__main__':

#     pipeline_run([fix_columns], verbose = 2)


@subdivide(fix_columns, formatter(),
            # Output parameter: Glob matches any number of output file names
            "{path[0]}/{basename[0]}.*.subdivide_monthly.pickle",
            # Extra parameter:  Append to this for output file names
            "{path[0]}/{basename[0]}")
def divide_by_month(infile, outfiles, output_file_name_root):

    infile_df = pickle.load(open(infile, 'rb'))

    print(infile)

    for month_number in infile_df.month.unique():

        copy_df = infile_df.copy()

        mask_df_for_output_by_month = copy_df[copy_df.month == month_number]

        # output_file_name_root == /path/to/where/you/are/working/{city}_{year}.fixed_columns
        # note: the output_file_name_root is the previous outfile without the extension "pickle"

        output_file_name = f'{output_file_name_root}.{month_number}.subdivide_monthly.pickle'

        print(output_file_name)

        pickle.dump(mask_df_for_output_by_month, open(output_file_name, 'wb'))

# if __name__ == '__main__':

#     pipeline_run([divide_by_month], verbose = 2)


@transform(divide_by_month, suffix('.subdivide_monthly.pickle'), '.monthly_analysis.pickle')
def monthly_analysis(infile, outfile):

    infile_df = pickle.load(open(infile, 'rb'))

    output_df = analysis_module.calculate_monthly_avgs(infile_df)

    pickle.dump(output_df, open(outfile, 'wb'))

# if __name__ == '__main__':

#     pipeline_run([monthly_analysis], verbose = 2)


@collate(monthly_analysis, regex(r'([A-Za-z]+_\d{4}).fixed_columns.\d{0,2}.monthly_analysis.pickle'), r'\1.joined_monthly_analysis.pickle')
def join_monthly_analysis(infiles, outfile):

    print(infiles, outfile)

    joined_input_df = pd.concat([pickle.load(open(infile, 'rb')) for infile in infiles])

    sorted_df = joined_input_df.sort_values(by = 'date_time')

    pickle.dump(sorted_df, open(outfile, 'wb'))

# if __name__ == '__main__':

#     pipeline_run([join_monthly_analysis], verbose = 2)


@transform(join_monthly_analysis, suffix('joined_monthly_analysis.pickle'), 'plots_made.pickle')
def make_plots(infile, outfile):

    df  = pickle.load(open(infile, 'rb'))

    for label, group in df.groupby(['station_name','year']):

        analysis_module.plot_data_ruffus(label, group)

    output_df = pd.DataFrame([{'plots_made': True}])   

    pickle.dump(output_df, open(outfile, 'wb')) 


if __name__ == '__main__':

    pipeline_run([make_plots], verbose = 2, multiprocess= 8)

    pipeline_printout_graph("flowchart.jpg", "jpg", [make_plots])

    # End the timer
    end_time = time.time()

    # Calculate elapsed time
    elapsed_time = end_time - start_time

    # Print the results
    print(f"Elapsed time: {elapsed_time:.6f} seconds")


