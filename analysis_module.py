import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import wget
import os
import time
import random
import warnings
from glob import glob
import re

# Suppress all warnings
warnings.filterwarnings('ignore')


def download_data(entry):

    city = entry['City']

    stationID = entry['City_Code']

    year = entry['year']

    http_str = f"https://climate.weather.gc.ca/climate_data/bulk_data_e.html?format=csv&stationID={stationID}&Year={year}&Month=1&Day=1&timeframe=2&submit=Download+Data"
    
    data_file = f'{city}_{year}.csv'

    wget_str = f'wget -O "{data_file}" --content-disposition "{http_str}"'

    sleep_time = random.randint(1, 3)

    time.sleep(sleep_time)

    os.system(wget_str)

    return(data_file)



def prep_df(this_file):

    df = pd.read_csv(this_file)

    # Function to clean column names
    # Function to clean column names
    def clean_column_name(col_name):
        # Find and capture any units in parentheses
        match = re.search(r'\((.*?)\)', col_name)
        unit = match.group(1) if match else ''
        
        # Remove the units in parentheses from the original column name
        col_name = re.sub(r'\s?\(.*\)', '', col_name)
        
        # Convert to lowercase and replace spaces with underscores
        col_name = col_name.lower().replace(' ', '_')
        
        # Remove the degree sign (°) if it exists
        col_name = col_name.replace('°', '')
        
        # Replace km/h with kmh (remove the slash)
        col_name = col_name.replace('km/h', 'kmh')
        
        # Special case for ' deg' to replace it with '_deg'
        col_name = re.sub(r'\s?deg$', '_deg', col_name)
        
        # If a unit was found, append it with an underscore to the column name
        if unit:
            # Remove any degree signs or symbols from the unit itself
            unit = unit.replace('°', '')
            unit = unit.replace('/', '')  # Remove any slashes from the unit
            col_name = f"{col_name}_{unit}"
        
        return col_name

    # Apply the function to all column names
    df.columns = [clean_column_name(col) for col in df.columns]

    df.rename(columns={'dir_of_max_gust_10s deg': 'dir_of_max_gust_10s_deg'}, inplace=True)

    df.rename(columns={'date/time': 'date_time'}, inplace=True)

    df['date_time'] = [pd.Timestamp(this_str_date_time, tz = 'utc') for this_str_date_time in df['date_time']]


    return(df)


def calculate_monthly_avgs(df):

    copy_df = df.copy()

    output_df_list = []

    for this_month in copy_df.month.unique():

        mask = copy_df[copy_df.month == this_month]

        mask['avg_max_temp_C'] = mask.max_temp_C.mean()

        mask['avg_min_temp_C'] = mask.min_temp_C.mean()

        mask['avg_mean_temp_C'] = mask.mean_temp_C.mean()

        output_df_list.append(mask)
    
    return(pd.concat(output_df_list))

def plot_data(label, group):

    plot_city_dict = {'CALGARY INTL A': 'Calgary',
    'MONTREAL/PIERRE ELLIOTT TRUDEAU INTL': 'Montreal',
    'VANCOUVER INTL A': 'Vancouver',
    'WINNIPEG INTL A': 'Winnipeg'}

    fig = plt.figure(figsize = (20,10))

    ax = fig.add_subplot(1,2,1)

    ax2 = fig.add_subplot(1,2,2)

    ax.plot(group.date_time, group.mean_temp_C, label = 'daily mean')

    ax.plot(group.date_time,group.max_temp_C, label  = 'daily max')

    ax.plot(group.date_time,group.min_temp_C, label = 'daily min')

    ax2.plot(group.date_time,group.avg_mean_temp_C, label = 'monthly daily-mean avg')
    ax2.plot(group.date_time,group.avg_max_temp_C, label = 'monthly max avg')
    ax2.plot(group.date_time,group.avg_min_temp_C, label = 'monthly min avg')

    ax.set_title(label)
    ax.legend()

    ax2.set_title(f'{label} averages')
    ax2.legend()

    city_name = plot_city_dict[label[0]]

    fig_name = f'{city_name}_{label[1]}_non_ruffus.jpg'

    ax.set_ylim(-30,40)
    ax2.set_ylim(-30,40)

    plt.savefig(fig_name)

def plot_data_ruffus(label, group):

    plot_city_dict = {'CALGARY INTL A': 'Calgary',
    'MONTREAL/PIERRE ELLIOTT TRUDEAU INTL': 'Montreal',
    'VANCOUVER INTL A': 'Vancouver',
    'WINNIPEG INTL A': 'Winnipeg'}

    fig = plt.figure(figsize = (20,10))

    ax = fig.add_subplot(1,2,1)

    ax2 = fig.add_subplot(1,2,2)

    ax.plot(group.date_time, group.mean_temp_C, label = 'daily mean')

    ax.plot(group.date_time,group.max_temp_C, label  = 'daily max')

    ax.plot(group.date_time,group.min_temp_C, label = 'daily min')

    ax2.plot(group.date_time,group.avg_mean_temp_C, label = 'monthly daily-mean avg')
    ax2.plot(group.date_time,group.avg_max_temp_C, label = 'monthly max avg')
    ax2.plot(group.date_time,group.avg_min_temp_C, label = 'monthly min avg')

    ax.set_title(label)
    ax.legend()

    ax2.set_title(f'{label} averages')
    ax2.legend()

    city_name = plot_city_dict[label[0]]

    fig_name = f'{city_name}_{label[1]}_ruffus.jpg'

    ax.set_ylim(-30,40)
    ax2.set_ylim(-30,40)

    plt.savefig(fig_name)