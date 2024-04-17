from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from datetime import datetime
import os, glob, re, logging
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.colors import Normalize
from matplotlib.cm import ScalarMappable
from shapely.geometry import Point
import apache_beam as beam

# Load world map data
world_map = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 14),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

# Define the DAG
dag = DAG(
    'weather_data_pipeline',
    default_args=default_args,
    description='A DAG to process weather data',
)

# Sensor to wait for the archive file to appear
wait_for_archive_task = FileSensor(
    task_id='wait_for_archive',
    filepath='/tmp/new_data_dir/2020_data.zip',
    timeout=5,
    mode='poke',
    poke_interval=1,
    dag=dag,
)

# Define year and file paths
year = 2023
file_path = f'/tmp/new_data_dir/{year}_data/'
plot_path = '/tmp/new_data_dir/Plots/'
os.makedirs(plot_path, exist_ok=True)

# Task to unzip the archive
unzip_task = BashOperator(
    task_id='unzip_archive',
    bash_command=f'mkdir -p {file_path} && unzip -o /tmp/new_data_dir/{year}_data.zip -d {file_path}',
    dag=dag,
)

# Function to extract and filter data
def extract_and_filter(org_path):
    def read_csv_file(file_path):
        df = pd.read_csv(file_path,on_bad_lines='skip')
        cols = df.columns
        filtered_strings = [s for s in cols if re.match(r'^Hourly', s)]
        cols_to_keep = ['DATE', 'LATITUDE', 'LONGITUDE'] + filtered_strings
        df['DATE'] = df['DATE'].astype(str)
        processed_df = df[cols_to_keep].assign(DATE=df['DATE'].str[5:7])
        return file_path, processed_df
    
    file_pattern = org_path + '*.csv'
    with beam.Pipeline() as p:
        csv_files = (
            p| 'MatchFiles' >> beam.Create([file_pattern])
             | 'FindCSVFiles' >> beam.FlatMap(lambda pattern: glob.glob(pattern))
             | 'ReadCSVFiles' >> beam.Map(read_csv_file)
             | "Save_files" >> beam.Map(lambda file: file[1].to_csv(file[0], index=False)))

# Function to compute monthly averages
def compute_monthly_averages(org_path):

    def read_csv_file(file_path):
        df = pd.read_csv(file_path,on_bad_lines='skip')
        df['DATE'] = df['DATE'].astype(int)
        df = df.groupby('DATE').mean(numeric_only=True).reset_index()
        return file_path, df
    
    file_pattern = org_path + '*.csv'
    with beam.Pipeline() as p:
        csv_files = (
            p| 'MatchFiles' >> beam.Create([file_pattern])
             | 'FindCSVFiles' >> beam.FlatMap(lambda pattern: glob.glob(pattern))
             | 'ReadCSVFiles' >> beam.Map(read_csv_file)
             | 'Saving' >> beam.Map(lambda file: file[1].to_csv(file[0], index=False)))

# Function to combine data from multiple CSV files
def combine_data(file_path):
    # Get list of files in the directory
    files=os.listdir(file_path)
    # Read the first file to get column names
    sample_file=os.path.join(file_path,files[0])
    data=pd.read_csv(sample_file)
    cols=data.columns
    month=[1,2,3,4,5,6,7,8,9,10,11,12]

    # Loop through all files to find common columns and selected month
    for file in files:
        path=os.path.join(file_path,file)
        data=pd.read_csv(path)
        date=data['DATE'].values
        if len(date)>1 and len(list(set(month) & set(date)))>0 and len(list(set(cols) & set(data.columns))) > 7:
            column=data.columns
            cols=list(set(cols) & set(column))
            month=list(set(month) & set(date))

    # Get the selected month
    selected_month=month[0]
    
    # Initialize an empty DataFrame to store merged data
    merged_df = pd.DataFrame(columns=cols)
    
    # Merge data from all files into one DataFrame
    for file in files:
        path=os.path.join(file_path,file)
        data=pd.read_csv(path)
        columns=data.columns
        if not set(cols).issubset(set(columns)):
            os.remove(path)
            continue

        data=data[cols]
        if len(data['DATE'].values)>1:
            data=data[data['DATE']==selected_month]
            merged_df = pd.concat([merged_df,data], ignore_index=True,axis=0)
        os.remove(path)

    # Save the merged DataFrame to a CSV file
    outpath=f'/tmp/new_data_dir/combined_data/{year}_comb.csv'
    merged_df.to_csv(outpath,index=False) 

# File path for the combined data CSV
geo_map_csv=f'/tmp/new_data_dir/combined_data/{year}_comb.csv'

# Function to generate geomaps
def get_geomap(file_path):
    def read_csv(file_path):
        Data=pd.read_csv(file_path)
        return Data
    
    def create_plot(data):
        # List of months
        month=['Jan','Feb','March','Apr','May','Jun','July','Aug','Sep','Oct','Nov','Dec']
        # Get the selected month
        month_idx=data['DATE'].values[0]
        sel_month=month[month_idx-1]
        # Drop unnecessary columns
        data.drop('DATE',axis=1,inplace=True)
        columns=data.columns
        cols_to_del=['LATITUDE','LONGITUDE',]
        columns=[i for i in columns if i not in cols_to_del]
        # Create geometry for plotting
        geometry = [Point(xy) for xy in zip(data['LONGITUDE'], data['LATITUDE'])]
        for col in columns:
            gdf = gpd.GeoDataFrame(np.abs(data[col]), geometry=geometry)
            fig, ax = plt.subplots(figsize=(20, 20))
            world_map.plot(ax=ax, color="lightyellow",edgecolor='black')
            gdf.plot(ax=ax, c=col, cmap='gnuplot_r', legend=True, markersize=gdf[col] * 30.0,alpha=0.6)
            norm = Normalize(vmin=gdf[col].min(), vmax=gdf[col].max())
            sm = ScalarMappable(norm=norm, cmap='gnuplot_r')
            sm.set_array([])
            plt.title(f"{year} Data for month {sel_month}",fontsize=30,weight='bold')
            plt.xticks(fontsize=15)
            plt.yticks(fontsize=15)
            cbar = plt.colorbar(sm, ax=ax, orientation='horizontal', aspect=50, pad=0.05)
            cbar.set_label(label=col,weight='bold',size=15)
            out_path=plot_path+f'{year}_{col}.png'
            plt.savefig(out_path)

    # Use Apache Beam to process data in parallel
    with beam.Pipeline() as p:
        processing = (
            p| 'MatchFiles' >> beam.Create([file_path])
             | 'ReadCSVFiles' >> beam.Map(read_csv)
             | 'CreateGeomap' >> beam.Map(create_plot)
        )
    
# Define task for extracting and filtering data
extract_and_filter_data = PythonOperator(task_id='extract_and_filter_data',
                                         python_callable=extract_and_filter,
                                         op_args=[file_path],
                                         dag=dag)

# Define task for computing monthly averages
compute_averages = PythonOperator(task_id='compute_averages',
                                  python_callable=compute_monthly_averages,
                                  op_args=[file_path],
                                  dag=dag)

# Define task for combining data
combine_data_pipe = PythonOperator(task_id='Comb_data_loc',
                                   python_callable=combine_data,
                                   op_args=[file_path],
                                   dag=dag)

# Define task for generating geomaps
geo_map_pipe = PythonOperator(task_id='Geo_map',
                              python_callable=get_geomap,
                              op_args=[geo_map_csv],
                              dag=dag)


# Define task dependencies
wait_for_archive_task >> unzip_task >> extract_and_filter_data >> compute_averages >> combine_data_pipe >> geo_map_pipe
