# Import necessary libraries and modules
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
from bs4 import BeautifulSoup
import random, logging, re, os, zipfile
import pandas as pd

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 14),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

# Define the DAG with its parameters
dag = DAG(
    'weather_data_extraction_1',  # Name of the DAG
    default_args=default_args,
    description='A DAG to fetch and process weather data',  # Description of the DAG
    schedule=None,  # Schedule for the DAG, set to None for manual triggering
)

# Define variables for download path, base URL, and year
download_path = "/tmp"
base_url = "https://www.ncei.noaa.gov/data/local-climatological-data/access"
year = "2023"


# Function to fetch CSV files
def fetch_csv_files(**kwargs):
    selected_files = kwargs['ti'].xcom_pull(task_ids='select_files')  # Get selected files from XCom
    
    # Loop through selected files and download them using curl
    for file_name in selected_files:
        os.system(f"curl -o {download_path}/{file_name} {base_url}/{year}/{file_name}")

# Function to randomly select CSV files
def select_random_files(**kwargs):
    # Read HTML content from a file
    with open(f'{download_path}/{year}_data.html', 'r') as file:
        html_content = file.read()
    
    # Parse HTML content using BeautifulSoup
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Find all links ending with .csv extension
    links = soup.find_all('a', href=re.compile(r'.csv$'))
    
    # Extract CSV file names from the links
    csv_files = [link.get('href').split('/')[-1] for link in links]
    logging.info(f"Number of CSV files found: {len(csv_files)}")
    
    # Select 20 random files
    selected_files = random.sample(csv_files, 20)
    
    return selected_files

# Function to zip selected files
def zip_files(**kwargs):
    selected_files = kwargs['ti'].xcom_pull(task_ids='select_files')  # Get selected files from XCom
    
    # Create a zip file and add selected CSV files to it
    with zipfile.ZipFile(f"{download_path}/{year}_data.zip", "w") as zipf:
        for file_name in selected_files:
            Data = pd.read_csv(f"{download_path}/{file_name}")
            if 'DATE' in Data.columns:
                zipf.write(f"{download_path}/{file_name}", arcname=file_name)  # Add file to zip
                os.remove(f"{download_path}/{file_name}")  # Remove the original file
            else:
                logging.warning(f"DATE column not found in {file_name}. Skipping the file.")
                os.remove(f"{download_path}/{file_name}")  # Remove the file if DATE column not found

# Define tasks in the DAG

# Task to fetch HTML page containing CSV links
fetch_page_task = BashOperator(
    task_id='fetch_page',
    bash_command=f'curl -o {download_path}/{year}_data.html {base_url}/{year}/',
    dag=dag,
)

# Task to select random files
select_files_task = PythonOperator(
    task_id='select_files',
    python_callable=select_random_files,
    dag=dag,
)

# Task to fetch CSV files
fetch_files_task = PythonOperator(
    task_id='fetch_files',
    python_callable=fetch_csv_files,
    dag=dag,
)

# Task to zip selected files
zip_files_task = PythonOperator(
    task_id='zip_files',
    python_callable=zip_files,
    dag=dag,
)

new_directory = "/tmp/new_data_dir"  # Specify the new directory here

# Task to move the zip file to a specified location and create the directory if it doesn't exist
move_zip_file_task = BashOperator(
    task_id='move_zip_file',
    bash_command=f"mkdir -p {new_directory} && mv {download_path}/{year}_data.zip {new_directory}",
    dag=dag,
)

# Define task dependencies
fetch_page_task >> select_files_task >> fetch_files_task >> zip_files_task >> move_zip_file_task
