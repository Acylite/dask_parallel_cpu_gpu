
from google.cloud import bigquery
import pandas as pd
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
import re
from google.auth import default
import dask.dataframe as dd
from dask import delayed
import dask
from dask.distributed import Client, LocalCluster, wait, as_completed, Queue, fire_and_forget
import math
import dask.dataframe as dd
from dask_cuda import LocalCUDACluster
import time


def authenticate_gdrive_gsheets():
    credentials, project = default(scopes=[
        'https://www.googleapis.com/auth/drive',
        'https://www.googleapis.com/auth/bigquery',
        'https://www.googleapis.com/auth/cloud-platform'
    ])
    drive_service = build('drive', 'v3', credentials = credentials)
    
    return drive_service



def list_folders_in_folder(drive_service, folder_id):
    query = f"'{folder_id}' in parents and mimeType='application/vnd.google-apps.folder'"
    page_token = None
    folders = []
    while True:
        
        results = drive_service.files().list(
        pageSize=100,
        q = query,
        pageToken=page_token,
        fields="nextPageToken, files(id, name)").execute()
        folders.extend(results.get('files', []))
        page_token = results.get('nextPageToken', None)
        # Check if there is a next page
        if page_token is None:
            break
    
    return folders

def search_csv_files(drive_service, folder_id=None):
    query = "mimeType='text/csv'"
    if folder_id:
        query += f" and '{folder_id}' in parents"
    results = drive_service.files().list(q=query, fields="files(id, name)").execute()
    files = results.get('files', [])
    return files

def download_csv_file(drive_service, file_id):
    request = drive_service.files().get_media(fileId=file_id)
    fh = io.BytesIO()

    downloader = MediaIoBaseDownload(fh, request)
    
    done = False
    while not done:
        status, done = downloader.next_chunk()

    fh.seek(0)
    try:
        df = pd.read_csv(fh)
        df = dd.from_pandas(df)
    except pd.errors.EmptyDataError:
        df = dd.from_pandas(pd.DataFrame())
    return df

# Write data to a Google Sheet
def write_to_sheets(sheets_service, spreadsheet_id, range_name, dataframe):
    # Convert dataframe to a list of lists
    values = dataframe.values.tolist()
    
    body = {
        'values': values
    }
    result = sheets_service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id, range=range_name,
        valueInputOption="RAW", body=body).execute()

    return result

def process_csv_file(file, drive_service):
    
    df = download_csv_file(drive_service, file['id'])
    # Clean column names
    df.columns = [re.sub(r'[^a-zA-Z0-9]', '_', j) for j in list(df.columns)]
    
    if len(df.column) > 0:
        for column in df.columns:
            df[column] = df[column].astype(str)

        return df
    else:
        return dd.from_pandas(pd.DataFrame())

def process_folder(folder_details):
    folder_id = folder_details["id"]
    print("Processing folder " + folder_id)
    # Create the drive_service inside the functioncond
    drive_service = authenticate_gdrive_gsheets()
    csv_files = search_csv_files(drive_service, folder_id)
    
    if not csv_files:
        print(f"No CSV files found in folder: {folder_id}")
    for file in csv_files:
        return process_csv_file(file, drive_service)
    
def process_futures_concurrently(futures):
    dfs = []
    for df in futures:
        try:
            if len(df.column) > 0:
                dfs.append(df)
        except Exception as e:
            print(f"An error occurred while processing the future: {e}")
    if len(dfs) > 0:
        big_df = dd.concat(dfs)
        upload_to_bigquery(big_df)

def upload_to_bigquery(df):
    try:
        if len(df):
            df = df.compute()
            bigquery_client = bigquery.Client()
            table = bigquery_client.get_table("thomas-425620.Company.company_data")
            config = bigquery.LoadJobConfig()
            config.autodetect = True
            config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
            config.schema_update_options = [
                bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
            ]
            bigquery_client.load_table_from_dataframe(df, table, job_config=config).result()
            print("Upload successful")
        else:
            print("DataFrame is empty, skipping upload.")
    except Exception as e:
        print(f"An error occurred during upload: {e}") 

if __name__ == "__main__":
    # CPU Cluster
    cpu_cluster = LocalCluster(n_workers=3, threads_per_worker=4) 

    # GPU Cluster
    gpu_cluster = LocalCUDACluster()

    # Connect to both clusters
    cpu_client = Client(cpu_cluster)

    gpu_client = Client(gpu_cluster)
    
    dask.config.set({"dataframe.backend": "cudf"})
    parent_folder = '1FcN3O-uCGFyG_fGpnyzKf4j0dxFYm8hz' 
    drive_service = authenticate_gdrive_gsheets()
    print('printing')
    folders = list_folders_in_folder(drive_service, parent_folder)
    print('finished')
    
    print('working')
    bigquery_client = bigquery.Client()
    
    # (Optional) Folder ID if you're searching in a specific folder
    # Replace with actual folder ID if needed

    batch_size = 350
    num_batches = math.ceil(len(folders) / batch_size)
    for i in range(num_batches):
        batch_folders = folders[i * batch_size:(i + 1) * batch_size]
        print(f"Processing batch {i + 1}/{num_batches} with {len(batch_folders)} folders.")
        
        # Run process_folder for each folder in the current batch
        futures = []
        for ind, folder in enumerate(batch_folders, start=0):  
            if ind % 4 == 0:
                gpu_task = gpu_client.submit(process_folder, folder)
                futures.append(gpu_task)
            else:
                cpu_task = cpu_client.submit(process_folder, folder)
                futures.append(cpu_task)
        results = []
        for future in as_completed(futures):
            try:
                result = future.result()  # This ensures folder processing is done
                results.append(result)
            except Exception as e:
                print(f"Error processing or uploading folder: {e}")
        wait(futures, return_when = "ALL_COMPLETED")
        process_futures_concurrently(results)