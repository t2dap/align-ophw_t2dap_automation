import bcp
import datetime
from config import server_info, server_info_dev, database_testing, database_upload
import warnings
import logging
import time

import chunk
import time
import pandas as pd

import pyodbc

from pathlib import Path

import multiprocessing
# print(multiprocessing.cpu_count())

from pathlib import Path

# Disable the warnings
warnings.filterwarnings('ignore')

path_raw_files = '.\\check\\'
# path_raw_files=r'C:\Users\sartoria1\Desktop\prova\raw_files'
# print()
today = datetime.datetime.now().strftime('%Y-%m-%d')
filename = f'check_clarity_{today}'
filename_dap=f'check_dap_{today}'
# filename = f'dump_{time}'


# connect to the database Clarity and download the data dump from the assigned table
def download_clarity(path_raw_files,filename, database):

    conn = bcp.Connection(host= server_info, driver='mssql')
    my_bcp = bcp.BCP(conn)

    path = Path(f'{path_raw_files}{filename}.csv')
    file = bcp.DataFile(file_path= path, delimiter='|')
    my_bcp.dump(query= f'''SELECT ORDER_TIME,ORDER_PROC_ID
        FROM [CovResponse].[Tableau].[V_COVID19_PCR_Testing_All_Care_Settings]''', output_file=file)

#  connect to the database T2DAP and download the data dump from the assigned table
def download_tdap(path_raw_files,filename, database):

    conn = bcp.Connection(host= server_info_dev, driver='mssql')
    my_bcp = bcp.BCP(conn)

    path = Path(f'{path_raw_files}{filename}.csv')
    file = bcp.DataFile(file_path= path, delimiter='|')
    my_bcp.dump(query= f'''SELECT ORDER_TIME, order_procedure_id
                FROM [T2DAP].[ven].[covid19_pcr_testing_all_care_settings]''', output_file=file)

def create_output_file(path_raw_files,filename):

    columns = ['order_time', 'order_procedure_id']
    path_filename = f'{path_raw_files}{filename}.csv'
    raw_df = pd.read_csv(path_filename, delimiter='|' ,
                            names = columns,
                            parse_dates=['order_time'])
    clarity_df =raw_df.replace('|', ' ') # this is redundant
    return clarity_df


def create_output_file_dap(path_raw_files,filename):

    columns = ['order_time', 'order_procedure_id']
    path_filename = f'{path_raw_files}{filename}.csv'
    raw_df = pd.read_csv(path_filename, delimiter='|' ,
                            names = columns,
                            parse_dates=['order_time'])
    dap_df =raw_df.replace('|', ' ') # this is redundant
    return dap_df

def merge_df(clarity_df,dap_df):
    merged_df=pd.merge(clarity_df,dap_df,on ='order_procedure_id', how='left')
    missing_data=merged_df[merged_df['order_time_y'].isna()].sort_values(by='order_time_x', ascending=True).reset_index(drop=True)
    missing_data['date']= missing_data['order_time_x'].dt.date
    missing_data.to_csv('missing_data.csv')
    print(missing_data)

    # print(missing_data[missing_data['order_time_x']=='2022-03-14'])

if __name__ == '__main__':


    '''run function to download bulk data from ophw database'''

    start = time.time()
    download_clarity(path_raw_files=path_raw_files,filename=filename, database=database_testing)
    stop = time.time()
    print(stop - start)

    # '''run function to upload bulk data to T2DAP database'''

    start = time.time()
    download_tdap(path_raw_files=path_raw_files,filename=filename_dap, database=database_upload)
    stop = time.time()
    print(stop - start)

    # columns = ['order_time', 'order_procedure_id']

    # connection_to_load_tdap = create_server_connection(server=server_info_dev, database=database_upload)
    # cursor=connection_to_load_tdap.cursor()
    # start = time.time()
    filename = f'check_clarity_{today}'
    filename_dap=f'check_dap_{today}'
    path_filename = f'{path_raw_files}{filename}.csv'
    path_filename_dap = f'{path_raw_files}{filename_dap}.csv'
   
    clarity_df = create_output_file(path_raw_files=path_raw_files,filename=filename)
    dap_df = create_output_file_dap(path_raw_files=path_raw_files,filename=filename_dap)

    merge_df(clarity_df,dap_df)