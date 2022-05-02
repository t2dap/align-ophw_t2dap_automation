import bcp
import datetime
from config import server_info, server_info_dev, database_testing, database_upload,load_table
import warnings
import logging
import time

import chunk
import time
import pandas as pd

import pyodbc

from pathlib import Path

# Disable the warnings
warnings.filterwarnings('ignore')

path_raw_files = '.\\daily_dump\\'
# path_raw_files=r'C:\Users\sartoria1\Desktop\prova\raw_files'
# print()
today = datetime.datetime.now().strftime('%Y-%m-%d')
filename = f'test_sync_{today}'
# filename = f'dump_{time}'




# connect to the database Clarity
def download_clarity(path_raw_files,filename, query_string, database):

    conn = bcp.Connection(host= server_info, driver='mssql')
    my_bcp = bcp.BCP(conn)

    path = Path(f'{path_raw_files}{filename}.csv')
    file = bcp.DataFile(file_path= path, delimiter='|')
    my_bcp.dump(query= query_string, output_file=file)





# connect to the database T2DAP
def create_server_connection(server, database):
    conn = None
    try:
        conn = pyodbc.connect(
                Trusted_Connection='Yes',
                Driver='{ODBC Driver 17 for SQL Server}',
                Server= server,
                Database= database
            )
        print('SQL Server Database connection successful')

    except pyodbc.Error as e:
        print(f'Error: {e}')

    return conn




def load_data(cursor, df, table):

    # Insert DataFrame to Table
    insert_to_tmp_tbl=f"INSERT INTO {table} VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
    
    cursor.fast_executemany = True
    cursor.executemany(insert_to_tmp_tbl, df.values.tolist())
    print(f'{len(df)} rows inserted to the {table} table')

    cursor.commit()





#entrypoint
if __name__ == '__main__':

    # ord_id_list = tuple(['309565523','353589637','353492972','324969721'])
    #READ CSV FILE MISSING DAT
    df_missing  = pd.read_csv('missing_data.csv')
   # select a column as series and then convert it into a list
    list_of_missing_ord = df_missing['order_procedure_id'].to_list()
    # print(list_of_missing_ord)
    ord_id_list = tuple(list_of_missing_ord[2501:5000])
    query_string = f'''SELECT LOC_NAME 
                ,FACILITY_ID
                ,DEPARTMENT_NAME
                ,DEPARTMENT_ID
                ,HOSPITAL_SERVICE
                ,PAT_ID 
                ,PAT_NAME
                , MRN
                ,CONTACT_DATE
                ,BIRTH_DATE
                ,ORDER_TIME
                ,ORD_RESULT_TIME
                , ORDER_PROC_ID
                ,SPECIMEN_TYPE
                , PROC_NAME
                , COVID19_PROC_TYPE
                , AGE
                , AGE_GROUP
                ,PATIENT_SEX 
                ,PATIENT_RACE
                ,PATIENT_LANGUAGE 
                ,RESULT
                ,ZIP_CODE
                ,HOSP_ADMSN_TIME
                ,INP_ADM_DATE
                ,HOSP_DISCH_TIME
                ,Death_Date
                ,Disch_Disposition
                ,Site_Type
                ,PRIMARY_PAYER_GROUP
                ,admit_dep_name
                ,Borough
                ,Neighborhood
        FROM [CovResponse].[Tableau].[V_COVID19_PCR_Testing_All_Care_Settings]
        WHERE ORDER_PROC_ID IN {ord_id_list}'''


    start = time.time()
    download_clarity(path_raw_files=path_raw_files,filename=filename, query_string=query_string, database=database_testing)
    stop = time.time()
    print(stop - start)
    # print(query_string)
    columns = ['loc_name', 'facility_id', 'department_name', 'department_id','hospital_service', 'pat_id', 'pat_name', 'mrn','contact_date', 
            'birth_date','order_time', 'ord_result_time', 'order_procedure_id','specimen_type', 'procedure_name', 'covid19_proc_type', 'age', 'age_group',
            'patient_sex', 'patient_race', 'patient_language', 'result',
            'zipcode','hosp_adm_time', 'inpatient_adm_date', 'hosp_disch_time',
            'death_date', 'discharge_disposition',  'site_type',
            'primary_payer_group', 'admit_dep_name', 
            'borough', 'neighborhood']

    connection_to_load_tdap = create_server_connection(server=server_info_dev, database=database_upload)
    cursor=connection_to_load_tdap.cursor()
    # start = time.time()
    path_filename = f'{path_raw_files}{filename}.csv'
    print(path_filename)
    chuncks = pd.read_csv(path_filename, delimiter='|' , dtype={"loc_name": str, "admit_dep_name":str ,"department_id":object ,"hospital_service":str}, names = columns, parse_dates=['birth_date','contact_date', 'ord_result_time','order_time','hosp_disch_time','inpatient_adm_date','hosp_adm_time','death_date'], chunksize= 10000)
    # df= pd.read_csv(path_filename, delimiter='|' , dtype={"loc_name": str, "admit_dep_name":str ,"department_id":object ,"hospital_service":str}, names = columns, nrows=50, parse_dates=['birth_date','contact_date', 'ord_result_time','order_time','hosp_disch_time','inpatient_adm_date','hosp_adm_time','death_date'])
    # ,  dtype={"salary": int}

    # print(len(df.loc[9]['primary_payer_group']))
    
    for ii, df in enumerate(chuncks):
        # print(df['order_procedure_id'])
        start = time.time()
        
        
        df = df.astype(object).where(pd.notnull(df), None)

        df['crtd_dt'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        # print(df.info())
        # df = df.fillna("NULL")
        # print(df.loc[0])
        # print(df['death_date'])
        # print(df['pat_name'])
        print(ii)
       
        load_data(cursor,df,table=load_table)
    
        stop = time.time()
        print('Time elapsed: ', stop - start)
    #     # print(df['mrn'].str.len().max())
    #     # break
    #     # if(ii == 5):
    #     #     break
    # cursor.close()
    # connection_to_load_tdap.close()