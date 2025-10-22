import os
import sys

# Save the original working directory
#original_directory = os.getcwd()

#os.chdir(r'X:\Prestige\Beauty\Data\001_DataProductDevelopment\Toolbox') #commented for testing phase #S:\Beauty_Data\Virtual_Environments\ToolBox
#os.chdir(r'S:\Beauty_Data\Virtual_Environments\ToolBox') #added during testing phase
#SCRIPTPATH = r'X:\Prestige\Beauty\Data\001_DataProductDevelopment\Toolbox\sql.py'
#sys.path.append(r'X:\Prestige\Beauty\Data\001_DataProductDevelopment\Toolbox')#commented for testing phase
#sys.path.append(r'X:\Prestige\Beauty\Data\001_DataProductDevelopment\Toolbox')#added during testing phase
#from sql import MSSQLClient
from bigquery_ops import BigQueryLoader
from google.oauth2 import service_account
import numpy as np
import pandas as pd
from datetime import datetime, date

key_path = r'key.json'#commented during testing phase ## Access BigQuery workspace through our credentialskey_path = 'S:\Beauty_Data\Virtual_Environments\ToolBox\key.json'credentials = service_account.Credentials.from_service_account_file(key_path)
#key_path = "C:\\Users\\Balaji.Muthuvelapp77\\OneDrive - THG\\DESKTOP\\Adhoc Task\\Propensity Model\\Toolbox\\agile-bonbon-662-53c306e3dce7.json"
#added during testing phase

credentials = service_account.Credentials.from_service_account_file(key_path)
PROJECT = 'thg-beauty'
bq_loader = BigQueryLoader(PROJECT, credentials)




def pull_run_id_hyper_params(site_key,country_name,prestige_region):
    """ this query will help in pulling the last best hyper parameter data for each combination"""
    run_id_level_data = f"""
                   select model_parameters from 
                   thg-beauty.Workspace_Sandbox.mode_parameters_performance_metrics me
                   where 
                   me.site_key = {site_key}
                   and me.Prestige_Region = '{prestige_region}'
                   and me.Country_Name = '{country_name}'
                   and me.runid = (select max(runid) from thg-beauty.Workspace_Sandbox.mode_parameters_performance_metrics
                   where 
                   site_key = {site_key}
                   and Prestige_Region = '{prestige_region}'
                   and Country_Name = '{country_name}')
                   """

    model_params = bq_loader.exec_bq_query(run_id_level_data)
    return model_params
    

    

    


# In[ ]:




