#!/usr/bin/env python
# coding: utf-8
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

# Path to CSV
#pth = r'X:\Prestige\Beauty\Data\002_TradingAndMarketing\2024\007_Beauty_Edits_GB_Report\skus file\edits_gb_sku.csv' 

# Read CSV into a dataframe
#df = pd.read_csv(pth)
#run_id,best_params,best_hit_rate,site_key,bu,country_name,prestige_region
def append_model_parameter(run_id,best_params,best_hit_rate,site_key,bu,country_name,prestige_region,project_name,model_parameters_table_name):
    df = pd.DataFrame({'runid':[int(run_id)],'model_parameters':[best_params],'model_performance':[float(best_hit_rate)],'Site_Key':[site_key],
                       'Business_Unit':[bu],'Country_Name':[country_name],'Prestige_Region':[prestige_region]})
    PROJECT = project_name
    #display(df)
    #print(df.shape)
    #print(df.columns)
    #print(df.head())
    #print(df.dtypes)
    bq_loader.nero_to_bq(df, model_parameters_table_name, if_exists='append')

    # Reset to the original directory
#print(f"Directory reset to: {original_dir}")
    



# Check the dataframe and change name of column and datatypes if needed


# df['Edits_Goody_SKU'].value_counts()

# ## Change data type if needed ##
# df['Edits_Goody_SKU'] = df['Edits_Goody_SKU'].astype('int') # int, str, float

# df['Brand_Key'] = df['Brand_Key'].fillna(-1)

# df['Brand_Key'].value_counts()

## Rename column if needed ##
# df = df.rename(columns={"PostCodes": "Postcode"})
# print(df.head())

# Push to BigQuery 


