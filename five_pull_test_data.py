#!/usr/bin/env python
# coding: utf-8

# In[ ]:

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



def pull_customer_category_test_data(start_date_train,end_date_train,start_date_test,end_data_test,site_key,country_name,prestige_region,customer_key_set,user_mapping,category_mapping,reverse_user_mapping,
                                     reverse_category_mapping):
    
    '''Retrieves customer purchase data from a BigQuery database based on filters like date range, site, country, and region. 
    It extracts each customer's earliest order date, retains only their first purchases,
    maps categories to predefined labels, and reverses user/category mappings. 
    Finally, it groups the data by customer, returning a list of categories they interacted with which will be passed to the hyper parameter tuning data.'''
    
    #customer_keys = ', '.join(map(str,customer_key_set))- Thi is not working getting time out error 
    #display(customer_keys)
    # Include it in the SQL query
    # Over here i have considered only new_Category = 1 actually it should be the categories which they have not intereacted.
    # As the trainign data is one year, there can be chances, there are categories which they have already interacted with before one year.
    # If we want we change this and pull all the cateogries and remove the categories which they have interaced with
    # this was the code before 1-04-2025
    # test_level_data_q = f"""
    # SELECT 
    #     cp.Customer_Key,
    #     cp.Category,
    #     cp.Order_Date
    # FROM thg-beauty.Beauty_Data.Customer_Product cp
    # WHERE cp.Site_Key = {site_key}
    #     AND cp.Order_Date >= '{start_date_test}' and cp.Order_Date <= '{end_data_test}'
    #     AND cp.Units > 0 
    #     AND cp.Order_Status = 'Despatched'
    #     AND cp.IS_GWP != 1
    #     AND cp.Is_Full_Size_GWP != 1
    #     AND cp.Prestige_Region = '{prestige_region}'
    #     and cp.Country_Name = '{country_name}'
    #     AND cp.New_Category = 1 # to filter the customers purchasing the next new category
        
    # """
    #added the below line of code to make the code flexible to use us_cateogry and category
    category_column = "US_Category" if site_key == 166 else "Category" # switcing between us category and normal category
    test_level_data_q = f"""WITH train_data AS (
    SELECT 
        cp.Customer_Key,
        cp.{category_column} as Category
    FROM thg-beauty.Beauty_Data.Customer_Product cp
    WHERE cp.Site_Key = {site_key}
        AND cp.Order_Date BETWEEN '{start_date_train}' AND '{end_date_train}'
        AND cp.Units > 0
        AND cp.Order_Status = 'Despatched'
        AND cp.IS_GWP != 1
        AND cp.Is_Full_Size_GWP != 1
        AND cp.Prestige_Region = '{prestige_region}'
        AND cp.Country_Name = '{country_name}'
        AND cp.{category_column} IS NOT NULL

),
test_data AS (
    SELECT 
        cp.Customer_Key,
        cp.{category_column} AS Category,
        cp.Order_Date
    FROM thg-beauty.Beauty_Data.Customer_Product cp
    WHERE cp.Site_Key = {site_key}
        AND cp.Order_Date BETWEEN '{start_date_test}' AND '{end_data_test}'
        AND cp.Units > 0
        AND cp.Order_Status = 'Despatched'
        AND cp.IS_GWP != 1
        AND cp.Is_Full_Size_GWP != 1
        AND cp.Prestige_Region = '{prestige_region}'
        AND cp.Country_Name = '{country_name}'
        AND cp.{category_column} IS NOT NULL
)
SELECT 
    t.Customer_Key,
    t.Category,
    t.Order_Date
FROM test_data t
LEFT JOIN train_data tr 
    ON t.Customer_Key = tr.Customer_Key 
    AND t.Category = tr.Category
WHERE tr.Customer_Key IS NULL  -- Only keep categories the user has NOT interacted with in training period
"""


    #--AND cp.Customer_Key IN ({customer_keys})
    bq_loader = BigQueryLoader(PROJECT, credentials)
    test_level_data = bq_loader.exec_bq_query(test_level_data_q)
    #display('test_level_data_is_pulled')
    customer_key_set_2 = set(test_level_data['Customer_Key'])
    common_customers = customer_key_set & customer_key_set_2
    #display(len(common_customers ))
    test_level_data['common_customers'] = np.where(test_level_data['Customer_Key'].isin(common_customers),1,0)# this code was kepts as a part of old SQL code
    test_level_data = test_level_data[test_level_data['common_customers']==1]
    test_level_data.drop('common_customers',axis=1,inplace=True)
    test_level_data['Order_Date'] = pd.to_datetime(test_level_data['Order_Date'])
    test_level_data.sort_values(by=['Customer_Key','Order_Date'],inplace=True)
    rank_calculation = test_level_data.groupby('Customer_Key')['Order_Date'].min().reset_index()
    rank_calculation['rank'] = 1 # Over here we are trying to keep the rank 1 for every customer after the merge
    test_level_data= test_level_data.merge(rank_calculation,left_on=['Customer_Key','Order_Date'],right_on=['Customer_Key','Order_Date'],how='inner')
    test_level_data.drop('rank',inplace=True,axis=1)
    test_level_data.sort_values(by=['Customer_Key'],inplace=True)
    test_level_data.drop_duplicates(subset=['Customer_Key','Category','Order_Date'],inplace=True)
    #category_mapping_test = {'Skin':'skin_score', 'Body':'body_score', 'Cosmetics':'cosmetics_score','Hair':'hair_score',
     #  'Fragrance':'fragrance_score', 'Beauty Box':'beauty_box_score', 'Others':'others_score'}
    #display(test_level_data['Category'].unique())
    #display(test_level_data.head(5))

    #For test data we need reverse user_mapping and category mapping which was used in train_data
    def reverese_user_mapping(row):
        c_key = user_mapping[row]
        return c_key

    def reverese_category_mapping(row):
        c_key = category_mapping[row]
        return c_key
    

    def category_mapping_function(row, site_key):
        if site_key == 166:
            category_mapping_test = {
                'Haircare': 'hair_score',
                'Professional Skincare': 'professional_skincare_score',
                'Prestige Skincare': 'prestige_skincare_score',
                'Cosmetics': 'cosmetics_score',
                'Fragrance': 'fragrance_score',
                'Devices': 'devices_score',
                'Beauty Box': 'beauty_box_score'
            }
        else:
            category_mapping_test = {
                'Skin': 'skin_score',
                'Body': 'body_score',
                'Cosmetics': 'cosmetics_score',
                'Hair': 'hair_score',
                'Fragrance': 'fragrance_score',
                'Beauty Box': 'beauty_box_score',
                'Others': 'others_score'
            }

        return category_mapping_test.get(row, 'unknown_category')  # Optional fallback for safety


    
    #display(category_mapping)
    #display(test_level_data.head(5))

    #test_level_data['New_Category'] = test_level_data['Category'].apply(category_mapping_function)

    test_level_data['New_Category'] = test_level_data['Category'].apply(lambda row: category_mapping_function(row, site_key))


    test_level_data.drop(columns=['Category','Order_Date'],inplace=True)
    test_level_data.rename(columns={'New_Category':'Category'},inplace=True)

    #display('category_mapping_function completed')
    test_level_data['reverse_user_mapping_customer_key'] = test_level_data['Customer_Key'].apply(reverese_user_mapping)
    #display('reverese_user_function completed')
    #display(category_mapping)
    #display(test_level_data.head(5))
    test_level_data['reverse_cateogry_mapping_customer_key'] = test_level_data['Category'].apply(reverese_category_mapping)

    customer_key_mapping_values = test_level_data['reverse_user_mapping_customer_key'].unique()

    test_level_data_grouped = test_level_data.groupby(['Customer_Key','reverse_user_mapping_customer_key'])['Category'].apply(list).reset_index()
    
    # test_level_data_grouped.to_csv('test_level_data_grouped.csv')
    return test_level_data_grouped,customer_key_mapping_values
        

