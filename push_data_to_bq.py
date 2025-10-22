#!/usr/bin/env python
# coding: utf-8

# In[ ]:


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

# Function to get top 2 recommendations for the already intreacted categories by the customer
#Existing score_columns# In future if the categories are getting added, we need to update it here

def get_top_categories(row, site_key):
    # Use correct score columns based on site_key
    score_cols = ['beauty_box_score', 'skin_score', 'body_score', 'cosmetics_score', 'hair_score', 'fragrance_score', 'others_score']
    score_cols_us = ['beauty_box_score', 'cosmetics_score', 'hair_score', 'professional_skincare_score', 'prestige_skincare_score', 'fragrance_score', 'devices_score']
    
    selected_scores = score_cols_us if site_key == 166 else score_cols

    # Build scores dict and sort
    scores = {col: row[col] for col in selected_scores}
    sorted_scores = sorted(scores.items(), key=lambda x: x[1], reverse=True)

    # Get top categories where score > 0
    top_categories = [cat.replace('_score', '') for cat, score in sorted_scores if score > 0]

    recommendation_1 = top_categories[0] if len(top_categories) > 0 else None
    recommendation_2 = top_categories[1] if len(top_categories) > 1 else None

    return pd.Series([recommendation_1, recommendation_2])




def customer_categories(row, site_key):
    '''This function checks which categories customers interacted with and what they need to interact with'''
    purchased = []
    not_purchased = []

    # Only define the one you need based on site_key
    if site_key == 166:
        selected_categories = {
            'Beauty_Box': row['beauty_box_score'],
            'Cosmetics': row['cosmetics_score'],
            'Hair': row['hair_score'],
            'Fragrance': row['fragrance_score'],
            'Professional_Skincare': row['professional_skincare_score'],
            'Prestige_Skincare': row['prestige_skincare_score'],
            'Devices': row['devices_score']
        }
    else:
        selected_categories = {
            'Beauty_Box': row['beauty_box_score'],
            'Skin': row['skin_score'],
            'Body': row['body_score'],
            'Cosmetics': row['cosmetics_score'],
            'Hair': row['hair_score'],
            'Fragrance': row['fragrance_score'],
            'Others': row['others_score']
        }

    for category, score in selected_categories.items():
        if score > 0:
            purchased.append(category)
        else:
            not_purchased.append(category)

    return pd.Series({
        'categories_purchased': "_".join(purchased) if purchased else "No_Purchase",
        'categories_not_purchased': "_".join(not_purchased) if not_purchased else "All_Categories_Purchased"
    })


# # Apply function while keeping other columns
# df[['Recommendation_1_existing_categories', 'Recommendation_2_existing_categories']] = df.apply(get_top_categories, axis=1)


def df_to_bq(user_level_data_dataset,site_key,bu,country_name,prestige_region,model_results_table_name):
    """ this query will replace the existing data with the new data """
    #the below query is for removing the contents in the table
    deleted_query = f""" DELETE from thg-beauty.Workspace_Sandbox.propensity_category
    where 
    site_key = {site_key}
    and Prestige_Region = '{prestige_region}'
    and Country_Name = '{country_name}';"""
    bq_loader.exec_bq_query(deleted_query)
    # Next step is uploading the data based on the recent run
    bq_loader.nero_to_bq(user_level_data_dataset, model_results_table_name, if_exists='append')




