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


def pull_customer_category_data(start_date_train, end_date_train, site_key, country_name, prestige_region):
    if site_key == 166:
        category_field = "cp.US_Category"
        categories = [
            "Haircare", "Professional Skincare", "Prestige Skincare",
            "Cosmetics", "Fragrance", "Devices", "Beauty Box"
        ]
    else:
        category_field = "cp.Category"
        categories = [
            "Beauty Box", "Skin", "Body", "Cosmetics",
            "Hair", "Fragrance", "Others"
        ]

    # Dynamically SQL CASE WHEN blocks for each metric
    def gen_case_blocks(field, category):
        clean = category.replace(" ", "_").replace("-", "_")
        return f"""
        SUM(CASE WHEN {field} = '{category}' THEN 1 ELSE 0 END) AS {clean},
        SUM(CASE WHEN {field} = '{category}' THEN cp.Units ELSE 0 END) AS {clean}_Units,
        COUNT(DISTINCT CASE WHEN {field} = '{category}' THEN cp.Order_Number END) AS {clean}_Order_Count,
        SUM(CASE WHEN {field} = '{category}' THEN cp.RRP ELSE 0 END) AS {clean}_RRP,
        SUM(CASE WHEN {field} = '{category}' THEN cp.List_Price ELSE 0 END) AS {clean}_List_Price,
        SUM(CASE WHEN {field} = '{category}' THEN cp.Total_Revenue ELSE 0 END) AS {clean}_Revenue,
        SUM(CASE WHEN {field} = '{category}' THEN cp.Product_Revenue ELSE 0 END) AS {clean}_product_Revenue,
        DATE_DIFF(CURRENT_DATE, MAX(CASE WHEN {field} = '{category}' AND cp.Units > 0 THEN cp.Order_Date ELSE NULL END), DAY) AS {clean}_Recency_Days
        """

    category_metrics_sql = ",\n".join([gen_case_blocks(category_field, cat) for cat in categories])

    query = f"""
    SELECT 
        cp.Customer_Key,
        {category_metrics_sql},
        SUM(cp.Total_Revenue) AS total_revenue,
        COUNT(DISTINCT cp.Order_Number) AS no_of_orders
    FROM thg-beauty.Beauty_Data.Customer_Product cp
    LEFT JOIN (
        SELECT customer_key 
        FROM thg-beauty.Customer_Segmentations.Bulk_Buyer_class 
        WHERE bb_flag = 1
    ) bb ON cp.Customer_Key = bb.customer_key
    WHERE cp.Site_Key = {site_key}
        AND cp.order_date >= '{start_date_train}' AND cp.order_date <= '{end_date_train}'
        AND cp.Units > 0 
        AND cp.Order_Status = 'Despatched'
        AND cp.IS_GWP != 1
        AND cp.Is_Full_Size_GWP != 1
        AND cp.Prestige_Region = '{prestige_region}'
        AND cp.Country_Name = '{country_name}'
        AND bb.customer_key IS NULL
    GROUP BY cp.Customer_Key
    HAVING no_of_orders > 1
    """

    user_level_data_dataset = bq_loader.exec_bq_query(query)
    # user_level_data_dataset.to_csv('user_level_data_dataset.csv')
    return user_level_data_dataset



#Old code changed on 07-04-2025 above code is the new one
# def pull_customer_category_data(start_date_train,end_date_train,site_key,country_name,prestige_region):
#     user_level_data =f"""
#    SELECT 
#     cp.Customer_Key,
#     -- Category No of times this category was bought-- 
#     SUM(CASE WHEN cp.Category = 'Beauty Box' THEN 1 ELSE 0 END) AS Beauty_Box,
#     SUM(CASE WHEN cp.Category = 'Skin' THEN 1 ELSE 0 END) AS Skin,
#     SUM(CASE WHEN cp.Category = 'Body' THEN 1 ELSE 0 END) AS Body,
#     SUM(CASE WHEN cp.Category = 'Cosmetics' THEN 1 ELSE 0 END) AS Cosmetics,
#     SUM(CASE WHEN cp.Category = 'Hair' THEN 1 ELSE 0 END) AS Hair,
#     SUM(CASE WHEN cp.Category = 'Fragrance' THEN 1 ELSE 0 END) AS Fragrance,
#     SUM(CASE WHEN cp.Category = 'Others' THEN 1 ELSE 0 END) AS Others,
#     -- Units-- no of units was bought in the cateogry 
#     SUM(CASE WHEN cp.Category = 'Beauty Box' THEN cp.Units ELSE 0 END) AS Beauty_Box_Units,
#     SUM(CASE WHEN cp.Category = 'Skin' THEN cp.Units ELSE 0 END) AS Skin_Units,
#     SUM(CASE WHEN cp.Category = 'Body' THEN cp.Units ELSE 0 END) AS Body_Units,
#     SUM(CASE WHEN cp.Category = 'Cosmetics' THEN cp.Units ELSE 0 END) AS Cosmetics_Units,
#     SUM(CASE WHEN cp.Category = 'Hair' THEN cp.Units ELSE 0 END) AS Hair_Units,
#     SUM(CASE WHEN cp.Category = 'Fragrance' THEN cp.Units ELSE 0 END) AS Fragrance_Units,
#     SUM(CASE WHEN cp.Category = 'Others' THEN cp.Units ELSE 0 END) AS Others_Units,
#     -- Distinct Order Numbers per Category
#     COUNT(DISTINCT CASE WHEN cp.Category = 'Beauty Box' THEN cp.Order_Number END) AS Beauty_Box_Order_Count,
#     COUNT(DISTINCT CASE WHEN cp.Category = 'Skin' THEN cp.Order_Number END) AS Skin_Order_Count,
#     COUNT(DISTINCT CASE WHEN cp.Category = 'Body' THEN cp.Order_Number END) AS Body_Order_Count,
#     COUNT(DISTINCT CASE WHEN cp.Category = 'Cosmetics' THEN cp.Order_Number END) AS Cosmetics_Order_Count,
#     COUNT(DISTINCT CASE WHEN cp.Category = 'Hair' THEN cp.Order_Number END) AS Hair_Order_Count,
#     COUNT(DISTINCT CASE WHEN cp.Category = 'Fragrance' THEN cp.Order_Number END) AS Fragrance_Order_Count,
#     COUNT(DISTINCT CASE WHEN cp.Category = 'Others' THEN cp.Order_Number END) AS Others_Order_Count,
#     -- RRP
#     SUM(CASE WHEN cp.Category = 'Beauty Box' THEN cp.RRP ELSE 0 END) AS Beauty_Box_RRP,
#     SUM(CASE WHEN cp.Category = 'Skin' THEN cp.RRP ELSE 0 END) AS Skin_RRP,
#     SUM(CASE WHEN cp.Category = 'Body' THEN cp.RRP ELSE 0 END) AS Body_RRP,
#     SUM(CASE WHEN cp.Category = 'Cosmetics' THEN cp.RRP ELSE 0 END) AS Cosmetics_RRP,
#     SUM(CASE WHEN cp.Category = 'Hair' THEN cp.RRP ELSE 0 END) AS Hair_RRP,
#     SUM(CASE WHEN cp.Category = 'Fragrance' THEN cp.RRP ELSE 0 END) AS Fragrance_RRP,
#     SUM(CASE WHEN cp.Category = 'Others' THEN cp.RRP ELSE 0 END) AS Others_RRP,
#     -- List Price
#     SUM(CASE WHEN cp.Category = 'Beauty Box' THEN cp.List_Price ELSE 0 END) AS Beauty_Box_List_Price,
#     SUM(CASE WHEN cp.Category = 'Skin' THEN cp.List_Price ELSE 0 END) AS Skin_List_Price,
#     SUM(CASE WHEN cp.Category = 'Body' THEN cp.List_Price ELSE 0 END) AS Body_List_Price,
#     SUM(CASE WHEN cp.Category = 'Cosmetics' THEN cp.List_Price ELSE 0 END) AS Cosmetics_List_Price,
#     SUM(CASE WHEN cp.Category = 'Hair' THEN cp.List_Price ELSE 0 END) AS Hair_List_Price,
#     SUM(CASE WHEN cp.Category = 'Fragrance' THEN cp.List_Price ELSE 0 END) AS Fragrance_List_Price,
#     SUM(CASE WHEN cp.Category = 'Others' THEN cp.List_Price ELSE 0 END) AS Others_List_Price,
#     -- Total Revenue
#     SUM(CASE WHEN cp.Category = 'Beauty Box' THEN cp.Total_Revenue ELSE 0 END) AS Beauty_Box_Revenue,
#     SUM(CASE WHEN cp.Category = 'Skin' THEN cp.Total_Revenue ELSE 0 END) AS Skin_Revenue,
#     SUM(CASE WHEN cp.Category = 'Body' THEN cp.Total_Revenue ELSE 0 END) AS Body_Revenue,
#     SUM(CASE WHEN cp.Category = 'Cosmetics' THEN cp.Total_Revenue ELSE 0 END) AS Cosmetics_Revenue,
#     SUM(CASE WHEN cp.Category = 'Hair' THEN cp.Total_Revenue ELSE 0 END) AS Hair_Revenue,
#     SUM(CASE WHEN cp.Category = 'Fragrance' THEN cp.Total_Revenue ELSE 0 END) AS Fragrance_Revenue,
#     SUM(CASE WHEN cp.Category = 'Others' THEN cp.Total_Revenue ELSE 0 END) AS Others_Revenue,

#     SUM(cp.Total_Revenue) AS total_revenue,
#     COUNT(DISTINCT cp.Order_Number) AS no_of_orders,
# #     -- Product Revenue - will be usefule for calculating total_discount

#     SUM(CASE WHEN cp.Category = 'Beauty Box' THEN cp.Product_Revenue ELSE 0 END) AS Beauty_Box_product_Revenue,
#     SUM(CASE WHEN cp.Category = 'Skin' THEN cp.Product_Revenue ELSE 0 END) AS Skin_product_Revenue,
#     SUM(CASE WHEN cp.Category = 'Body' THEN cp.Product_Revenue ELSE 0 END) AS Body_product_Revenue,
#     SUM(CASE WHEN cp.Category = 'Cosmetics' THEN cp.Product_Revenue ELSE 0 END) AS Cosmetics_product_Revenue,
#     SUM(CASE WHEN cp.Category = 'Hair' THEN cp.Product_Revenue ELSE 0 END) AS Hair_product_Revenue,
#     SUM(CASE WHEN cp.Category = 'Fragrance' THEN cp.Product_Revenue ELSE 0 END) AS Fragrance_product_Revenue,
#     SUM(CASE WHEN cp.Category = 'Others' THEN cp.Product_Revenue ELSE 0 END) AS Others_product_Revenue,


#     -- Recency
#     DATE_DIFF(CURRENT_DATE, MAX(CASE WHEN cp.Category = 'Beauty Box' AND cp.Units > 0 THEN cp.Order_Date ELSE NULL END), DAY) AS Beauty_Box_Recency_Days,
#     DATE_DIFF(CURRENT_DATE, MAX(CASE WHEN cp.Category = 'Skin' AND cp.Units > 0 THEN cp.Order_Date ELSE NULL END), DAY) AS Skin_Recency_Days,
#     DATE_DIFF(CURRENT_DATE, MAX(CASE WHEN cp.Category = 'Body' AND cp.Units > 0 THEN cp.Order_Date ELSE NULL END), DAY) AS Body_Recency_Days,
#     DATE_DIFF(
#       CURRENT_DATE, 
#       MAX(CASE WHEN cp.Category = 'Cosmetics' AND cp.Units > 0 THEN cp.Order_Date ELSE NULL END),
#       DAY
#     ) AS Cosmetics_Recency_Days,
#     DATE_DIFF(
#       CURRENT_DATE, 
#       MAX(CASE WHEN cp.Category = 'Hair' AND cp.Units > 0 THEN cp.Order_Date ELSE NULL END),
#       DAY
#     ) AS Hair_Recency_Days,
#     DATE_DIFF(
#       CURRENT_DATE, 
#       MAX(CASE WHEN cp.Category = 'Fragrance' AND cp.Units > 0 THEN cp.Order_Date ELSE NULL END),
#       DAY
#     ) AS Fragrance_Recency_Days,
#       DATE_DIFF(
#       CURRENT_DATE, 
#      MAX(CASE WHEN cp.Category = 'Others' AND cp.Units > 0 THEN cp.Order_Date ELSE NULL END),
#       DAY
#    ) AS Others_Recency_Days

# FROM thg-beauty.Beauty_Data.Customer_Product cp
# LEFT JOIN (select customer_key from thg-beauty.Customer_Segmentations.Bulk_Buyer_class where bb_flag = 1) bb
# ON cp.Customer_Key = bb.customer_key
# WHERE cp.Site_Key = {site_key}
# AND cp.order_date >= '{start_date_train}' AND cp.order_date <= '{end_date_train}'
# AND cp.Units > 0 
# AND cp.Order_Status = 'Despatched'
# AND cp.IS_GWP != 1
# AND cp.Is_Full_Size_GWP != 1
# and cp.Prestige_Region = '{prestige_region}'
# and cp.Country_Name = '{country_name}'
# --and cp.Customer_Key = 48444049
# AND bb.customer_key IS NULL
# GROUP BY cp.Customer_Key
# HAVING no_of_orders > 1; """
#     user_level_data_dataset = bq_loader.exec_bq_query(user_level_data)
#     return user_level_data_dataset
#change back to the original directory


#start_date_train,end_date_train,site_key,country_name,prestige_region