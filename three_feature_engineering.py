#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import logging

# Set up logging
logging.basicConfig(level=logging.INFO)

import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt


def feature_engineering(user_level_data_dataset,site_key):

    if site_key != 166:

        #Purchase Frequency
        selected_variables_purchase_frequency = ['Beauty_Box_Order_Count', 'Skin_Order_Count', 'Body_Order_Count',
            'Cosmetics_Order_Count', 'Hair_Order_Count', 'Fragrance_Order_Count',
            'Others_Order_Count']
        
        user_level_data_dataset['Purchase_Frequency_Beauty_Box'] = (user_level_data_dataset['Beauty_Box_Order_Count'] / user_level_data_dataset['no_of_orders']).where(user_level_data_dataset['no_of_orders'] > 0, 0)
        #user_level_data_dataset['Purchase_Frequency_Beauty_Box'].isnull().sum()
        user_level_data_dataset['Purchase_Frequency_Skin'] = (user_level_data_dataset['Skin_Order_Count'] / user_level_data_dataset['no_of_orders']).where(user_level_data_dataset['no_of_orders'] > 0, 0)
        user_level_data_dataset['Purchase_Frequency_Body'] = (user_level_data_dataset['Body_Order_Count'] / user_level_data_dataset['no_of_orders']).where(user_level_data_dataset['no_of_orders'] > 0, 0)
        user_level_data_dataset['Purchase_Frequency_Cosmetics'] = (user_level_data_dataset['Cosmetics_Order_Count'] / user_level_data_dataset['no_of_orders']).where(user_level_data_dataset['no_of_orders'] > 0, 0)
        user_level_data_dataset['Purchase_Frequency_Hair'] = (user_level_data_dataset['Hair_Order_Count'] / user_level_data_dataset['no_of_orders']).where(user_level_data_dataset['no_of_orders'] > 0, 0)
        user_level_data_dataset['Purchase_Frequency_Fragrance'] = (user_level_data_dataset['Fragrance_Order_Count'] / user_level_data_dataset['no_of_orders']).where(user_level_data_dataset['no_of_orders'] > 0, 0)
        user_level_data_dataset['Purchase_Frequency_Others'] = (user_level_data_dataset['Others_Order_Count'] / user_level_data_dataset['no_of_orders']).where(user_level_data_dataset['no_of_orders'] > 0, 0)
        
        
        #Revenue Contribution Monetary
        #Revenue Contribution
        selected_variables_revenue_contribution = ['Beauty_Box_Revenue', 'Skin_Revenue','Body_Revenue', 'Cosmetics_Revenue', 'Hair_Revenue','Fragrance_Revenue', 'Others_Revenue']
        #Calculating the revenue contribution
        for rev in selected_variables_revenue_contribution:
            print(rev)
            user_level_data_dataset[f'{rev}_contribution'] = (user_level_data_dataset[rev]/user_level_data_dataset['total_revenue']).where(user_level_data_dataset['total_revenue'] > 0, 0)
        
        

        #display('recency_calculation started')
        #Recency of purchase- this is to normalize it
        selected_variables_recency = ['Beauty_Box_Recency_Days','Skin_Recency_Days', 'Body_Recency_Days', 'Cosmetics_Recency_Days','Hair_Recency_Days','Fragrance_Recency_Days','Others_Recency_Days']
        #Not all the customers would have bought the product can we keep the recency days
        max_recency_beauty_box = user_level_data_dataset['Beauty_Box_Recency_Days'].max()
        for recency_feature in selected_variables_recency:
            user_level_data_dataset[f'{recency_feature}_normalized'] = (1 / (user_level_data_dataset[recency_feature] + 1)).fillna(0) 
            # Fill NaN AFTER calculation, it has been done puposefully so that when we are multiplying the weights we get the output as zero
            #by doing this lower recency values are given high weightage
        
        #display('recency_calculation completed')
        #Discount
        user_level_data_dataset['Beauty_Box_total_discount'] = ((user_level_data_dataset['Beauty_Box_RRP'] - user_level_data_dataset['Beauty_Box_product_Revenue']) /
            user_level_data_dataset['Beauty_Box_RRP']).where(user_level_data_dataset['Beauty_Box_RRP'] > 0, 0)  # Avoid division by zero
        
        user_level_data_dataset['Skin_total_discount'] = ((user_level_data_dataset['Skin_RRP'] - user_level_data_dataset['Skin_Revenue']) /
            user_level_data_dataset['Skin_RRP']).where(user_level_data_dataset['Skin_RRP'] > 0, 0)  # Avoid division by zero
        
        user_level_data_dataset['Body_discount'] = ((user_level_data_dataset['Body_RRP'] - user_level_data_dataset['Body_Revenue']) /
            user_level_data_dataset['Body_RRP']).where(user_level_data_dataset['Body_RRP'] > 0, 0)  # Avoid division by zero
        
        user_level_data_dataset['Cosmetics_discount'] = ((user_level_data_dataset['Cosmetics_RRP'] - user_level_data_dataset['Cosmetics_Revenue']) /
            user_level_data_dataset['Cosmetics_RRP']).where(user_level_data_dataset['Cosmetics_RRP'] > 0, 0)  # Avoid division by zero
        
        user_level_data_dataset['Hair_discount'] = ((user_level_data_dataset['Hair_RRP'] - user_level_data_dataset['Hair_Revenue']) /
            user_level_data_dataset['Hair_RRP']).where(user_level_data_dataset['Hair_RRP'] > 0, 0)  # Avoid division by zero
        
        user_level_data_dataset['Fragrance_discount'] = ((user_level_data_dataset['Fragrance_RRP'] - user_level_data_dataset['Fragrance_Revenue']) /
            user_level_data_dataset['Fragrance_RRP']).where(user_level_data_dataset['Fragrance_RRP'] > 0, 0)  # Avoid division by zero
        
        user_level_data_dataset['Others_discount'] = ((user_level_data_dataset['Others_RRP'] - user_level_data_dataset['Others_Revenue']) /
            user_level_data_dataset['Others_RRP']).where(user_level_data_dataset['Others_RRP'] > 0, 0)  # Avoid division by zero
        
        #display('scoring_started')
        #Scoring
        #beauty_box_score_variables = ['Purchase_Frequency_Beauty_Box','Beauty_Box_Revenue_contribution','Beauty_Box_Recency_Days_normalized','Beauty_Box_total_discount']
        #user_level_data_dataset['beauty_box_score'] = (user_level_data_dataset[beauty_box_score_variables].fillna(0) * 0.25).sum(axis=1)
        # List of feature variables for each category
        score_variables = {
            'beauty_box_score': [
                'Purchase_Frequency_Beauty_Box', 'Beauty_Box_Revenue_contribution',
                'Beauty_Box_Recency_Days_normalized', 'Beauty_Box_total_discount'
            ],
            'skin_score': [
                'Purchase_Frequency_Skin', 'Skin_Revenue_contribution',
                'Skin_Recency_Days_normalized', 'Skin_total_discount'
            ],
            'body_score': [
                'Purchase_Frequency_Body', 'Body_Revenue_contribution',
                'Body_Recency_Days_normalized', 'Body_discount'
            ],
            'cosmetics_score': [
                'Purchase_Frequency_Cosmetics', 'Cosmetics_Revenue_contribution',
                'Cosmetics_Recency_Days_normalized', 'Cosmetics_discount'
            ],
            'hair_score': [
                'Purchase_Frequency_Hair', 'Hair_Revenue_contribution',
                'Hair_Recency_Days_normalized', 'Hair_discount'
            ],
            'fragrance_score': [
                'Purchase_Frequency_Fragrance', 'Fragrance_Revenue_contribution',
                'Fragrance_Recency_Days_normalized', 'Fragrance_discount'
            ],
            'others_score': [
                'Purchase_Frequency_Others', 'Others_Revenue_contribution',
                'Others_Recency_Days_normalized', 'Others_discount'
            ]
        }
        
        # Loop through each category and calculate the score
        #for category, features in score_variables.items():
        #    user_level_data_dataset[category] = (
        #        user_level_data_dataset[features].fillna(0) * 0.25
        #    ).sum(axis=1)
        
        #display(score_variables)
        
        for category, features in score_variables.items():
            user_level_data_dataset[category] = (
                user_level_data_dataset[features] * 0.25  # No .fillna(0) here
            ).sum(axis=1, skipna=True)  # This will ignore NaNs instead of treating them as 0
    # elif site_key == 166:
    elif site_key == 166:

        # Purchase Frequency
        us_categories = [
            'Haircare', 'Professional_Skincare', 'Prestige_Skincare',
            'Cosmetics', 'Fragrance', 'Devices', 'Beauty_Box'
        ]

        for category in us_categories:
            order_col = f'{category}_Order_Count'
            freq_col = f'Purchase_Frequency_{category}'
            user_level_data_dataset[freq_col] = (user_level_data_dataset[order_col] / user_level_data_dataset['no_of_orders']).where(user_level_data_dataset['no_of_orders'] > 0, 0)

        # Revenue Contribution
        for category in us_categories:
            rev_col = f'{category}_Revenue'
            contribution_col = f'{rev_col}_contribution'
            user_level_data_dataset[contribution_col] = (user_level_data_dataset[rev_col] / user_level_data_dataset['total_revenue']).where(user_level_data_dataset['total_revenue'] > 0, 0)

        # Recency Normalization
        for category in us_categories:
            recency_col = f'{category}_Recency_Days'
            norm_col = f'{recency_col}_normalized'
            user_level_data_dataset[norm_col] = (1 / (user_level_data_dataset[recency_col] + 1)).fillna(0)

        # Discount Calculation
        discount_map = {
            'Haircare': ['Haircare_product_Revenue', 'Haircare_RRP'],
            'Professional_Skincare': ['Professional_Skincare_product_Revenue', 'Professional_Skincare_RRP'],
            'Prestige_Skincare': ['Prestige_Skincare_product_Revenue', 'Prestige_Skincare_RRP'],
            'Cosmetics': ['Cosmetics_product_Revenue', 'Cosmetics_RRP'],
            'Fragrance': ['Fragrance_product_Revenue', 'Fragrance_RRP'],
            'Devices': ['Devices_product_Revenue', 'Devices_RRP'],
            'Beauty_Box': ['Beauty_Box_product_Revenue', 'Beauty_Box_RRP'],
        }

        for category, (rev_col, rrp_col) in discount_map.items():
            discount_col = f"{category.lower()}_discount" if category != 'Beauty_Box' else "Beauty_Box_total_discount"
            user_level_data_dataset[discount_col] = ((user_level_data_dataset[rrp_col] - user_level_data_dataset[rev_col]) / user_level_data_dataset[rrp_col]).where(user_level_data_dataset[rrp_col] > 0, 0)

        # Scoring Variables
        score_variables = {
            'hair_score': [
                'Purchase_Frequency_Haircare', 'Haircare_Revenue_contribution',
                'Haircare_Recency_Days_normalized', 'haircare_discount'
            ],
            'professional_skincare_score': [
                'Purchase_Frequency_Professional_Skincare', 'Professional_Skincare_Revenue_contribution',
                'Professional_Skincare_Recency_Days_normalized', 'professional_skincare_discount'
            ],
            'prestige_skincare_score': [
                'Purchase_Frequency_Prestige_Skincare', 'Prestige_Skincare_Revenue_contribution',
                'Prestige_Skincare_Recency_Days_normalized', 'prestige_skincare_discount'
            ],
            'cosmetics_score': [
                'Purchase_Frequency_Cosmetics', 'Cosmetics_Revenue_contribution',
                'Cosmetics_Recency_Days_normalized', 'cosmetics_discount'
            ],
            'fragrance_score': [
                'Purchase_Frequency_Fragrance', 'Fragrance_Revenue_contribution',
                'Fragrance_Recency_Days_normalized', 'fragrance_discount'
            ],
            'devices_score': [
                'Purchase_Frequency_Devices', 'Devices_Revenue_contribution',
                'Devices_Recency_Days_normalized', 'devices_discount'
            ],
            'beauty_box_score': [
                'Purchase_Frequency_Beauty_Box', 'Beauty_Box_Revenue_contribution',
                'Beauty_Box_Recency_Days_normalized', 'Beauty_Box_total_discount'
            ],
        }

        for category, features in score_variables.items():
            user_level_data_dataset[category] = ((
                user_level_data_dataset[features] * 0.25
            ).sum(axis=1, skipna=True)).abs() # i have added .abs as for some of the customer data is getting into negative values(for handful of customers) due to rrp and list price issue
  

    #user_level_data_dataset.to_csv('user_level_data_dataset_after_rfm_cal.csv')
    return user_level_data_dataset,score_variables



# %%
