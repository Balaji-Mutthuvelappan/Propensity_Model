#!/usr/bin/env python
# coding: utf-8

# In[1]:


from scipy.sparse import coo_matrix
import pandas as pd

def prepare_interaction_matrix(user_level_data_dataset, score_variables):
    """
    Prepares a sparse interaction matrix for customers and product categories.

    Parameters:
        user_level_data_dataset (pd.DataFrame): The user-level dataset containing category scores.
        score_variables (dict): Dictionary containing category score variable names.

    Returns:
        tuple: (interaction_matrix, user_mapping, category_mapping, reverse_user_mapping, reverse_category_mapping, customer_key_set)
    """
    
    # Stack the category scores
    stacked_df = user_level_data_dataset.set_index("Customer_Key")[list(score_variables.keys())].stack().reset_index()
    
    # Rename columns
    stacked_df.columns = ["Customer_Key", "Category", "Score"]

    # Remove rows where Score is 0
    stacked_df = stacked_df[stacked_df["Score"] > 0]

    # Create unique integer IDs for Customers and Categories
    user_mapping = {user: i for i, user in enumerate(stacked_df["Customer_Key"].unique())}
    category_mapping = {category: i for i, category in enumerate(stacked_df["Category"].unique())}

    # Apply mappings
    stacked_df["User_ID"] = stacked_df["Customer_Key"].map(user_mapping)
    stacked_df["Category_ID"] = stacked_df["Category"].map(category_mapping)

    # Reverse mappings for lookup
    reverse_user_mapping = {i: user for user, i in user_mapping.items()}
    reverse_category_mapping = {i: category for category, i in category_mapping.items()}

    # Create sparse interaction matrix
    user_ids = stacked_df["User_ID"].values
    category_ids = stacked_df["Category_ID"].values
    scores = stacked_df["Score"].values
    interaction_matrix = coo_matrix((scores, (user_ids, category_ids)))

    # Create a set of unique customer keys
    customer_key_set = set(stacked_df["Customer_Key"].unique())

    return interaction_matrix, user_mapping, category_mapping, reverse_user_mapping, reverse_category_mapping, customer_key_set


# In[ ]:




