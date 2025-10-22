#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np



def model_recommendations(no_of_recommendations_final_model,model,user_mapping,reverse_category_mapping,reverse_user_mapping,interaction_matrix_csr):

    # Initialize a dictionary to store recommendations for each user
    recommendations_dict = {}
    
    N = no_of_recommendations_final_model  # Number of recommendations per user
    
    # Iterate through each user
    for user_idx in range(len(user_mapping)):  # Iterate through all users
        # Get the top N recommendations for the current user
        recommended = model.recommend(user_idx, interaction_matrix_csr[user_idx], N=N)
        
        # Extract recommended category indices and scores
        recommended_categories = recommended[0]  # First array: category indices
        recommended_scores = recommended[1]      # Second array: corresponding scores
        
        # Map category indices to their actual category names using reverse mapping
        recommended_category_names = [reverse_category_mapping[item_id] for item_id in recommended_categories]
        
        # Store the recommended categories and their scores in the dictionary
        recommendations_dict[user_idx] = recommended_category_names + list(recommended_scores)
    
    # Create a DataFrame from the recommendations dictionary
    # For each user, the first N columns will be the recommended categories, and the last N columns will be the corresponding scores
    recommendation_columns = [f'Recommendation_{i+1}' for i in range(N)] + [f'Score_{i+1}' for i in range(N)]
    
    # Create the DataFrame
    recommendations_df = pd.DataFrame.from_dict(recommendations_dict, orient='index', columns=recommendation_columns)
    
    # Optional: Map the index back to the Customer_Key
    recommendations_df.index = [reverse_user_mapping[idx] for idx in recommendations_df.index]
    
    # Display the final recommendations DataFrame
    #print(recommendations_df)

    return recommendations_df




# In[ ]:




