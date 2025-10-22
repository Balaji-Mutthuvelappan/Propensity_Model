#!/usr/bin/env python
# coding: utf-8

# In[1]:


from threadpoolctl import threadpool_limits
threadpool_limits(1, "blas")



from itertools import product
from implicit.als import AlternatingLeastSquares

from scipy.sparse import csr_matrix
import implicit

from sklearn.manifold import TSNE
import matplotlib.pyplot as plt

import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from sklearn.preprocessing import MinMaxScaler
from sklearn.preprocessing import LabelEncoder
import matplotlib.pyplot as plt






def hyper_parameter_tuning(interaction_matrix,customer_key_mapping_values,test_level_data_grouped,reverse_user_mapping,reverse_category_mapping):

    N = 1 # No of recommendations

    # Convert to CSR matrix (for ALS model)
    interaction_matrix_csr = interaction_matrix.tocsr()
    
    # Define hyperparameter grid final grid used
    factors_list = [25,50,75,90,100,120,150]  # Number of latent factors
    iterations_list = [10,20,40,50]  # Number of iterations
    regularization_list = [0.001, 0.01, 0.1, 1,2,5,10]  # Regularization parameter
    

    
    #factors_list = [120]
    #iterations_list = [30]
    #regularization_list = [2]
    
    
    # Placeholder for best model and best hit rate
    best_model = None
    best_hit_rate = -np.inf
    best_params = None
    
    # Grid search over hyperparameters
    for factors, iterations, regularization in product(factors_list, iterations_list, regularization_list):
        #print(f"\nTraining Model - Factors: {factors}, Iterations: {iterations}, Regularization: {regularization}")
    
        # Train the ALS model
        model = AlternatingLeastSquares(factors=factors, iterations=iterations, regularization=regularization)
        model.fit(interaction_matrix_csr)
    
        # Initialize recommendation storage and evaluation counters
        test_recommendations_dict = {}
        total_users = 0
        hits = 0
    
        # Generate recommendations for each user in test set
        for test_user in customer_key_mapping_values:
            if test_user not in reverse_user_mapping:  # Ensure user exists in the mapping
                continue
    
            recommended = model.recommend(test_user, interaction_matrix_csr[test_user], N=N)
    
            # Extract recommended category indices and scores
            recommended_categories = recommended[0]  # Category indices
            recommended_scores = recommended[1]  # Scores
    
            # Map category indices to actual category names
            recommended_category_names = [reverse_category_mapping[item_id] for item_id in recommended_categories]
    
            # Store recommendations
            test_recommendations_dict[test_user] = recommended_category_names
    
        # Evaluate hit rate
        for test_user in test_level_data_grouped["reverse_user_mapping_customer_key"]:
            if test_user not in test_recommendations_dict:
                continue  # Skip users without recommendations
    
            total_users += 1
    
            # Get actual categories for the user
            actual_categories = test_level_data_grouped.loc[
                test_level_data_grouped["reverse_user_mapping_customer_key"] == test_user, "Category"
            ].values
    
            if len(actual_categories) == 0:
                continue  # Skip users with no actual categories
    
            actual_categories = set(actual_categories[0])  # Convert first row to a set
            recommended_categories = set(test_recommendations_dict.get(test_user, []))  # Convert recommendations to set
    
            # Check if there is a match
            #if actual_categories & recommended_categories:  # Set intersection
                #hits += 1
    
            if any(category in actual_categories for category in recommended_categories):
                hits += 1
    
    
        # Compute hit rate
        hit_rate = hits / total_users if total_users > 0 else 0
    
        #print(f"Total Users Evaluated: {total_users}")
        #print(f"Hits: {hits}")
        #print(f"Hit Rate: {hit_rate:.2f}")
    
        # Check if this is the best model
        if hit_rate > best_hit_rate:
            best_hit_rate = hit_rate
            best_model = model
            best_params = (factors, iterations, regularization)
    
    # Print best hyperparameters
    # print("\nBest Model Found:")
    # print(f"Factors: {best_params[0]}, Iterations: {best_params[1]}, Regularization: {best_params[2]}")
    # print(f"Best Hit Rate: {best_hit_rate:.2f}")
    #logger.info("\nBest Model Found:")
    #logger.info(f"Factors: {best_params[0]}, Iterations: {best_params[1]}, Regularization: {best_params[2]}")
    #logger.info(f"Best Hit Rate: {best_hit_rate:.2f}")


    return best_params,best_hit_rate


# In[ ]:




