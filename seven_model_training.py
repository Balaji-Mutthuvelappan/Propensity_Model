#!/usr/bin/env python
# coding: utf-8

# In[ ]:

from threadpoolctl import threadpool_limits# added on 02-05-2025
from itertools import product
from implicit.als import AlternatingLeastSquares
from scipy.sparse import coo_matrix, csr_matrix

def final_model_based_hyper_params(best_params,interaction_matrix):
    """
    Trains an ALS model using the best hyperparameters.

    Parameters:
        best_params (tuple): (factors, iterations, regularization) for ALS.
        interaction_matrix (scipy.sparse matrix): User-item interaction matrix.

    Returns:
        AlternatingLeastSquares: Trained ALS model.
    """
    
    # ✅ Limit OpenBLAS threads to 1
    threadpool_limits(1, "blas")#added this one 02-05-2025 due to performance issue
    # Convert to CSR matrix (for ALS model)
    interaction_matrix_csr = interaction_matrix.tocsr()
    
    # Train the ALS model
    model = AlternatingLeastSquares(factors=best_params[0], iterations=best_params[1], regularization=best_params[2])
    model.fit(interaction_matrix_csr)

    return model,interaction_matrix_csr

