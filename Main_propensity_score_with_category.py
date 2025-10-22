#!/usr/bin/env python
# coding: utf-8

# In[1]:


#Python pip modules
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from sklearn.preprocessing import MinMaxScaler
from sklearn.preprocessing import LabelEncoder
import matplotlib.pyplot as plt
import ast # For safetly handling the tuple
from datetime import datetime, timedelta
import os
#pd.set_option("logger.info.max_columns", None)
#pd.set_option("logger.info.max_rows", None)


#For ALS model modules
from itertools import product
from implicit.als import AlternatingLeastSquares

from scipy.sparse import csr_matrix
import implicit

from sklearn.manifold import TSNE
import matplotlib.pyplot as plt

#Modules created Locally
from one_pull_transcation import pull_customer_category_data #remember to change the toold box location after testing
from two_eda import eda_analysis
from three_feature_engineering import feature_engineering
from four_model_building import prepare_interaction_matrix
from five_pull_test_data import pull_customer_category_test_data #
from six_hyper_parameter_tuning import hyper_parameter_tuning
from df_to_bq import append_model_parameter # this is the seventh py file
from seven_model_training import final_model_based_hyper_params
from eight_modelrecommendations import model_recommendations
from pull_run_id_and_hyper_params import pull_run_id_hyper_params
from push_data_to_bq import get_top_categories,customer_categories,df_to_bq



import logging


# In[3]:


#User_inputs
input_path = 'C://Users//balaji_muthuvelapp77//Deployment on VM//Propensity_Model//'
output_path = r'C://Users//balaji_muthuvelapp77//Deployment on VM//Propensity_Model//'
output_path_logs = r'C://Users//balaji_muthuvelapp77//Deployment on VM//Propensity_Model//logs'
project_name = 'thg-beauty'
model_parameters_table_name = 'Workspace_Sandbox.mode_parameters_performance_metrics'
model_results_table_name = 'thg-beauty.Workspace_Sandbox.propensity_category' # this will contain the model results for each refresh.
no_of_recommendations_final_model  = 7 # how many recommendations you want it from the final model

#Retrain dates contrains the list of dates the model will be trained, actual date was 01-09 now changed to 12-29 to avoid vm issues
retrain_dates = [f'11-04-{datetime.today().year}','01-03-{datetime.today().year}',f'01-06-{datetime.today().year}',f'01-09-{datetime.today().year}'] #f,
today_date = datetime.now().strftime("%d-%m-%Y")


# In[4]:


if today_date in retrain_dates:
    print(today_date)


# In[5]:


#For removing the existing handlers as it was conflicting with this one
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)


# In[6]:


# Till we complete the test, dsiable the logging

os.makedirs(output_path, exist_ok=True)  # Ensure the directory exists

log_file = os.path.join(output_path_logs, f'propensity_pipeline_final_{today_date}.log')
logging.basicConfig(
    level=logging.INFO,  # Set logging level
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file, mode='w'),  # File logging
        logging.StreamHandler()  # Print logs to console
    ]
)

logger = logging.getLogger(__name__)  # Create a logger


# In[7]:


#This fundcion will help in generating run id
def generate_run_id():
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")  # Current time in YYYYMMDDHHMMSS format
    return f"{timestamp}"


# In[8]:


run_id = generate_run_id()
# logger.info(f"Pipeline Run ID: {run_id}")


# In[9]:


#Defining the training and testing date for retrain part of the code
# Get the current date
current_date = datetime.today()

#current_date = datetime.today()
end_date_train  = current_date - timedelta(days=60)
start_date_train = end_date_train - timedelta(days=365)
start_date_test = end_date_train + timedelta(days=1)
end_date_test = current_date - timedelta(days=1)
#For testing purpose
#end_date_train  = current_date - timedelta(days=5)
#start_date_train = end_date_train - timedelta(days=5)
#start_date_test = end_date_train + timedelta(days=1)
#end_date_test = current_date - timedelta(days=1)



# Print the results
print("Start Date for Training:", start_date_train.strftime('%Y-%m-%d'))
print("End Date for Training:", end_date_train.strftime('%Y-%m-%d'))
print("Start Date for Testing:", start_date_test.strftime('%Y-%m-%d'))
print("End Date for Testing:", end_date_test.strftime('%Y-%m-%d'))


# Format the dates as 'yyyy-mm-dd'
end_date_train_str = end_date_train.strftime('%Y-%m-%d')
start_date_train_str = start_date_train.strftime('%Y-%m-%d')
start_date_test_str = start_date_test.strftime('%Y-%m-%d')
end_date_test_str = end_date_test.strftime('%Y-%m-%d')


## Defining the date for refresh based on the past 365 days
start_date_refresh = current_date - timedelta(days=365)
end_date_refresh = current_date- timedelta(days=1)
#For testing purpose
# start_date_refresh = current_date - timedelta(days=5)
# end_date_refresh = current_date- timedelta(days=1)
print("Start day of refresh:",start_date_refresh.strftime('%Y-%m-%d'))
print("End day of refresh:", end_date_refresh.strftime('%Y-%m-%d'))

start_date_refresh_str = start_date_refresh.strftime('%Y-%m-%d')
end_date_refresh_str = end_date_refresh.strftime('%Y-%m-%d')



# In[10]:


def pipeline(start_date_train_str, end_date_train_str, start_date_test_str, end_date_test_str, start_date_refresh_str, end_date_refresh_str):
    try:
        user_inputs = pd.read_excel(f'{input_path}Input_to_code_file.xlsx')
        input_columns = ['Site_Key', 'Business_Unit', 'Country_Name', 'Prestige_Region']
        user_inputs = user_inputs[input_columns].drop_duplicates()  # Remove duplicate inputs
       # Drop completely empty rows
        user_inputs = user_inputs.dropna(how='all') 

        # Loop through user inputs
        for index, row in user_inputs.iterrows():
            site_key = int(row['Site_Key'])
            bu = row['Business_Unit']
            country_name = row['Country_Name']
            prestige_region = row['Prestige_Region']

            run_id = generate_run_id()
            logger.info(f"Run ID: {run_id}")

            start_date_train = start_date_train_str
            end_date_train = end_date_train_str
            start_date_test = start_date_test_str
            end_data_test = end_date_test_str

            logger.info(f"Training period: {start_date_train} to {end_date_train}")
            logger.info(f"Processing: Site_Key={site_key}, Business_Unit={bu}, Country_Name={country_name}, Prestige_Region={prestige_region}")

            if today_date in retrain_dates:
                user_level_data_dataset = pull_customer_category_data(start_date_train, end_date_train, site_key, country_name, prestige_region)#
                #print(user_level_data_dataset.head(5))# Data added on 07-04-2025
                #user_level_data_dataset.to_csv(f'{output_path}user_level_data_dataset.csv')
                logger.info(f"User dataset shape: {user_level_data_dataset.shape}")
                logger.info("Feature engineering started")
                
                user_level_data_dataset, score_variables = feature_engineering(user_level_data_dataset,site_key)
                logger.info("Feature engineering completed")
                # display(user_level_data_dataset.head(5))
                # display(user_level_data_dataset)
                        
                interaction_matrix, user_mapping, category_mapping, reverse_user_mapping, reverse_category_mapping, customer_key_set = prepare_interaction_matrix(user_level_data_dataset, score_variables)
                logger.info("Model building data preparation completed")

                test_level_data_grouped, customer_key_mapping_values = pull_customer_category_test_data(start_date_train,end_date_train,
                    start_date_test, end_data_test, site_key, country_name, prestige_region, customer_key_set, user_mapping, category_mapping, reverse_user_mapping, reverse_category_mapping
                )
                logger.info("Test data pulled successfully")

                best_params, best_hit_rate = hyper_parameter_tuning(interaction_matrix, customer_key_mapping_values, test_level_data_grouped, reverse_user_mapping, reverse_category_mapping)
                logger.info("Hyperparameter tuning completed")
                logger.info(f"Best Parameters: {best_params}")

                best_params_str = str(best_params)

                input_to_bq_model_parameters_table = append_model_parameter(
                    run_id, best_params_str, best_hit_rate, site_key, bu, country_name, prestige_region, project_name, model_parameters_table_name
                )
                logger.info("Model parameters data ingested to BQ")

            else:
                start_date_train = start_date_refresh_str
                end_date_train = end_date_refresh_str

                user_level_data_dataset = pull_customer_category_data(start_date_train, end_date_train, site_key, country_name, prestige_region)
                logger.info("Feature engineering started")
                
                user_level_data_dataset, score_variables = feature_engineering(user_level_data_dataset,site_key)
                logger.info("Feature engineering completed")

                interaction_matrix, user_mapping, category_mapping, reverse_user_mapping, reverse_category_mapping, customer_key_set = prepare_interaction_matrix(user_level_data_dataset, score_variables)
                logger.info("Model building data preparation completed")

                best_params = pull_run_id_hyper_params(site_key, country_name, prestige_region)
                logger.info(f"Best parameters fetched for Site_Key={site_key}, Country_Name={country_name}, Prestige_Region={prestige_region}")
                logger.info(f"Best Parameters: {best_params}")

                logger.info("Model refresh started")
                first_row_value = best_params.iloc[0]['model_parameters']
                best_params_parsed_tuple = ast.literal_eval(first_row_value)
                
                model, interaction_matrix_csr = final_model_based_hyper_params(best_params_parsed_tuple, interaction_matrix)
                logger.info("Model refresh completed")

                logger.info("Generating model recommendations")
                recommendations_df = model_recommendations(no_of_recommendations_final_model, model, user_mapping, reverse_category_mapping, reverse_user_mapping, interaction_matrix_csr)
                logger.info("Model recommendations completed")
                logger.info(f"Top Recommendations:\n{recommendations_df.head(5)}")

                recommendations_df.reset_index(inplace=True)
                user_level_data_dataset = pd.merge(user_level_data_dataset, recommendations_df, how='left', left_on=['Customer_Key'], right_on=['index'])
                user_level_data_dataset.drop('index', axis=1, inplace=True)
                logger.info("user_level_data_dataset merged")
                #user_level_data_dataset[['categories_purchased', 'categories_not_purchased']] = user_level_data_dataset.apply(customer_categories, axis=1)
                logger.info("user_level_data_dataset categories purchased and categories not purchased has started")
                #user_level_data_dataset.to_csv('user_level_data_dataset_before_Validation.csv')
                user_level_data_dataset[['categories_purchased', 'categories_not_purchased']] = user_level_data_dataset.apply(lambda row: customer_categories(row, site_key), axis=1)

                logger.info(f"Refresh completed for {site_key}, {bu}, {country_name}, {prestige_region}")

                #user_level_data_dataset[['Recommendation_1_existing_categories', 'Recommendation_2_existing_categories']] = user_level_data_dataset.apply(get_top_categories, axis=1)
                user_level_data_dataset[['Recommendation_1_existing_categories', 'Recommendation_2_existing_categories']] = user_level_data_dataset.apply(lambda row: get_top_categories(row, site_key), axis=1)


                user_level_data_dataset['Business_Unit'] = bu
                user_level_data_dataset['Country_Name'] = country_name
                user_level_data_dataset['Prestige_Region'] = prestige_region
                user_level_data_dataset['Site_Key'] = site_key

                # display(user_level_data_dataset.head(5))

                columns_to_clean = ['Recommendation_1', 'Recommendation_2', 'Recommendation_3', 'Recommendation_4', 'Recommendation_5','Recommendation_6','Recommendation_7']
                user_level_data_dataset[columns_to_clean] = user_level_data_dataset[columns_to_clean].applymap(lambda x: x.replace("_score", "") if isinstance(x, str) else x)
                user_level_data_dataset[columns_to_clean] = user_level_data_dataset[columns_to_clean].applymap(lambda x: x.capitalize() if isinstance(x, str) else x)

                if site_key == 166:
                    features_for_model_results_table = [
                        'Business_Unit', 'Country_Name', 'Prestige_Region', 'Site_Key', 'Customer_Key',
                        'Recommendation_1', 'Recommendation_2', 'Recommendation_3', 'Recommendation_4', 'Recommendation_5','Recommendation_6','Recommendation_7',
                        'Score_1', 'Score_2', 'Score_3', 'Score_4', 'Score_5','Score_6','Score_7',
                        'beauty_box_score', 'professional_skincare_score', 'prestige_skincare_score',
                        'cosmetics_score', 'hair_score', 'fragrance_score', 'devices_score',
                        'categories_purchased', 'categories_not_purchased',
                        'Recommendation_1_existing_categories', 'Recommendation_2_existing_categories'
                    ]
                else:
                    features_for_model_results_table = [
                        'Business_Unit', 'Country_Name', 'Prestige_Region', 'Site_Key', 'Customer_Key',
                        'Recommendation_1', 'Recommendation_2', 'Recommendation_3', 'Recommendation_4', 'Recommendation_5',
                        'Score_1', 'Score_2', 'Score_3', 'Score_4', 'Score_5','Score_6','Score_7',
                        'beauty_box_score', 'skin_score', 'body_score',
                        'cosmetics_score', 'hair_score', 'fragrance_score', 'others_score',
                        'categories_purchased', 'categories_not_purchased',
                        'Recommendation_1_existing_categories', 'Recommendation_2_existing_categories'
                    ]

                user_level_data_dataset = user_level_data_dataset[features_for_model_results_table]
                #file_name = f"{bu}_{country_name},{prestige_region},{site_key}_output_check.csv"
                #user_level_data_dataset.to_csv(file_name)
                #user_level_data_dataset.to_csv(f'{output_path}user_level_data_dataset.csv')
                df_to_bq(user_level_data_dataset, site_key, bu, country_name, prestige_region, model_results_table_name)
                logger.info("Data pushed to BQ successfully")

    except Exception as e:
        logger.error(f"An error occurred: {e}")

    finally:
        logging.shutdown()  # Ensure all logs are flushed and saved before exiting


# In[11]:


def main():
    print("Starting the pipeline")
    #pipeline()
    pipeline(start_date_train_str,end_date_train_str,start_date_test_str,end_date_test_str,start_date_refresh_str,end_date_refresh_str)
    print("Closing the pipeline")

if __name__ == "__main__":
    main()
    


# In[ ]:




