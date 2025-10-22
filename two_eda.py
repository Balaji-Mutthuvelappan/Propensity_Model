import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import os

def eda_analysis(user_level_data_dataset, output_path, site_key, bu, country_name, prestige_region, retrain_refresh):
    """Performs Exploratory Data Analysis (EDA) and saves results to an Excel file."""
    
    # Ensure output path is valid
    if not os.path.exists(output_path):
        print(f"Error: Output path {output_path} does not exist.")
        return

    # Selecting only numeric columns that exist in the dataset
    relevant_columns = [
        'Beauty_Box', 'Skin', 'Body', 'Cosmetics', 'Hair', 'Fragrance', 'Others',
        'Beauty_Box_Units', 'Skin_Units', 'Body_Units', 'Cosmetics_Units', 'Hair_Units', 
        'Fragrance_Units', 'Others_Units', 'Beauty_Box_Order_Count', 'Skin_Order_Count', 
        'Body_Order_Count', 'Cosmetics_Order_Count', 'Hair_Order_Count', 'Fragrance_Order_Count', 
        'Others_Order_Count', 'Beauty_Box_RRP', 'Skin_RRP', 'Body_RRP', 'Cosmetics_RRP', 
        'Hair_RRP', 'Fragrance_RRP', 'Others_RRP', 'Beauty_Box_List_Price', 'Skin_List_Price', 
        'Body_List_Price', 'Cosmetics_List_Price', 'Hair_List_Price', 'Fragrance_List_Price', 
        'Others_List_Price', 'Beauty_Box_Revenue', 'Skin_Revenue', 'Body_Revenue', 
        'Cosmetics_Revenue', 'Hair_Revenue', 'Fragrance_Revenue', 'Others_Revenue', 
        'total_revenue', 'no_of_orders', 'Beauty_Box_product_Revenue', 'Skin_product_Revenue', 
        'Body_product_Revenue', 'Cosmetics_product_Revenue', 'Hair_product_Revenue', 
        'Fragrance_product_Revenue', 'Others_product_Revenue', 'Beauty_Box_Recency_Days', 
        'Skin_Recency_Days', 'Body_Recency_Days', 'Cosmetics_Recency_Days', 'Hair_Recency_Days', 
        'Fragrance_Recency_Days', 'Others_Recency_Days'
    ]
    
    # Select only existing columns to avoid errors
    available_columns = list(set(relevant_columns) & set(user_level_data_dataset.columns))
    
    if not available_columns:
        print("Error: No relevant columns found in dataset.")
        return

    # Compute correlation matrix
    correlation_matrix = user_level_data_dataset[available_columns].corr()
    
    # Handle NaN values
    correlation_matrix.fillna(0, inplace=True)
    
    # Display the correlation matrix
    #display(correlation_matrix)

    # Create and display heatmap
    plt.figure(figsize=(14, 10))
    sns.heatmap(correlation_matrix, cmap="coolwarm", fmt=".2f", linewidths=0.5)
    plt.title("Heatmap of Correlation Matrix")
    plt.show()

    # Extract top correlated variables
    top_categories = ['Beauty_Box', 'Skin', 'Body', 'Cosmetics', 'Hair', 'Fragrance', 'Others']
    top_correlations = {}

    for col in top_categories:
        if col in correlation_matrix:
            top_correlations[col] = correlation_matrix[col].sort_values(ascending=False)[1:8]  # Exclude self-correlation

    top_correlations_df = pd.DataFrame(top_correlations)

    # Construct file name safely
    file_name = f"EDA_{site_key}_{bu}_{country_name}_{prestige_region}_{retrain_refresh}.xlsx"
    file_path = os.path.join(output_path, file_name)

    # Save to Excel
    with pd.ExcelWriter(file_path) as writer:
        correlation_matrix.to_excel(writer, sheet_name='Correlation Matrix')
        top_correlations_df.to_excel(writer, sheet_name='Top Correlations')

    #print(f"EDA results saved to {file_path}")
