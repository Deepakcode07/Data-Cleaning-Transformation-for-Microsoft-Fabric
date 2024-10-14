# #This is the code for transforming unstructured data into structured form for processing in fabric.According to fabric rules it can transform data on giving files.

# import pandas as pd
# import re

# # Function to clean column names according to Microsoft Fabric's constraints
# def clean_column_names(df):
#     # Strip spaces at the beginning and end of column names
#     df.columns = df.columns.str.strip()

#     # Replace spaces and non-word characters with underscores
#     df.columns = df.columns.str.replace(r'[^\w]', '_', regex=True)
    
#     # Ensure column names are a maximum of 128 characters
#     df.columns = df.columns.str[:128]

#     return df

# # Function to clean and remove dollar symbols from the data
# def clean_data(df):
#     # Remove rows where all values are null
#     df = df.dropna(how='all')

#     # Remove dollar symbols from string cells (trim the dollar sign without replacing it)
#     df = df.apply(lambda col: col.apply(lambda x: re.sub(r'\$', '', str(x)) if isinstance(x, str) else x))

#     return df

# # Load the Excel file with multiple sheets
# file_path = r'D:\fabricspark_transformation\construction_project_data2.csv'  # Update with your file path

# try:
#     # Load all sheets into a dictionary of DataFrames
#     sheet_dict = pd.read_excel(file_path, sheet_name=None)
#     print("All sheets loaded successfully!")
# except Exception as e:
#     print(f"Error reading the file: {e}")
#     sheet_dict = None  # Ensure sheet_dict is None if reading fails

# # Only proceed if the file was successfully loaded
# if sheet_dict is not None:
#     # Process each sheet and save them as separate CSV files
#     for sheet_name, df in sheet_dict.items():
#         print(f"Processing sheet: {sheet_name}")
        
#         # Clean column names and data for the sheet
#         df = clean_column_names(df)
#         df_cleaned = clean_data(df)

#         # Save the cleaned data to a separate CSV file for each sheet
#         cleaned_file_path = f'cleaned_data_{sheet_name}.csv'
#         df_cleaned.to_csv(cleaned_file_path, index=False)

#         print(f"Cleaned data for sheet '{sheet_name}' has been saved to {cleaned_file_path}")
# else:
#     print("File loading failed, cleaning process aborted.")








import pandas as pd
import re
import os

# Function to clean column names according to Microsoft Fabric's constraints
def clean_column_names(df):
    # Strip spaces at the beginning and end of column names
    df.columns = df.columns.str.strip()

    # Replace spaces and non-word characters with underscores
    df.columns = df.columns.str.replace(r'[^\w]', '_', regex=True)
    
    # Ensure column names are a maximum of 128 characters
    df.columns = df.columns.str[:128]

    return df

# Function to clean and remove dollar symbols from the data
def clean_data(df):
    # Remove rows where all values are null
    df = df.dropna(how='all')

    # Remove dollar symbols from string cells (trim the dollar sign without replacing it)
    df = df.apply(lambda col: col.apply(lambda x: re.sub(r'\$', '', str(x)) if isinstance(x, str) else x))

    return df

# Function to process a DataFrame (clean column names and data, then save to CSV)
def process_and_save(df, output_file):
    df = clean_column_names(df)
    df_cleaned = clean_data(df)
    df_cleaned.to_csv(output_file, index=False)
    print(f"Cleaned data has been saved to {output_file}")

# Specify the file path
file_path = r'D:\fabricspark_transformation\construction_project_data2.csv'  # Update with your file path

# Check the file extension
file_extension = os.path.splitext(file_path)[1].lower()

if file_extension == '.csv':
    # If the file is already in CSV format, load it and clean it directly
    try:
        df = pd.read_csv(file_path)
        print("CSV file loaded successfully!")

        # Generate the cleaned file path
        cleaned_file_path = 'cleaned_data.csv'

        # Process and save the cleaned data
        process_and_save(df, cleaned_file_path)

    except Exception as e:
        print(f"Error reading the CSV file: {e}")

elif file_extension == '.xlsx' or file_extension == '.xls':
    # If the file is in Excel format, handle it with the previous logic
    try:
        # Load all sheets into a dictionary of DataFrames
        sheet_dict = pd.read_excel(file_path, sheet_name=None)
        print("All sheets loaded successfully!")
    except Exception as e:
        print(f"Error reading the Excel file: {e}")
        sheet_dict = None  # Ensure sheet_dict is None if reading fails

    # Only proceed if the file was successfully loaded
    if sheet_dict is not None:
        # Process each sheet and save them as separate CSV files
        for sheet_name, df in sheet_dict.items():
            print(f"Processing sheet: {sheet_name}")
            
            # Generate the cleaned file path for each sheet
            cleaned_file_path = f'cleaned_data_{sheet_name}.csv'

            # Process and save the cleaned data for each sheet
            process_and_save(df, cleaned_file_path)
else:
    print("Unsupported file format. Please provide a CSV or Excel file.")
