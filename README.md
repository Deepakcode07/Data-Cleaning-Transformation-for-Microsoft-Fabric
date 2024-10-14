# Data-Cleaning-Transformation-for-Microsoft-Fabric
This repository contains a Python script for cleaning and transforming data according to Microsoft Fabric's constraints. The script efficiently processes CSV and Excel files, cleaning column names and removing dollar symbols, ensuring the dataset adheres to maximum length constraints and is optimized for further analysis. This project is highly relevant for data engineers, analysts, and developers working with large datasets in data pipelines or cloud platforms like Microsoft Fabric, Azure Synapse Analytics, and more.
Key Features:
Column Name Cleaning: Automatically trims column names, replaces invalid characters with underscores, and ensures names are a maximum of 128 characters in length.
Dollar Symbol Removal: Removes dollar symbols from string values in the dataset without affecting numeric data.
Supports Multiple File Formats: Processes both CSV and Excel files, ensuring compatibility across common formats.
Multi-Sheet Excel Processing: Handles multiple sheets in Excel files, saving cleaned data into individual CSV files.
Optimized Data Cleaning: Removes rows where all values are null, streamlining the dataset for further analysis.
