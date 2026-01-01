"""
This script connects to an SQLite database, creates a table named 'Policies' if it does not exist, 
and inserts data from a CSV file into that table. The table is created with only the specific columns
needed by report_active_policies.py, regardless of what's in the CSV file.

Steps:
1. Loads a CSV file into a pandas DataFrame.
2. Connects to an SQLite database named 'policies.db'. If the database does not exist, it is created.
3. Creates a 'Policies' table in the database with only the needed columns.
4. Maps columns from the CSV to the required columns in the database.
5. Inserts the data from the CSV into the 'Policies' table.
6. Commits the changes to the database and closes the connection.

Dependencies:
- sqlite3 (built into Python)
- pandas (for loading and processing the CSV)

Notes:
- Only specific columns needed by report_active_policies.py are included in the table.
- The 'fcertificate' column is treated as the primary key.
- Leading and trailing whitespace is removed from column headings.
"""

import sqlite3
import pandas as pd
import re

# Load CSV file
csv_file = 'March 2025 Sales File.csv'
df = pd.read_csv(csv_file)

# Clean column names by removing leading/trailing whitespace
original_columns = df.columns.tolist()
clean_columns = [col.strip() for col in original_columns]

# Print out the column names before and after cleaning for verification
print("Original column names:")
for col in original_columns:
    print(f"  '{col}'")
    
print("\nCleaned column names:")
for col in clean_columns:
    print(f"  '{col}'")

# Rename DataFrame columns with cleaned names
df.columns = clean_columns

# Connect to SQLite database
conn = sqlite3.connect("policies.db")
cursor = conn.cursor()

# Define the required columns for the Policies table
required_columns = [
    "fcertificate TEXT PRIMARY KEY",
    "FirstCollectionDate TEXT",
    "Premium REAL",
    "Payment_Method TEXT",
    "PreferredCollectionDay TEXT",
    "InceptionDate TEXT",
    "TransactionNo TEXT",
    "Status_Name TEXT",
    "CellPhone TEXT",
    "Client_Name TEXT", 
    "PayAtReference TEXT"
]

# Drop existing table and create a new one with only the required columns
cursor.execute('DROP TABLE IF EXISTS Policies')
create_table_sql = f"""
CREATE TABLE IF NOT EXISTS Policies (
    {', '.join(required_columns)}
);
"""
cursor.execute(create_table_sql)

print("\nCreated table with these columns:")
for col in required_columns:
    print(f"  {col}")

# Function to map CSV columns to required columns (case-insensitive and ignoring whitespace)
def get_matching_column(df, target_col):
    # Remove the type definition from the target column
    clean_target = target_col.split()[0] if ' ' in target_col else target_col
    
    # First check for exact match after whitespace stripping
    if clean_target in df.columns:
        return clean_target
    
    # Handle underscore vs space (Status_Name vs Status Name)
    space_version = clean_target.replace('_', ' ')
    underscore_version = clean_target.replace(' ', '_')
    
    for col in df.columns:
        if col == space_version or col == underscore_version:
            return col
    
    # Then try case-insensitive match
    for col in df.columns:
        if col.lower() == clean_target.lower():
            return col
        if col.lower() == space_version.lower():
            return col
        if col.lower() == underscore_version.lower():
            return col
    
    # If no match, return None
    return None

# Prepare data for insertion
data_to_insert = []
for _, row in df.iterrows():
    new_row = {}
    for col_def in required_columns:
        col_name = col_def.split()[0]
        matching_col = get_matching_column(df, col_name)
        
        if matching_col:
            new_row[col_name] = row[matching_col]
        else:
            new_row[col_name] = None
            
    data_to_insert.append(new_row)

# Convert to DataFrame for easy insertion
insert_df = pd.DataFrame(data_to_insert)

# Replace spaces with underscores in column names for SQL compatibility
insert_df.columns = [col.replace(" ", "_") for col in insert_df.columns]

# Insert data into the table
insert_df.to_sql('Policies', conn, if_exists='append', index=False)

# Commit and close connection
conn.commit()
conn.close()

print("\nDatabase created and connection closed successfully!")
#to obatin a single view of a customer: core policy number, with sub-policies under. just filter on specific TransactionNo and put fcertificate and all these values into a table 