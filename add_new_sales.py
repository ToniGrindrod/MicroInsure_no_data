"""
This script connects to an SQLite database, loads data from a CSV file into a pandas DataFrame, 
and inserts or updates data in the 'Policies' table of the database based on the primary key 'fcertificate'.

Steps:
1. Connects to an SQLite database named 'policies.db'.
2. Loads data from a CSV file ('new_sales.csv') into a pandas DataFrame.
3. Cleans column headings by removing leading and trailing whitespace.
4. Maps the CSV columns to the required columns for report_active_policies.py.
5. For each row in the new data:
   - If the fcertificate exists, update the existing record
   - If the fcertificate doesn't exist, insert as a new record
6. Commits the changes to the database and closes the connection.

Dependencies:
- sqlite3 (built into Python)
- pandas (for handling the CSV file)

Notes:
- The script preserves all existing data in the database
- Only records with matching fcertificates are updated
- New records are added without affecting existing ones
- Leading and trailing whitespace is removed from column headings
- Only columns required by report_active_policies.py are used
"""

import sqlite3
import pandas as pd

# Connect to SQLite database
conn = sqlite3.connect('policies.db')

# Load the new sales data from CSV
new_sales_df = pd.read_csv('Sales File 5th May.csv')

# Clean column names by removing leading/trailing whitespace
original_columns = new_sales_df.columns.tolist()
clean_columns = [col.strip() for col in original_columns]

# Print out the column names before and after cleaning for verification
print("Original column names in new_sales.csv:")
for col in original_columns:
    print(f"  '{col}'")
    
print("\nCleaned column names:")
for col in clean_columns:
    print(f"  '{col}'")

# Rename DataFrame columns with cleaned names
new_sales_df.columns = clean_columns

# Define the required columns for the Policies table
required_columns = [
    "fcertificate",
    "FirstCollectionDate",
    "Premium",
    "Payment_Method",
    "PreferredCollectionDay",
    "InceptionDate",
    "TransactionNo",
    "Status_Name",
    "CellPhone",
    "Client_Name", 
    "PayAtReference"
]

print("\nRequired columns for report_active_policies.py:")
for col in required_columns:
    print(f"  '{col}'")

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

# Create cursor for database operations
cursor = conn.cursor()

# Process each row in the new sales data
for _, row in new_sales_df.iterrows():
    # Map CSV columns to required columns
    mapped_values = {}
    fcertificate_value = None
    
    for target_col in required_columns:
        matching_col = get_matching_column(new_sales_df, target_col)
        
        if matching_col:
            if target_col == "fcertificate":
                fcertificate_value = row[matching_col]
            mapped_values[target_col] = row[matching_col]
        else:
            mapped_values[target_col] = None
    
    # Skip if no fcertificate value
    if not fcertificate_value:
        print(f"Skipping row: No fcertificate value found")
        continue
    
    # Check if the fcertificate already exists
    cursor.execute("SELECT fcertificate FROM Policies WHERE fcertificate = ?", (fcertificate_value,))
    exists = cursor.fetchone() is not None
    
    if exists:
        # Update existing record
        set_clauses = []
        values = []
        
        for col in required_columns:
            if col != "fcertificate":  # Skip fcertificate in the SET clause
                set_clauses.append(f"{col} = ?")
                values.append(mapped_values[col])
        
        # Add fcertificate value at the end for the WHERE clause
        values.append(fcertificate_value)
        
        query = f"UPDATE Policies SET {', '.join(set_clauses)} WHERE fcertificate = ?"
        cursor.execute(query, values)
        print(f"Updated record for fcertificate: {fcertificate_value}")
    else:
        # Insert new record
        columns = ', '.join(required_columns)
        placeholders = ', '.join(['?' for _ in required_columns])
        values = [mapped_values[col] for col in required_columns]
        
        query = f"INSERT INTO Policies ({columns}) VALUES ({placeholders})"
        cursor.execute(query, values)
        print(f"Inserted new record for fcertificate: {fcertificate_value}")

# Commit changes and close connection
conn.commit()
conn.close()

print("\nData successfully inserted/updated in the Policies table!")