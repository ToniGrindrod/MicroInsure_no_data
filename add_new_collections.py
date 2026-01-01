"""
This script connects to an SQLite database, loads data from a CSV file into a pandas DataFrame, 
and inserts or updates data in the 'Collections' table of the database based on Policy_No and Transaction_Date.

Steps:
1. Connects to an SQLite database named 'policies.db'.
2. Loads data from a CSV file ('April to 3 May CPS.csv') into a pandas DataFrame.
3. Cleans column headings by removing leading and trailing whitespace.
4. Maps the CSV columns to the required columns for report_active_policies.py.
5. For each row in the new data:
   - If the Policy_No and Transaction_Date combination exists, update the existing record
   - If the combination doesn't exist, insert as a new record
   - Empty Transaction_Date values are preserved as empty
6. Commits the changes to the database and closes the connection.

Dependencies:
- sqlite3 (built into Python)
- pandas (for handling the CSV file)

Notes:
- The script preserves all existing data in the database
- Only records with matching Policy_No and Transaction_Date are updated
- New records are added without affecting existing ones
- Leading and trailing whitespace is removed from column headings
- Only columns required by report_active_policies.py are used
- Empty Transaction_Date values are kept as NULL in the database
"""

import sqlite3
import pandas as pd

# Connect to SQLite database
conn = sqlite3.connect('policies.db')

# Load the new collections data from CSV
print("\nLoading CSV file: 'April to 3 May CPS.csv'")
new_collections_df = pd.read_csv('April to 3 May CPS.csv')

# Print shape of DataFrame to check how many rows/columns were loaded
print(f"CSV loaded: {new_collections_df.shape[0]} rows, {new_collections_df.shape[1]} columns")

# Show first 5 rows of raw data to see what we're working with
print("\nFirst 5 rows of raw data:")
print(new_collections_df.head(5))

# Clean column names by removing leading/trailing whitespace
original_columns = new_collections_df.columns.tolist()
clean_columns = [col.strip() for col in original_columns]

# Print out the column names before and after cleaning for verification
print("\nOriginal column names in new collections csv:")
for col in original_columns:
    print(f"  '{col}'")
    
print("\nCleaned column names:")
for col in clean_columns:
    print(f"  '{col}'")

# Rename DataFrame columns with cleaned names
new_collections_df.columns = clean_columns

# Define the required columns for the Collections table
required_columns = [
    "Transaction_Date",
    "Premium",
    "Transaction_type",
    "Payment_Method",
    "Policy_No"
]

print("\nRequired columns for report_active_policies.py:")
for col in required_columns:
    print(f"  '{col}'")

# Function to map CSV columns to required columns (case-insensitive and ignoring whitespace)
def get_matching_column(df, target_col):
    # First check for exact match after whitespace stripping
    if target_col in df.columns:
        return target_col
    
    # Check for match with underscores replaced by spaces
    space_version = target_col.replace("_", " ")
    if space_version in df.columns:
        return space_version
    
    # Try case-insensitive match
    for col in df.columns:
        if col.lower() == target_col.lower() or col.lower() == space_version.lower():
            return col
    
    # If no match, return None
    return None

# Manual mapping for specific columns we know need special handling
column_mapping = {
    "Transaction_Date": "Transaction Date",
    "Premium": "Premium",
    "Transaction_type": "Transaction Type",
    "Payment_Method": "Payment Method",
    "Policy_No": "Policy No"
}

# Display mapping from CSV columns to required columns
print("\nColumn mapping from CSV to required columns:")
for target_col in required_columns:
    # Try the manual mapping first
    if target_col in column_mapping and column_mapping[target_col] in new_collections_df.columns:
        matching_col = column_mapping[target_col]
        print(f"  '{target_col}' → mapped to → '{matching_col}'")
    else:
        # Fall back to automatic mapping
        matching_col = get_matching_column(new_collections_df, target_col)
        if matching_col:
            print(f"  '{target_col}' → mapped to → '{matching_col}'")
        else:
            print(f"  '{target_col}' → NO MATCH FOUND")

# Specifically check for Policy_No column and its values
policy_col = "Policy No"  # We know this is the correct column name from the mapping
if policy_col in new_collections_df.columns:
    print(f"\nPolicy column found: '{policy_col}'")
    # Check for empty values
    empty_count = new_collections_df[policy_col].isna().sum()
    print(f"  Number of empty values: {empty_count} out of {len(new_collections_df)}")
    # Show some sample values
    print("  Sample values:")
    sample_values = new_collections_df[policy_col].dropna().sample(min(5, len(new_collections_df))).tolist()
    for val in sample_values:
        print(f"    '{val}'")
else:
    print("\nWARNING: 'Policy No' column not found in the CSV!")
    print("Available columns are:", new_collections_df.columns.tolist())

# Create cursor for database operations
cursor = conn.cursor()

# Track stats
rows_processed = 0
rows_skipped = 0
rows_updated = 0
rows_inserted = 0

# Process each row in the new collections data
for idx, row in new_collections_df.iterrows():
    # Map CSV columns to required columns
    mapped_values = {}
    policy_no_value = None
    transaction_date_value = None
    
    for target_col in required_columns:
        # Try to get the corresponding column from our mapping
        if target_col in column_mapping:
            csv_col = column_mapping[target_col]
            if csv_col in new_collections_df.columns:
                if target_col == "Policy_No":
                    policy_no_value = row[csv_col]
                elif target_col == "Transaction_Date":
                    value = row[csv_col]
                    transaction_date_value = value
                mapped_values[target_col] = row[csv_col]
            else:
                mapped_values[target_col] = None
        else:
            # Fall back to automatic mapping
            matching_col = get_matching_column(new_collections_df, target_col)
            if matching_col:
                if target_col == "Policy_No":
                    policy_no_value = row[matching_col]
                elif target_col == "Transaction_Date":
                    value = row[matching_col]
                    transaction_date_value = value
                mapped_values[target_col] = row[matching_col]
            else:
                mapped_values[target_col] = None
    
    # Check transaction type and adjust Premium value
    transaction_type = mapped_values.get("Transaction_type")
    premium_value = mapped_values.get("Premium")
    
    if transaction_type is not None and premium_value is not None and not pd.isna(premium_value):
        # Convert to string, strip whitespace, and convert to lowercase for comparison
        trans_type_clean = str(transaction_type).strip().lower()
        
        # Convert Premium to numeric if it's not already
        if not isinstance(premium_value, (int, float)):
            try:
                premium_value = float(premium_value)
            except (ValueError, TypeError):
                # If conversion fails, keep original value
                pass
        
        if isinstance(premium_value, (int, float)):
            # If transaction type is "accepted", make Premium positive
            if trans_type_clean == "accepted":
                mapped_values["Premium"] = abs(premium_value)
                if premium_value != abs(premium_value):  # Only log if changed
                    print(f"Row {idx+1}: Transaction type 'accepted' - Premium adjusted to positive: {abs(premium_value)}")
            
            # If transaction type is "default", make Premium negative
            elif trans_type_clean == "default":
                mapped_values["Premium"] = -abs(premium_value)
                if premium_value != -abs(premium_value):  # Only log if changed
                    print(f"Row {idx+1}: Transaction type 'default' - Premium adjusted to negative: {-abs(premium_value)}")
    
    # Skip if no Policy_No value (we still need this one)
    if pd.isna(policy_no_value) or str(policy_no_value).strip() == "":
        print(f"Skipping row {idx+1}: Missing Policy_No (value: {policy_no_value})")
        rows_skipped += 1
        continue
    
    rows_processed += 1
    
    # Check if the Policy_No and Transaction_Date combination already exists
    # Use IS NULL for NULL transaction dates
    if pd.isna(transaction_date_value) or str(transaction_date_value).strip() == "":
        cursor.execute("""
            SELECT Policy_No 
            FROM Collections 
            WHERE Policy_No = ? AND Transaction_Date IS NULL
        """, (policy_no_value,))
        transaction_date_for_query = None
    else:
        cursor.execute("""
            SELECT Policy_No 
            FROM Collections 
            WHERE Policy_No = ? AND Transaction_Date = ?
        """, (policy_no_value, transaction_date_value))
        transaction_date_for_query = transaction_date_value
    
    exists = cursor.fetchone() is not None
    
    if exists:
        # First retrieve the existing record to show what's being updated
        if pd.isna(transaction_date_value) or str(transaction_date_value).strip() == "":
            cursor.execute("""
                SELECT * FROM Collections 
                WHERE Policy_No = ? AND Transaction_Date IS NULL
            """, (policy_no_value,))
        else:
            cursor.execute("""
                SELECT * FROM Collections 
                WHERE Policy_No = ? AND Transaction_Date = ?
            """, (policy_no_value, transaction_date_value))
        
        existing_record = cursor.fetchone()
        column_names = [description[0] for description in cursor.description]
        existing_record_dict = dict(zip(column_names, existing_record))
        
        # Update existing record
        set_clauses = []
        values = []
        
        for col in required_columns:
            if col not in ["Policy_No", "Transaction_Date"]:  # Skip these in the SET clause
                set_clauses.append(f"{col} = ?")
                values.append(mapped_values[col])
        
        # Add values for the WHERE clause
        values.append(policy_no_value)
        
        # Handle NULL in the WHERE clause
        if pd.isna(transaction_date_value) or str(transaction_date_value).strip() == "":
            query = f"""
                UPDATE Collections 
                SET {', '.join(set_clauses)} 
                WHERE Policy_No = ? AND Transaction_Date IS NULL
            """
        else:
            query = f"""
                UPDATE Collections 
                SET {', '.join(set_clauses)} 
                WHERE Policy_No = ? AND Transaction_Date = ?
            """
            values.append(transaction_date_value)
        
        cursor.execute(query, values)
        rows_updated += 1
        
        if rows_updated <= 5 or rows_updated % 100 == 0:  # Limit output to first 5 and then every 100
            print(f"\nUpdated record {rows_updated} for Policy_No: {policy_no_value}, Transaction_Date: {transaction_date_value or 'NULL'}")
            print("  Before update:")
            for col, val in existing_record_dict.items():
                print(f"    {col}: {val}")
            print("  After update:")
            for col in required_columns:
                if col in mapped_values:
                    print(f"    {col}: {mapped_values[col]}")
    else:
        # Insert new record
        columns = ', '.join(required_columns)
        placeholders = ', '.join(['?' for _ in required_columns])
        values = [mapped_values[col] for col in required_columns]
        
        query = f"INSERT INTO Collections ({columns}) VALUES ({placeholders})"
        cursor.execute(query, values)
        rows_inserted += 1
        if rows_inserted <= 5 or rows_inserted % 100 == 0:  # Limit output to first 5 and then every 100
            print(f"Inserted new record {rows_inserted} for Policy_No: {policy_no_value}, Transaction_Date: {transaction_date_value or 'NULL'}")

# Commit changes and close connection
conn.commit()
conn.close()

print("\nSummary:")
print(f"  Total rows in CSV: {len(new_collections_df)}")
print(f"  Rows processed: {rows_processed}")
print(f"  Rows skipped: {rows_skipped}")
print(f"  Records updated: {rows_updated}")
print(f"  Records inserted: {rows_inserted}")
print("\nData successfully inserted/updated in the Collections table!")