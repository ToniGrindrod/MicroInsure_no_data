#!/usr/bin/env python3
"""
Compare CSV Headers

This script compares the column headings between two CSV files and reports differences.
It identifies headers that are present in one file but missing in the other.
Column headings have leading and trailing whitespace removed before comparison.

Simply edit the file1 and file2 variables below to specify which files to compare,
then run the script directly in your editor.
"""

import pandas as pd
import os


def compare_csv_headers(file1, file2):
    """
    Compare column headers between two CSV files and report differences.
    Leading and trailing whitespace is stripped from headers before comparison.
    
    Args:
        file1 (str): Path to first CSV file
        file2 (str): Path to second CSV file
        
    Returns:
        bool: True if headers are identical, False otherwise
    """
    # Validate file existence
    if not os.path.exists(file1):
        print(f"Error: File '{file1}' does not exist.")
        return False
    
    if not os.path.exists(file2):
        print(f"Error: File '{file2}' does not exist.")
        return False
    
    # Read only the headers (first row) of each CSV file
    try:
        df1 = pd.read_csv(file1, nrows=0)
        df2 = pd.read_csv(file2, nrows=0)
    except Exception as e:
        print(f"Error reading CSV files: {e}")
        return False
    
    # Strip whitespace from column names and convert to sets for comparison
    columns1 = {col.strip() for col in df1.columns}
    columns2 = {col.strip() for col in df2.columns}
    
    # Print original and cleaned column names for verification
    print("\nOriginal column names in file 1:")
    for col in df1.columns:
        print(f"  '{col}'")
    print("\nCleaned column names in file 1:")
    for col in sorted([col.strip() for col in df1.columns]):
        print(f"  '{col}'")
        
    print("\nOriginal column names in file 2:")
    for col in df2.columns:
        print(f"  '{col}'")
    print("\nCleaned column names in file 2:")
    for col in sorted([col.strip() for col in df2.columns]):
        print(f"  '{col}'")
    
    # Check if they're identical
    if columns1 == columns2:
        print(f"\n✅ The headers in '{file1}' and '{file2}' are identical when ignoring whitespace.")
        return True
    
    # Find differences in both directions
    only_in_file1 = columns1 - columns2
    only_in_file2 = columns2 - columns1
    
    # Report differences
    print(f"\n❌ The headers in '{file1}' and '{file2}' are different, even after ignoring whitespace.")
    
    if only_in_file1:
        print(f"\nHeaders in '{file1}' but not in '{file2}':")
        for col in sorted(only_in_file1):
            print(f"  - '{col}'")
    
    if only_in_file2:
        print(f"\nHeaders in '{file2}' but not in '{file1}':")
        for col in sorted(only_in_file2):
            print(f"  - '{col}'")
    
    return False


if __name__ == "__main__":
    # SPECIFY YOUR FILES HERE
    file1 = "March 2025 Sales File.csv"  # Edit this line to point to your first file
    file2 = "Sales File 5th May.csv"  # Edit this line to point to your second file
    
    # Run the comparison
    compare_csv_headers(file1, file2)
    print("\nDone!")