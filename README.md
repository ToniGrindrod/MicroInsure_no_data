# MicroInsure

MicroInsure is a project designed to streamline the process of managing policy data for insurance companies. It uses Python and SQLite to read, merge, and store policy data from CSV files into a local SQLite database. The project automates the process of inserting, updating, and querying insurance policy data.

## Quick Start Guide

This guide walks you through how to upload your latest sales and collections data, update the database, and generate a report of customers who need to be contacted about unpaid policies.

### Step 1: Prepare Your CSV Files

1. Get the latest **Sales CSV file** and **Collections CSV file** from your data source
2. Move both files into the MicroInsure folder (the same folder where you see `add_new_sales.py` and `add_new_collections.py`)

### Step 2: Update the Script File Names

You need to tell the scripts which CSV files to use.

**For Sales Data:**
1. Open the file called `add_new_sales.py` in a text editor (like Notepad or TextEdit)
2. Look for the line near the top that says: `csv_file = "March 2025 Sales File.csv"`
3. Change `"March 2025 Sales File.csv"` to the name of your new sales CSV file
4. Save the file

**For Collections Data:**
1. Open the file called `add_new_collections.py` in a text editor
2. Look for the line near the top that says: `csv_file = "April to 3 May CPS.csv"`
3. Change `"April to 3 May CPS.csv"` to the name of your new collections CSV file
4. Save the file

### Step 3: Run the Scripts to Update Your Database

1. Open **Terminal** (on Mac: Applications > Utilities > Terminal)
2. Navigate to the MicroInsure folder by typing:
   ```
   cd /Users/antoniagrindrod/Documents/MicroInsure/MicroInsure
   ```
3. Run the sales script first:
   ```
   python3 add_new_sales.py
   ```
   Wait for it to finish (you should see messages about data being processed)

4. Then run the collections script:
   ```
   python3 add_new_collections.py
   ```
   Wait for it to finish

### Step 4: Generate the Customer Contact Report

Once both scripts have finished, run the report script to generate a CSV file with all customers who have unpaid policies:

```
python3 report_active_policies.py
```

This will create a new CSV file named something like `not_up_to_date_20250522.csv` (with today's date). This file contains:
- Customer phone numbers
- Customer names
- Transaction numbers
- Payment methods
- Amount due
- PayAt reference numbers (for payment links)

You can now use this CSV file to contact customers about their unpaid policies.

### Starting Fresh (Resetting All Data)

If you ever need to start completely fresh with a new set of policies:

1. Delete these two database files:
   - `policies.db`
   - `policy_status_summary.db`

2. Then run the two table creation scripts to set up fresh empty tables:
   ```
   python3 create_policies_table.py
   python3 create_collections_table.py
   ```

3. Then run the two data scripts as described in Step 3:
   ```
   python3 add_new_sales.py
   python3 add_new_collections.py
   ```

This will create brand new databases with only your latest data.

## Files Overview

- `add_new_sales.py`: Adds new sales/policy data from a CSV file to the database
- `add_new_collections.py`: Adds new collection/payment data from a CSV file to the database
- `report_active_policies.py`: Generates a report of customers with unpaid policies
- `policies.db`: Database file that stores all policy and sales information
- `policy_status_summary.db`: Database file that stores the payment status reports

## License

This project is licensed under the MIT License - see the [LICENSE] file for details.
