# import sqlite3
# import pandas as pd
# from datetime import datetime
# from dateutil.relativedelta import relativedelta

# # Connect to the database
# conn = sqlite3.connect("policies.db")

# fcertificate="Misf10603F"
# # 1. Retrieve policy details
# policy_query = """
#         SELECT Payment_Method, InceptionDate, ExpiryDate, PaymentFrequency,FirstCollectionDate,PreferredCollectionDay,NextDebitDate
#         FROM Policies
#         WHERE fcertificate = ?
#     """
# policy = pd.read_sql_query(policy_query, conn, params=[fcertificate]).iloc[0]
# print(policy)

import sqlite3
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta

# Connect to the database
conn = sqlite3.connect("policies.db")

# cursor = conn.cursor()

# # Get column names for the Policies table
# cursor.execute("PRAGMA table_info(Policies);")
# columns = cursor.fetchall()

# # Extract and print column names
# column_names = [col[1] for col in columns]
# print("Policies table columns:", column_names)

# # Close the connection
# conn.close()

Payment_Method="PayAt"
# 1. Retrieve policy details
policy_query = """
        SELECT PreferredCollectionDay, fcertificate
        FROM Policies
        WHERE Payment_Method = ? AND PreferredCollectionDay != 0
    """
policy = pd.read_sql_query(policy_query, conn, params=[Payment_Method])

polic_query="""
        SELECT fcertificate, InceptionDate,fCancelDate,CancelReason
        FROM Policies
        WHERE Status_Name = ?
        """
policy = pd.read_sql_query(polic_query, conn, params=["Cancelled"])
# Set display options to show all rows and columns
# pd.set_option('display.max_rows', None)       # Show all rows
# pd.set_option('display.max_columns', None)    # Show all columns
# pd.set_option('display.width', None)          # Don't wrap lines
# pd.set_option('display.max_colwidth', None)   # Show full contents of each cell

print(policy)

# import pandas as pd

# # Inception and Expiry dates
# inception = pd.Timestamp("2024-06-09")
# expiry = pd.Timestamp("2025-06-09")  # Example expiry date

# # Frequency for subsequent periods (start of each month)
# freq_code = 'M'  # 'M

# # Adjusting the start to include the inception date itself
# periods = pd.date_range(start=inception, end=expiry, freq=freq_code)

# # Print the periods
# print(periods)
