"""
This script checks if all policies in the Collections table have a status of either 'Active' or 'Cancelled'.
In other words, it checks all the policies for which payment has been made, then checks that they are either active or cancelled (not void)
"""
import sqlite3
import pandas as pd

# Connect to your database
conn = sqlite3.connect("policies.db")


# Step 1: Get all unique fcertificate values from Collections
#DISTINCT removes duplicate values
#AS fcertificate is a column alias. It renames [Policy No] to fcertificate in the result of the query, so you can reference more easily
# Step 2: Join with Policies to get their Status_Name
status_check = pd.read_sql_query(
    """
    SELECT p.fcertificate, p.Status_Name
    FROM Policies p
    INNER JOIN (
        SELECT DISTINCT Policy_No AS fcertificate FROM Collections
    ) c ON p.fcertificate = c.fcertificate
    """,
    conn
)
#INNER JOIN (...) c means we're joining the main table with the results of a subquery, which is given the alias c
#An INNER JOIN behaves like an intersection between two tables — it only keeps the rows that match in both.
#the subquery gives us a list of unique fcertificate values from the Collections table
#ON p.fcertificate = c.fcertificate is the join condition. It tells SQL to match rows from Policies with the subquery result where their fcertificate values match


# Step 3: Check if all Status_Name are either 'Active' or 'Cancelled'
valid_statuses = {"Active", "Cancelled","Active Policy"}
all_valid = status_check["Status_Name"].isin(valid_statuses).all()
#status_check["Status_Name"].isin(valid_statuses) checks, for each row in the Status_Name column of status_check, whether the values is in the valid_statuses set
#returns a boolean series
#.all checks if every value in the boolean series is true.

# Print results
print("All relevant policies have valid statuses (Active/ Active Policy or Cancelled):", all_valid)

# Optional: show the ones that don’t match
if not all_valid:
    print("The following policies have unexpected statuses:")
    print(status_check[~status_check["Status_Name"].isin(valid_statuses)])

# Close connection
conn.close()
