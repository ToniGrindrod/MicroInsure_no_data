"""
check that all entries under the "PaymentFrequency" column in the policies table are 1
"""
import sqlite3
import pandas as pd

# Assuming you have a connection object `conn` to your SQLite database
conn = sqlite3.connect('policies.db')  # Replace with your actual database path

# # Step 1: Query to check distinct values in the PaymentFrequency column
# payment_frequency_check = pd.read_sql_query(
#     "SELECT DISTINCT PaymentFrequency FROM Policies",
#     conn
# )

# # Step 2: Check if all PaymentFrequency values are 1
# if all(payment_frequency_check["PaymentFrequency"] == 1):
#     print("All entries under 'PaymentFrequency' are 1.")
# else:
#     print("There are entries under 'PaymentFrequency' that are not 1.")
#     # Optionally, you can display the non-1 entries for further investigation
#     non_one_entries = payment_frequency_check[payment_frequency_check["PaymentFrequency"] != 1]
#     print("Non-1 entries found:", non_one_entries)

# status_name = pd.read_sql_query(
#     "SELECT DISTINCT Status_Name FROM Policies",
#     conn
# )
# print(status_name)


# payment_method = pd.read_sql_query(
#     "SELECT DISTINCT Payment_Method FROM Policies",
#     conn
# )
# print(payment_method)

polic_query="""
        SELECT Payment_Method, InceptionDate, FirstCollectionDate, preferredCollectionDay
        FROM Policies
        WHERE fcertificate= ?
        """
policy = pd.read_sql_query(polic_query, conn, params=['HOLLARDWA2582F'])
print(policy)

#"AG-0000181S"

# policy_query = """
#     SELECT fcertificate, Payment_Method, InceptionDate, FirstCollectionDate, preferredCollectionDay
#     FROM Policies
#     WHERE DATE(FirstCollectionDate) > DATE('now')
#     """
# policy = pd.read_sql_query(policy_query, conn)
# print(policy)

# #check that all cases where FirstCollectionDate is not null, the payment method is not direct debit
# policy = pd.read_sql_query("""
#     SELECT fcertificate, Payment_Method, InceptionDate, FirstCollectionDate, preferredCollectionDay
#     FROM Policies
#     WHERE FirstCollectionDate IS NOT NULL
# """, conn)

# # Convert to datetime
# policy["FirstCollectionDate"] = pd.to_datetime(policy["FirstCollectionDate"], format="%Y/%m/%d")

# # Filter in Python (with parentheses and case-insensitive comparison)
# filtered = policy[
#     (policy["FirstCollectionDate"] > pd.Timestamp.today())
# ]

# #Check whether any of these have actually made a payment yet or if next payment is just due on firstcollectionDate
# fcertWithTrans=[]
# # Iterate through each fcertificate from the filtered DataFrame
# for _, row in filtered.iterrows():
#     fcertificate = row["fcertificate"]
    
#     # Get all transactions for the sub-policy (fcertificate)
#     transactions = pd.read_sql_query(
#     """
#     SELECT Transaction_Date, _Premium_, Transaction_type, Payment_Method, Policy_No 
#     FROM Collections 
#     WHERE Policy_No = ?
#     """, conn, params=[fcertificate]
#     )

#     # Check if any transaction exists for this fcertificate
#     if not transactions.empty:
#         print(f"Payments found for {fcertificate}:")
#         fcertWithTrans.append(fcertificate)
# print(fcertWithTrans)

# # Query for the policy details
# policy_query = """
#     SELECT Payment_Method, InceptionDate, FirstCollectionDate, preferredCollectionDay
#     FROM Policies
#     WHERE fcertificate = ?
# """
# policy = pd.read_sql_query(policy_query, conn, params=['HOLLARDWA2392F'])
# print("Policy Details:")
# print(policy)

# # Now, get all transactions for the policy
# transactions_query = """
#     SELECT Transaction_Date, _Premium_, Transaction_type, Payment_Method, Policy_No 
#     FROM Collections 
#     WHERE Policy_No = ?
# """
# transactions = pd.read_sql_query(transactions_query, conn, params=['HOLLARDWA2392F'])

# # Convert Transaction_Date to datetime
# transactions["Transaction_Date"] = pd.to_datetime(transactions["Transaction_Date"])

# # Print the transactions
# print("\nTransactions for HOLLARDWA2392F:")
# print(transactions)


# # Step 1: Fetch all Direct Debit policies
# query = """
# SELECT fcertificate, InceptionDate, Payment_Method
# FROM Policies
# WHERE Payment_Method = 'Direct Debit'
# """
# direct_debit_policies = pd.read_sql_query(query, conn)

# # Convert InceptionDate to datetime format
# direct_debit_policies['InceptionDate'] = pd.to_datetime(direct_debit_policies['InceptionDate'], errors='coerce')

# # Step 2: Filter policies where InceptionDate is before 2024-01-01
# filtered_policies = direct_debit_policies[direct_debit_policies['InceptionDate'] < pd.to_datetime("2022-01-01")]

# # Print the filtered policies
# print(filtered_policies)
