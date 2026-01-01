import sqlite3
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
import os
import calendar
from dateutil.relativedelta import relativedelta

# Set pandas display options to show all rows and columns
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)

# Connect to the database
conn = sqlite3.connect("policies.db")

#today_date = pd.to_datetime("today").normalize()
today_date = pd.to_datetime("2025-05-30")
#just put this in manually for data
#print("today_date:", today_date)

def add_one_month_strict(date):
    day = date.day
    year = date.year
    month = date.month + 1
    if month > 12:
        month -= 12
        year += 1

    # Check if the next month has that day
    max_day = pd.Timestamp(year=year, month=month, day=1).days_in_month
    if day <= max_day:
        return pd.Timestamp(year=year, month=month, day=day)
    else:
        # Move to the 1st of the month after that
        next_month = month + 1
        next_year = year
        if next_month > 12:
            next_month = 1
            next_year += 1
        return pd.Timestamp(year=next_year, month=next_month, day=1)

def get_payment_status_for_sub_policy(fcertificate, conn):
    """
    Given a sub-policy ID (fcertificate), returns:
    1. A DataFrame of expected payment periods and payment totals.
    2. A DataFrame with summary status: up-to-date status, next payment due date, and next payment already made.
    """
    # 1. Retrieve policy details
    policy_query = """
        SELECT FirstCollectionDate, Premium AS Premium, Payment_Method, PreferredCollectionDay,InceptionDate
        FROM Policies
        WHERE fcertificate = ?
    """
    policy = pd.read_sql_query(policy_query, conn, params=[fcertificate]).iloc[0]
    firstCollection = pd.to_datetime(policy["FirstCollectionDate"])
    inception_date = pd.to_datetime(policy["InceptionDate"])
    premium_expected = pd.to_numeric(policy["Premium"], errors='coerce')
    payment_method = policy["Payment_Method"]
    collection_day = policy["PreferredCollectionDay"]

    # Generate expected periods
    periods = []
    period_start = None  # Initialize to None
    if payment_method.lower() == "payat":
        # For PayAt
        if inception_date < pd.to_datetime("2022-01-01") or inception_date > today_date:
            period_start = firstCollection
        else:
            period_start = inception_date
    if payment_method.lower() == "direct debit":
        # Ensure inception_date is a datetime object
        if isinstance(inception_date, str):
            inception_date = pd.to_datetime(inception_date, errors="coerce")

        if pd.isna(inception_date):
            raise ValueError("Invalid inception_date")

        collection_day_int = int(collection_day)

        # Try to construct a candidate date in the same month
        try:
            candidate_date = inception_date.replace(day=collection_day_int)
        except ValueError:
            # If the collection day doesn't exist in this month, move to the next valid month
            candidate_date = add_one_month_strict(inception_date)

        # If the candidate is still after the inception, use it
        if candidate_date > inception_date:
            period_start = candidate_date
        else:
            # Otherwise, advance one month while preserving the day
            next_candidate = add_one_month_strict(candidate_date)
            period_start = next_candidate

    try:
        collection_day = int(collection_day)
    except (TypeError, ValueError):
        collection_day = 0  # will trigger fallback in the next line

    if not (0 < collection_day < 32):
        collection_day = period_start.day

    if period_start is None or pd.isna(period_start):
        print(f"Error: Could not determine period_start for policy {fcertificate}")
        print(f"  Payment Method: {payment_method}")
        print(f"  FirstCollectionDate: {firstCollection}")
        print(f"  InceptionDate: {inception_date}")
        raise ValueError(f"Could not determine period_start for policy {fcertificate}")


    if period_start>today_date:###consider what happens if period_start=today_date
        #if period_start is on or after today's date, there will only be one period
        firstCollafterToday=True#first collection later than today's date
        period_end = add_one_month_strict(period_start) - pd.Timedelta(days=1)
        periods.append((period_start, period_end))
        #then could check if they've already paid, but not require that they have to
        #in fact, none of them will have paid if their first collection is after today
    
    else:
        firstCollafterToday=False
        if period_start== today_date:
            # If the first collection date is today, we need to include today in the period
            period_end = period_start
            periods.append((period_start, period_end))
        else:
            while period_start < today_date:
                period_end = add_one_month_strict(period_start) - pd.Timedelta(days=1)
                if period_end > today_date:
                    period_end = today_date
                periods.append((period_start, period_end))
                period_start = period_end + pd.Timedelta(days=1)
    
    
    period_df = pd.DataFrame(periods, columns=["Period Start", "Period End"])

    # Get all transactions for the sub-policy
    transactions = pd.read_sql_query(
        """
        SELECT Transaction_Date, Premium, Transaction_type, Payment_Method, Policy_No 
        FROM Collections 
        WHERE Policy_No = ?
        """, conn, params=[fcertificate]
    )
    transactions["Transaction_Date"] = pd.to_datetime(transactions["Transaction_Date"])

    if not firstCollafterToday:
        # Prepare period status DataFrame
        period_status = []
        for index, row in period_df.iterrows():
            start = row["Period Start"]
            end = row["Period End"]
            period_txns = transactions[(transactions["Transaction_Date"] >= start) & (transactions["Transaction_Date"] <= end)]
            #if start_date is today date there might be a key error, if no transaction yet
            #date of next payment will be today's date
            period_txns.loc[:, "Premium"] = pd.to_numeric(period_txns["Premium"], errors='coerce')
            total_paid = period_txns["Premium"].sum()

            period_status.append({
                "Period Start": start,
                "Period End": end,
                "Total Premium Paid": total_paid
            })

        period_status_df = pd.DataFrame(period_status)
        #remove any periods before the first non-zero payment
        # Find the index of the first non-zero payment period
        non_zero_rows = period_status_df[period_status_df["Total Premium Paid"] > 0]
        if not non_zero_rows.empty:
            non_zero_index = non_zero_rows.index.min()
        else:
            non_zero_index = None  # or some fallback value


        # If there are any non-zero payments, truncate the DataFrames
        #if there aren't any non-zero payments, keep all periods
        if pd.notnull(non_zero_index):
            period_status_df = period_status_df.loc[non_zero_index:].reset_index(drop=True)
            period_df = period_df.loc[non_zero_index:].reset_index(drop=True)
    else: #note that no policies with first collection after today will not have any transactions
        #the try clause just includes the case where first collection day is on today's date. there may or may not yet be a transaction
        #so next payment is just due on firstcollectionDate
        ###Also remember to handle the case where transactions is an empty dataframe
        # Prepare period status DataFrame
        
        period_status = []
        for index, row in period_df.iterrows():
            start = row["Period Start"]
            end = row["Period End"]
            total_paid = None#can't expect anything to have been paid if first collection is after today
            

            period_status.append({
                "Period Start": start,
                "Period End": end,
                "Total Premium Paid": total_paid
            })

        period_status_df = pd.DataFrame(period_status)



    # --- Determine current_status ---
    current_status = []

    
    if not firstCollafterToday:
        # Calculate number of full periods (1 full month)
        num_full_periods = 0
        for start, end in zip(period_df["Period Start"], period_df["Period End"]):
            if start + relativedelta(months=1) - pd.Timedelta(days=1) <= end:
                num_full_periods += 1
        
        #print("start:",period_df["Period Start"], "end:", period_df["Period End"])
        total_paid_all_periods = period_status_df["Total Premium Paid"].sum()
        expected_total = num_full_periods * premium_expected
        #print("num_full_periods:", num_full_periods)
        #print("expected:", expected_total)
        #print("total paid:", total_paid_all_periods)
        difference = max(0, expected_total - total_paid_all_periods)
        is_up_to_date = difference == 0

        current_status.append({"Status": "Up to Date", "Value": is_up_to_date})
        current_status.append({"Status": "Amount due", "Value": difference})
    else:
        # If first collection is after today, we don't need to check if up to date
        current_status.append({"Status": "Up to Date", "Value": True})
        current_status.append({"Status": "Amount due", "Value": 0})


    if firstCollafterToday:
        next_payment_date = start
    else:
        # --- Next Payment Date ---
        last_period_start = period_df.iloc[-1]["Period Start"]
        ###check what happens for firsCollAfterToday True
        if payment_method.lower() == "payat":
            next_payment_date = add_one_month_strict(last_period_start)
        elif payment_method.lower() == "direct debit":
            if last_period_start.day == int(collection_day):
                # If last period is on collection day, move to next month
                next_payment_date = add_one_month_strict(last_period_start)
            else:
            #if last period not on collection day, move to next collection day, or to the first of the next month if next collection day doesn't fall within month
                try:
                    next_payment_date = last_period_start.replace(day=int(collection_day))
                except ValueError:
                    # If the collection day doesn't exist in this month, move to the next valid month
                    next_payment_date = add_one_month_strict(last_period_start)
        else:
            next_payment_date = pd.NaT  # Unknown payment method
        
        # Remove the time from the next payment date
        if pd.notna(next_payment_date):
            next_payment_date = next_payment_date.date()

    current_status.append({"Status": "Next Payment Due", "Value": next_payment_date})

    # --- Has the next payment already been made? ---
    last_period_paid = period_status_df.iloc[-1]["Total Premium Paid"]
    if pd.isna(last_period_paid):
        next_collection_paid = False
    else:
        next_collection_paid = last_period_paid >= premium_expected
    current_status.append({"Status": "Next Collection Already Paid", "Value": next_collection_paid})
    current_status.append({"Status": "Payment Method", "Value": payment_method})

    current_status_df = pd.DataFrame(current_status)

    return period_status_df, current_status_df


#what i have defined above is for the active policies, so would only call for active policies
#for cancelled policies, need field for cancellation date and define last period accordingly then just need period_status_df

# # Example usage:
# fcertificate = 'HOLLARDWA0914F'  # Replace with a valid fcertificate value
# payment_status_df, current_status_df = get_payment_status_for_sub_policy(fcertificate, conn)

# # Print the resulting DataFrame
# print(payment_status_df)
# print(current_status_df)

# Assuming get_payment_status_for_sub_policy is defined correctly

def get_payment_status_for_main_policy(mainPolicyNo, conn):
    """
    Given a main policy ID, calls get_payment_status_for_sub_policy for each sub-policy.
    Returns:
        - merged time-series payment DataFrame for all sub-policies
        - current payment status summary across sub-policies
    """
    # 1. Get all sub-policies
    sub_policy_query = """
        SELECT fcertificate
        FROM Policies
        WHERE TransactionNo = ?
    """
    sub_policies_df = pd.read_sql_query(sub_policy_query, conn, params=[mainPolicyNo])
    sub_policies = sub_policies_df["fcertificate"].tolist()

    # Collect time-series payment status and current status for each sub-policy
    time_series_dfs = []
    current_status_dfs = {}

    for sub in sub_policies:
        result = get_payment_status_for_sub_policy(sub, conn)

        if isinstance(result, tuple) and len(result) == 2:
            ts_df, curr_df = result
            ts_df = ts_df.copy()
            ts_df.rename(columns={"Total Premium Paid": f"Paid for {sub}"}, inplace=True)
            time_series_dfs.append(ts_df[["Period Start", "Period End", f"Paid for {sub}"]])
            current_status_dfs[sub] = curr_df.copy()
        else:
            print(f"Warning: {sub} returned a non-tuple result")

    # --- Combine time-series data ---
    if time_series_dfs:
        merged_df = time_series_dfs[0]
        for df in time_series_dfs[1:]:
            merged_df = pd.merge(merged_df, df, on=["Period Start", "Period End"], how="outer")

        # Compute total across sub-policies
        paid_cols = [col for col in merged_df.columns if col.startswith("Paid for")]
        merged_df["Total Paid for All Sub-Policies"] = merged_df[paid_cols].sum(axis=1)
    else:
        print("No valid time-series data to merge.")
        merged_df = pd.DataFrame()

    # --- Combine current status data ---
    if current_status_dfs:
        all_status_rows = []
        index = current_status_dfs[sub_policies[0]]["Status"]  # assume all sub-policies have same "Status" order

        for sub, df in current_status_dfs.items():
            all_status_rows.append(df.set_index("Status")["Value"].rename(f"Value for {sub}"))

        combined_df = pd.concat(all_status_rows, axis=1)

        def combine_values(row):
            label = row.name
            if label in ["Up to Date", "Next Collection Already Paid"]:
                bools = [val is True or str(val).strip().lower() == "true" for val in row]
                return all(bools)
            elif label == "Amount due":
                # Convert to numeric (e.g., in case of strings), ignore non-numeric values
                return pd.to_numeric(row, errors="coerce").sum(min_count=1)
            elif label in ["Next Payment Due", "Payment Method"]:
                return row.iloc[0] if all(val == row.iloc[0] for val in row) else "Mixed"
            else:
                return "Mixed"

        combined_df["Combined Value"] = combined_df.apply(combine_values, axis=1)
        combined_df = combined_df.reset_index()
    else:
        print("No current status data found.")
        combined_df = pd.DataFrame()

    return merged_df, combined_df



# main_policy_status_df, main_current_status_df = get_payment_status_for_main_policy("HOLLARDWA2316", conn)

# print(main_policy_status_df)
# print(main_current_status_df)

#get a unique list of transactionno fromt he policies table, then find which of these have status=active or active_policy.
##then for each of these, call the above function to get the payment status for the main policy. store this in a new sql db
#create two csv files from this new database. one with those policies that have up to date = false and one with those which have next collection already paid = false


###Next to do:
#define another output of the above process as a boolean value whether all expected payments were made for the policy.
#make a function to call the above for each of the sub-policies for the main policy
#Then make another function that searches all the active policies and for the ones that have not been paid in full
#actually what he wants is a csv of all the policies that have not been paid in full with certain columns of data about the policy. then he wants the periods with the premium paid per period, then wants receieved, expected, number periods behind
#before find number of periods behind, need to add received and expected for all sub-policies of the main policy.
#At the end of the day I don't think I'll treat payat and debit differently. but in the csv he wants payment method as one of the data fields.

def check_queries(conn):
        # 1. See all tables in the database
    tables_query = "SELECT name FROM sqlite_master WHERE type='table';"
    tables = pd.read_sql_query(tables_query, conn)
    print("Available tables:")
    print(tables)

    # 2. See the structure of the Policies table
    schema_query = "PRAGMA table_info(Policies);"
    schema = pd.read_sql_query(schema_query, conn)
    print("\nPolicies table schema:")
    print(schema)

    # 3. See all distinct Status_Name values
    status_query = "SELECT DISTINCT Status_Name FROM Policies;"
    statuses = pd.read_sql_query(status_query, conn)
    print("\nDistinct Status_Name values:")
    print(statuses)

    # 4. Count policies by status
    count_query = """
        SELECT Status_Name, COUNT(*) as count 
        FROM Policies 
        GROUP BY Status_Name;
    """
    counts = pd.read_sql_query(count_query, conn)
    print("\nPolicy counts by status:")
    print(counts)

    # 5. See a sample of the Policies table (first 5 rows with all columns)
    sample_query = "SELECT * FROM Policies LIMIT 5;"
    sample = pd.read_sql_query(sample_query, conn)
    print("\nSample rows from Policies:")
    print(sample)
def generate_active_policy_status_reports(conn, output_db_path="policy_status_summary.db"):
    # Step 1: Get all active main policies
    query = """
        SELECT DISTINCT TransactionNo
        FROM Policies
        WHERE Status_Name IN ("Active Policy","Active")
    """
    main_policies_df = pd.read_sql_query(query, conn)
    main_policy_nos = main_policies_df["TransactionNo"].tolist()

    # Create or connect to the output SQLite DB
    if os.path.exists(output_db_path):
        os.remove(output_db_path)
    output_conn = sqlite3.connect(output_db_path)

    # Step 2: Loop through each policy and collect data
    status_dfs = []  # List to store individual policy status DataFrames

    for mainPolicyNo in main_policy_nos:
        time_series_df, status_summary_df = get_payment_status_for_main_policy(mainPolicyNo, conn)
        time_series_df["Main Policy"] = mainPolicyNo
        
        # Get only the Status and Combined Value columns
        policy_status = status_summary_df[["Status", "Combined Value"]].set_index("Status")
        #status_summary_df[["Status", "Combined Value"]] selects only two columns from the DataFrame: "Status" and "Combined Value"
        #The double brackets [[]] are used to select multiple columns
        #set_index("Status") sets the "Status" column as the index (the row labels) of the DataFrame, rather than a regular column

        policy_status.columns = [mainPolicyNo]  # Rename the column to the policy number
        status_dfs.append(policy_status)

    # Combine all policy status DataFrames
    if status_dfs:
        combined_status_summary_df = pd.concat(status_dfs, axis=1)
        #pd.concat() combines multiple DataFrames
        #axis=1 means combine horizontally (side by side) rather than vertically
    else:
        combined_status_summary_df = pd.DataFrame()
        #If there were no policies to process (empty status_dfs list)
        #Creates an empty DataFrame as a fallback

    # Step 1: Find the policy numbers where "Up to Date" is not True
    up_to_date_row = combined_status_summary_df.loc["Up to Date"]
    not_up_to_date_policy_nos = up_to_date_row[up_to_date_row == False].index.tolist()

    # Step 2: Get the amount due for these policies
    amount_due_row = combined_status_summary_df.loc["Amount due"]
    
    results = []

    for transaction_no in not_up_to_date_policy_nos:
        # Get the corresponding row from Policies
        query = f"""
            SELECT CellPhone, Client_Name, TransactionNo, Premium, PreferredCollectionDay, 
                PayAtReference, Payment_Method
            FROM Policies
            WHERE TransactionNo = ?
            LIMIT 1
        """
        policy_info = pd.read_sql_query(query, conn, params=(transaction_no,))

        if not policy_info.empty:
            row = policy_info.iloc[0]
            amount_due = amount_due_row[transaction_no]

            # Safely convert PayAtReference to int, handling NaN and invalid values
            pay_at_ref = row["PayAtReference"]
            if pd.isna(pay_at_ref) or pay_at_ref == "" or pay_at_ref == "None":
                pay_at_ref_int = None
                pay_at_qr = None
            else:
                try:
                    pay_at_ref_int = int(float(pay_at_ref))
                    pay_at_qr = f"https://payat.io/qr/{pay_at_ref_int}"
                except (ValueError, TypeError):
                    pay_at_ref_int = None
                    pay_at_qr = None

            # Build result row
            results.append({
                "CellPhone": "27" +str(row["CellPhone"])[1:],
                "Client_Name": row["Client_Name"],
                "TransactionNo": row["TransactionNo"],
                "PreferredCollectionDay": row["PreferredCollectionDay"],
                "PayAtReference": pay_at_ref_int,
                "PayAtQR": pay_at_qr,
                "Payment_Method": row["Payment_Method"],
                "Amount due": amount_due
            })

    # Create final DataFrame
    final_df = pd.DataFrame(results)
    print(final_df)

    # Get today's date and format it with underscores instead of dashes
    today_str = today_date.isoformat().replace("-", "")

    # Create filename
    filename = f"not_up_to_date_{today_str}.csv"

    # Save to CSV
    final_df.to_csv(filename, index=False)


        #then want another table for those that have next payment already paid = false

#check_queries(conn)
generate_active_policy_status_reports(conn)
#loop through all main policies and check, for each main policthis data frame and check 


# main_policy_status_df, main_current_status_df = get_payment_status_for_main_policy("Misf11052", conn)

# print(main_policy_status_df)
# print(main_current_status_df)

#next I need to create csv file from all the ones that are not up-to-date
