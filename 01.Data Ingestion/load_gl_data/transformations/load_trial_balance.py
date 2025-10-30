# Databricks notebook source
# ============================================================================
# TRIAL BALANCE SNAPSHOT GENERATOR - PYSPARK
# ============================================================================
# Generates trial balance as of a specific date with drill-down dimensions
# ============================================================================

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from calendar import monthrange

# COMMAND ----------
# ============================================================================
# ACCOUNT TYPE CLASSIFICATION
# ============================================================================

def classify_account_type(account_number_col):
    """
    Classify account type based on account number
    This determines normal balance (debit or credit)
    """
    return F.when(account_number_col < 2000, "Asset") \
            .when((account_number_col >= 2000) & (account_number_col < 3000), "Liability") \
            .when((account_number_col >= 3000) & (account_number_col < 4000), "Equity") \
            .when((account_number_col >= 4000) & (account_number_col < 5000), "Revenue") \
            .when(account_number_col >= 5000, "Expense") \
            .otherwise("Other")

def get_normal_balance(account_type_col):
    """
    Determine normal balance side for account type
    Assets, Expenses = Debit normal balance
    Liabilities, Equity, Revenue = Credit normal balance
    """
    return F.when(account_type_col.isin(["Asset", "Expense"]), "Debit") \
            .otherwise("Credit")


# COMMAND ----------
# ============================================================================
# GENERATE MONTHLY TRIAL BALANCE 
# ============================================================================


@dp.table(
    name="fact_gl_trial_balances",
    comment="GL entries for revenue and spend transactions",
)
def load_fact_gl_trial_balances():
    """
    Generate trial balance snapshots for all month-ends in a date range
    Returns a single DataFrame with all snapshots
    """

    start_date='2023-10-01'
    end_date='2025-09-30'
    
    start = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')
    
    # Generate list of month-end dates
    month_ends = []
    current = start.replace(day=1)  # Start at beginning of start month
    
    while current <= end:
        # Get last day of current month
        last_day = monthrange(current.year, current.month)[1]
        month_end = date(current.year, current.month, last_day)
        
        # Only include if it's within our range
        if start.date() <= month_end <= end.date():
            month_ends.append(month_end)
        
        # Move to next month
        current = current + relativedelta(months=1)

    
    print(f"Generating Trial Balance for {len(month_ends)} month-ends")
    print(f"Date range: {month_ends[0]} to {month_ends[-1]}")
    print("="*80)
    
    # Read GL entries once
    df_gl = spark.table("fact_gl_entries")
    
    # Add account classification once
    df_classified = df_gl \
        .withColumn("account_type", classify_account_type(F.col("account_number"))) \
        .withColumn("normal_balance", get_normal_balance(F.col("account_type")))
    
    # Generate trial balance for each month-end and collect in a list
    all_snapshots = []
    
    for as_of_date in month_ends:
        print(f"Processing {as_of_date}...")
        
        # Filter entries up to the as_of_date
        df_filtered = df_classified.filter(F.col("gl_date") <= as_of_date)
        
        # Aggregate by account and key dimensions
        df_trial_balance = df_filtered.groupBy(
            "account_number",
            "account_name",
            "account_type",
            "normal_balance",
            "legal_entity_id",
            "legal_entity_name",
            "cost_center_id",
            "cost_center_name",
            "category",
            "third_party_id",
            "third_party_name"
        ).agg(
            F.sum("debit_amount").alias("total_debits"),
            F.sum("credit_amount").alias("total_credits"),
            F.count("*").alias("entry_count"),
            F.min("gl_date").alias("first_entry_date"),
            F.max("gl_date").alias("last_entry_date")
        )
        
        # Calculate net balance
        df_snapshot = df_trial_balance \
            .withColumn("net_balance", F.col("total_debits") - F.col("total_credits")) \
            .withColumn("debit_balance", 
                F.when(F.col("net_balance") > 0, F.col("net_balance")).otherwise(0)) \
            .withColumn("credit_balance", 
                F.when(F.col("net_balance") < 0, F.abs(F.col("net_balance"))).otherwise(0)) \
            .withColumn("as_of_date", F.lit(as_of_date))
        
        all_snapshots.append(df_snapshot)
    
    # Union all snapshots into single DataFrame
    df_final = all_snapshots[0]
    for df in all_snapshots[1:]:
        df_final = df_final.union(df)
    
    # Add metadata and reorder columns
    df_final = df_final \
        .withColumn("snapshot_timestamp", F.current_timestamp()) \
        .select(
            "as_of_date",
            "account_number",
            "account_name",
            "account_type",
            "normal_balance",
            "total_debits",
            "total_credits",
            "net_balance",
            "debit_balance",
            "credit_balance",
            "legal_entity_id",
            "legal_entity_name",
            "cost_center_id",
            "cost_center_name",
            "category",
            "third_party_id",
            "third_party_name",
            "entry_count",
            "first_entry_date",
            "last_entry_date",
            "snapshot_timestamp"
        ) \
        .orderBy("as_of_date", "account_number", "legal_entity_id", "cost_center_id", "third_party_id")
    
    # Display summary
    total_rows = df_final.count()
    print(f"\n✓ Trial Balance generated with {total_rows:,} total rows across {len(month_ends)} months")
    
    # Validation: Check balance for each month
    df_balance_check = df_final.groupBy("as_of_date").agg(
        F.sum("debit_balance").alias("total_debits"),
        F.sum("credit_balance").alias("total_credits")
    ).withColumn("variance", F.col("total_debits") - F.col("total_credits"))
    
    unbalanced = df_balance_check.filter(F.abs(F.col("variance")) > 0.01).count()
    
    if unbalanced == 0:
        print("✓ BALANCED: All months are balanced!")
    else:
        print(f"✗ WARNING: {unbalanced} month(s) are out of balance")
        display(df_balance_check.filter(F.abs(F.col("variance")) > 0.01))
    
    return df_final

