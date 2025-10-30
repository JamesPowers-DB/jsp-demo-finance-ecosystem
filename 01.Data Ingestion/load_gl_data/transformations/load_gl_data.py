from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime

# COMMAND ----------
# ============================================================================
# CHART OF ACCOUNTS CONFIGURATION
# ============================================================================

# Optionally save as a reference table
@dp.table(
    name=f"dim_gl_accounts",
    comment="Reference table for the Chart of Accounts",
)
def load_chart_of_accounts():
    # Create Chart of Accounts as a DataFrame for reference
    coa_data = [
        (1000, "Cash", "Asset", "Cash and bank accounts"),
        (1100, "Accounts Receivable", "Asset", "Customer receivables"),
        (2000, "Accounts Payable", "Liability", "Supplier payables"),
        (4000, "Sales Revenue", "Revenue", "Revenue from sales"),
        (5000, "Operating Expenses", "Expense", "General operating expenses"),
        (5100, "Tax Expense", "Expense", "Tax and VAT expenses")
    ]

    coa_schema = ["account_number", "account_name", "account_type", "description"]
    df_coa = spark.createDataFrame(coa_data, coa_schema)

    return df_coa



# COMMAND ----------
# ============================================================================
# REVENUE GL ENTRIES - CUSTOMER INVOICES
# ============================================================================

@dp.table(
    name=f"stg_revenue_invoice_entries",
    comment="Revenue GL entries for customer invoices"
)
def generate_revenue_invoice_entries():
    """Generate GL entries for customer invoices (Invoice Recognition)"""
    
    # Read revenue transactions
    df_rev = spark.table("stg_revenue_transactions")
    
    # Filter for invoiced transactions
    df_invoiced = df_rev.filter(
        (F.col("invoice_date").isNotNull()) & 
        (F.col("billed_amount") > 0)
    )
    
    # Create account mapping for cross join
    accounts = spark.createDataFrame([
        (1100, "Accounts Receivable"),
        (4000, "Sales Revenue")
    ], ["account_number", "account_name"])
    
    # Cross join with accounts to create debit and credit entries
    df_entries = df_invoiced.crossJoin(accounts)
    
    # Add window for unique ID generation
    window_spec = Window.partitionBy("rev_trx_id").orderBy("account_number")
    
    # Create GL entries
    df_gl = df_entries.select(
        F.concat(
            F.lit("REV-INV-"),
            F.col("rev_trx_id").cast("string"),
            F.lit("-"),
            F.row_number().over(window_spec).cast("string")
        ).alias("gl_entry_id"),
        F.col("rev_trx_id").alias("transaction_id"),
        F.lit("REVENUE_INVOICE").alias("transaction_type"),
        F.col("invoice_number").alias("reference_number"),
        F.to_date(F.col("invoice_date")).alias("gl_date"),
        F.col("account_number"),
        F.col("account_name"),
        # Debit amount: AR gets debited
        F.when(F.col("account_number") == 1100, F.col("billed_amount"))
         .otherwise(F.lit(0)).alias("debit_amount"),
        # Credit amount: Revenue gets credited
        F.when(F.col("account_number") == 4000, F.col("billed_amount"))
         .otherwise(F.lit(0)).alias("credit_amount"),
        F.col("customer_id").alias("third_party_id"),
        F.col("customer_name").alias("third_party_name"),
        F.col("contract_id"),
        F.col("coa_meta.legal_entity_id").alias("legal_entity_id"),
        F.col("coa_meta.legal_entity_name").alias("legal_entity_name"),
        F.col("coa_meta.cost_center_id").alias("cost_center_id"),
        F.col("coa_meta.cost_center_name").alias("cost_center_name"),
        F.col("rev_category").alias("category"),
        F.current_timestamp().alias("created_timestamp")
    )
    
    return df_gl




# COMMAND ----------
# ============================================================================
# REVENUE GL ENTRIES - CUSTOMER PAYMENTS
# ============================================================================

@dp.table(
    name=f"stg_revenue_payment_entries",
    comment="Revenue GL entries for customer invoices"
)
def generate_revenue_payment_entries():
    """Generate GL entries for customer payments (Payment Receipt)"""
    
    df_rev = spark.table("stg_revenue_transactions")
    
    # Filter for paid transactions
    df_paid = df_rev.filter(
        (F.col("payment_received_date").isNotNull()) & 
        (F.col("billed_amount") > 0)
    )
    
    # Account mapping
    accounts = spark.createDataFrame([
        (1000, "Cash"),
        (1100, "Accounts Receivable")
    ], ["account_number", "account_name"])
    
    df_entries = df_paid.crossJoin(accounts)
    window_spec = Window.partitionBy("rev_trx_id").orderBy("account_number")
    
    # Calculate payment amount (net of retention)
    df_entries = df_entries.withColumn(
        "payment_amount",
        F.col("billed_amount") - F.coalesce(F.col("retention_amount"), F.lit(0))
    )
    
    df_gl = df_entries.select(
        F.concat(
            F.lit("REV-PMT-"),
            F.col("rev_trx_id").cast("string"),
            F.lit("-"),
            F.row_number().over(window_spec).cast("string")
        ).alias("gl_entry_id"),
        F.col("rev_trx_id").alias("transaction_id"),
        F.lit("REVENUE_PAYMENT").alias("transaction_type"),
        F.col("invoice_number").alias("reference_number"),
        F.to_date(F.col("payment_received_date")).alias("gl_date"),
        F.col("account_number"),
        F.col("account_name"),
        # Debit amount: Cash gets debited
        F.when(F.col("account_number") == 1000, F.col("payment_amount"))
         .otherwise(F.lit(0)).alias("debit_amount"),
        # Credit amount: AR gets credited
        F.when(F.col("account_number") == 1100, F.col("payment_amount"))
         .otherwise(F.lit(0)).alias("credit_amount"),
        F.col("customer_id").alias("third_party_id"),
        F.col("customer_name").alias("third_party_name"),
        F.col("contract_id"),
        F.col("coa_meta.legal_entity_id").alias("legal_entity_id"),
        F.col("coa_meta.legal_entity_name").alias("legal_entity_name"),
        F.col("coa_meta.cost_center_id").alias("cost_center_id"),
        F.col("coa_meta.cost_center_name").alias("cost_center_name"),
        F.col("rev_category").alias("category"),
        F.current_timestamp().alias("created_timestamp")
    )
    
    return df_gl


# COMMAND ----------
# ============================================================================
# SPEND GL ENTRIES - SUPPLIER INVOICES
# ============================================================================

@dp.table(
    name=f"stg_spend_invoice_entries",
    comment="Spend GL entries for supplier invoices",
) 
def generate_spend_invoice_entries():
    """Generate GL entries for supplier invoices (Invoice Recognition)"""
    
    df_spend = spark.table("stg_spend_transactions")
    
    # Filter for invoiced transactions (exclude cancelled invoices)
    df_invoiced = df_spend.filter(
        (F.col("invoice_date").isNotNull()) &
        (F.col("total_invoice_amount") > 0) &
        (F.col("invoice_status") != "cancelled")
    )
    
    # Account mapping - 3 accounts for expense, tax, and AP
    accounts = spark.createDataFrame([
        (5000, "Operating Expenses"),
        (5100, "Tax Expense"),
        (2000, "Accounts Payable")
    ], ["account_number", "account_name"])
    
    df_entries = df_invoiced.crossJoin(accounts)
    window_spec = Window.partitionBy("invoice_id").orderBy("account_number")
    
    df_gl = df_entries.select(
        F.concat(
            F.lit("SPEND-INV-"),
            F.col("invoice_id").cast("string"),
            F.lit("-"),
            F.row_number().over(window_spec).cast("string")
        ).alias("gl_entry_id"),
        F.col("invoice_id").alias("transaction_id"),
        F.lit("SPEND_INVOICE").alias("transaction_type"),
        F.col("invoice_number").alias("reference_number"),
        F.from_unixtime(F.col("invoice_date")).cast("date").alias("gl_date"),
        F.col("account_number"),
        F.col("account_name"),
        # Debit amounts: Expense and Tax get debited
        F.when(
            F.col("account_number") == 5000,
            F.col("invoice_amount") - F.coalesce(F.col("discount_amount"), F.lit(0))
        ).when(
            F.col("account_number") == 5100,
            F.coalesce(F.col("tax_amount"), F.lit(0))
        ).otherwise(F.lit(0)).alias("debit_amount"),
        # Credit amount: AP gets credited with total
        F.when(F.col("account_number") == 2000, F.col("total_invoice_amount"))
         .otherwise(F.lit(0)).alias("credit_amount"),
        F.col("supplier_id").alias("third_party_id"),
        F.col("supplier_name").alias("third_party_name"),
        F.col("contract_id"),
        F.col("coa_meta.legal_entity_id").alias("legal_entity_id"),
        F.col("coa_meta.legal_entity_name").alias("legal_entity_name"),
        F.col("coa_meta.cost_center_id").alias("cost_center_id"),
        F.col("coa_meta.cost_center_name").alias("cost_center_name"),
        F.col("invoice_category").alias("category"),
        F.current_timestamp().alias("created_timestamp")
    )
    
    return df_gl



# COMMAND ----------
# ============================================================================
# SPEND GL ENTRIES - SUPPLIER PAYMENTS
# ============================================================================

@dp.table(
    name=f"stg_spend_payment_entries",
    comment="Spend GL entries for supplier payments",
) 
def generate_spend_payment_entries():
    """Generate GL entries for supplier payments (Payment Made)"""
    
    df_spend = spark.table("stg_spend_transactions")
    
    # Filter for paid transactions
    df_paid = df_spend.filter(
        (F.col("payment_date").isNotNull()) & 
        (F.col("amount_paid") > 0)
    )
    
    # Account mapping
    accounts = spark.createDataFrame([
        (2000, "Accounts Payable"),
        (1000, "Cash")
    ], ["account_number", "account_name"])
    
    df_entries = df_paid.crossJoin(accounts)
    window_spec = Window.partitionBy("invoice_id").orderBy("account_number")
    
    df_gl = df_entries.select(
        F.concat(
            F.lit("SPEND-PMT-"),
            F.col("invoice_id").cast("string"),
            F.lit("-"),
            F.row_number().over(window_spec).cast("string")
        ).alias("gl_entry_id"),
        F.col("invoice_id").alias("transaction_id"),
        F.lit("SPEND_PAYMENT").alias("transaction_type"),
        F.col("payment_reference").alias("reference_number"),
        F.from_unixtime(F.col("payment_date")).cast("date").alias("gl_date"),
        F.col("account_number"),
        F.col("account_name"),
        # Debit amount: AP gets debited
        F.when(F.col("account_number") == 2000, F.col("amount_paid"))
         .otherwise(F.lit(0)).alias("debit_amount"),
        # Credit amount: Cash gets credited
        F.when(F.col("account_number") == 1000, F.col("amount_paid"))
         .otherwise(F.lit(0)).alias("credit_amount"),
        F.col("supplier_id").alias("third_party_id"),
        F.col("supplier_name").alias("third_party_name"),
        F.col("contract_id"),
        F.col("coa_meta.legal_entity_id").alias("legal_entity_id"),
        F.col("coa_meta.legal_entity_name").alias("legal_entity_name"),
        F.col("coa_meta.cost_center_id").alias("cost_center_id"),
        F.col("coa_meta.cost_center_name").alias("cost_center_name"),
        F.col("invoice_category").alias("category"),
        F.current_timestamp().alias("created_timestamp")
    )
    
    return df_gl

# COMMAND ----------
# ============================================================================
# COMBINE ALL GL ENTRIES AND SAVE TO DELTA TABLE
# ============================================================================

@dp.table(
    name="fact_gl_entries",
    comment="GL entries for revenue and spend transactions",
)
def load_fact_gl_entries():
       
    df_rev_invoice = spark.table("stg_revenue_invoice_entries")
    df_rev_payment = spark.table("stg_revenue_payment_entries")
    df_spend_invoice = spark.table("stg_spend_invoice_entries")
    df_spend_payment = spark.table("stg_spend_payment_entries")

    # Union all GL entry DataFrames
    df_gl_all = df_rev_invoice \
        .union(df_rev_payment) \
        .union(df_spend_invoice) \
        .union(df_spend_payment)

    print(f"\n✓ Total GL entries generated: {df_gl_all.count()}")

    return df_gl_all

# Databricks notebook source
# ============================================================================
# TRIAL BALANCE SNAPSHOT GENERATOR - PYSPARK
# ============================================================================
# Generates trial balance as of a specific date with drill-down dimensions
# Fiscal Year: Feb 1 - Jan 31
# ============================================================================

