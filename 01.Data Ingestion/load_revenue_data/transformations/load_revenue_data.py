from pyspark import pipelines as dp
from pyspark.sql.functions import col, months_between, add_months, sequence, explode, trunc, lit, expr
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DoubleType,
    BooleanType
    )

#TODO : JSP - This seems bad and I hate this
catalog = 'main'
schema = 'finance_lakehouse'

############################################################
# BRONZE LAYER

@dp.table(
    name=f"{catalog}.{schema}.raw_revenue_transactions",
)
def load_raw_rev_transaction_data():
    rev_trx_schema  = StructType([
        StructField("rev_trx_id", LongType(), nullable=False),
        StructField("contract_id", LongType(), nullable=True),
        StructField("customer_id", LongType(), nullable=False),
        StructField("transaction_date", LongType(), nullable=False),
        StructField("coa_id", LongType(), nullable=False),
        StructField("cost_category", StringType(), nullable=False),
        StructField("amount", DoubleType(), nullable=False)
    ])
    return spark.read\
        .option("recursiveFileLookup","true")\
        .csv(
            path=f'/Volumes/{catalog}/fin/data_gen_outputs/revenue_transactions/',
            header=True,
            schema=rev_trx_schema,
        )

@dp.table(
    name=f"{catalog}.{schema}.raw_revenue_billings",
)
def load_raw_rev_billings_data():
    rev_bill_schema  = StructType([
        StructField("billing_id", LongType(), nullable=False),
        StructField("rev_trx_id", LongType(), nullable=False),
        StructField("invoice_date", LongType(), nullable=False),
        StructField("invoice_number", StringType(), nullable=False),
        StructField("billed_amount", DoubleType(), nullable=False),
        StructField("payment_due_date", LongType(), nullable=False),
        StructField("payment_received_date", LongType(), nullable=False),
        StructField("retention_amount", DoubleType(), nullable=False)
    ])
    return spark.read\
        .option("recursiveFileLookup","true")\
        .csv(
            path=f'/Volumes/{catalog}/fin/data_gen_outputs/revenue_billings/',
            header=True,
            schema=rev_bill_schema,
        )

# ############################################################
# # SILVER LAYER


@dp.table(
    name=f"{catalog}.{schema}.stg_revenue_transactions",
    comment="Staging table joining revenue transactions with billings."
)
def load_stg_revenue_transcations():
    query = spark.sql(f"""        
        SELECT
            trx.rev_trx_id
            ,FROM_UNIXTIME(trx.transaction_date) AS transaction_date
            ,trx.customer_id
            ,party.third_party_id
            ,party.third_party_name AS customer_name
            ,struct(
                trx.coa_id,
                acct.cost_center_id,
                acct.cost_center_name,
                acct.legal_entity_id,
                acct.legal_entity_name
                ) AS coa_meta
            ,trx.cost_category AS rev_category 
            ,trx.amount
            ,trx.contract_id
            ,bill.billing_id
            ,FROM_UNIXTIME(bill.invoice_date) AS invoice_date
            ,bill.invoice_number
            ,bill.billed_amount
            ,FROM_UNIXTIME(bill.payment_received_date) AS payment_received_date
            ,FROM_UNIXTIME(bill.payment_due_date) AS payment_due_date
            ,bill.retention_amount

        FROM {catalog}.{schema}.raw_revenue_transactions AS trx
        LEFT JOIN {catalog}.party.dim_third_party AS party
            ON trx.customer_id = party.customer_id
        LEFT JOIN {catalog}.acct.raw_coa_hierarchy AS acct
            ON trx.coa_id = acct.coa_id
        LEFT JOIN {catalog}.{schema}.raw_revenue_billings AS bill
            ON trx.rev_trx_id = bill.rev_trx_id
        """
        ) 
    
    return query

# --------------------------------------------------------------------------#
# DBX Specific Demo 
@dp.table(
    name=f"{catalog}.{schema}.stg_dbx_consumption_revenue",
    comment="Staging table joining revenue transactions with billings."
)
def load_stg_revenue_transcations():
    query = spark.sql(f"""        
        SELECT
            trx.rev_trx_id
            ,FROM_UNIXTIME(trx.transaction_date) AS transaction_date
            ,trx.customer_id
            ,party.third_party_id
            ,party.third_party_name AS customer_name
            ,CASE 
                WHEN MOD(third_party_id, 100) < 50 THEN 'Azure' -- 0-49: 50% 
                WHEN MOD(third_party_id, 100) < 80 THEN 'AWS'  -- 50-79: 30%
                ELSE 'GCP' -- 80-99: 20%
                END AS platform
            ,trx.amount
            ,trx.amount * 
                CASE 
                    -- Increase
                    WHEN MOD(party.third_party_id, 10) NOT IN (2, 8, 9) AND YEAR(FROM_UNIXTIME(trx.transaction_date)) = 2023 THEN 1.000
                    WHEN MOD(party.third_party_id, 10) NOT IN (2, 8, 9) AND YEAR(FROM_UNIXTIME(trx.transaction_date)) = 2024 THEN 1.15
                    WHEN MOD(party.third_party_id, 10) NOT IN (2, 8, 9) AND YEAR(FROM_UNIXTIME(trx.transaction_date)) = 2025 THEN 1.30
                    -- Decrease
                    WHEN YEAR(FROM_UNIXTIME(trx.transaction_date)) = 2023 THEN 1.000
                    WHEN YEAR(FROM_UNIXTIME(trx.transaction_date)) = 2024 THEN 0.950
                    WHEN YEAR(FROM_UNIXTIME(trx.transaction_date)) = 2025 THEN 0.900
                    -- Flat
                    ELSE 1.000
                    END AS fab_amount
            --,struct(
            --    trx.coa_id,
            --    acct.cost_center_id,
            --    acct.cost_center_name,
            --    acct.legal_entity_id,
            --    acct.legal_entity_name
            --    ) AS coa_meta
            -- ,trx.cost_category AS rev_category 
            -- ,trx.contract_id
            -- ,bill.billing_id
            -- ,FROM_UNIXTIME(bill.invoice_date) AS invoice_date
            -- ,bill.invoice_number
            -- ,bill.billed_amount
            -- ,FROM_UNIXTIME(bill.payment_received_date) AS payment_received_date
            -- ,FROM_UNIXTIME(bill.payment_due_date) AS payment_due_date
            -- ,bill.retention_amount

        FROM {catalog}.{schema}.raw_revenue_transactions AS trx
        LEFT JOIN {catalog}.party.dim_third_party AS party
            ON trx.customer_id = party.customer_id
        LEFT JOIN {catalog}.acct.raw_coa_hierarchy AS acct
            ON trx.coa_id = acct.coa_id
        LEFT JOIN {catalog}.{schema}.raw_revenue_billings AS bill
            ON trx.rev_trx_id = bill.rev_trx_id
        """
        ) 
    
    return query
    
# --------------------------------------------------------------------------#


@dp.table(
    name=f"{catalog}.{schema}.stg_commit_revenue",
    comment="Amortizes total_contract_value by month between contract_start_date and estimated_completion_date, including all contract dimensions."
)
def load_stg_commit_revenue():
    contracts = spark.read.table("dim_all_contracts").filter(col("agreement_type") == "Outbound")
    contracts = contracts.withColumn(
        "start_date", col("contract_start_date").cast("date")
    ).withColumn(
        "end_date", col("estimated_completion_date").cast("date")
    )
    contracts = contracts.withColumn(
        "months",
        (months_between(col("end_date"), col("start_date")) + lit(1)).cast("int")
    )
    contracts = contracts.withColumn(
        "month_seq",
        sequence(
            trunc(col("start_date"), "month"),
            trunc(col("end_date"), "month"),
            expr("interval 1 month")
        )
    )
    contracts = contracts.withColumn("month", explode(col("month_seq")))
    contracts = contracts.withColumn(
        "monthly_amortized_value",
        col("total_contract_value") / col("months")
    )
    return contracts.select(
        "contract_id",
        "contract_number",
        col("month").alias("amortization_month"),
        "monthly_amortized_value",
        "third_party_id",
        "third_party_name",
        "agreement_type",
        "contract_type",
        "contract_description",
        "contract_currency",
        "total_contract_value",
        "contract_start_date",
        "estimated_completion_date",
        "contract_status",
        "legal_entity_id",
        "legal_entity_name",
        "payment_terms",
    )


# ############################################################
# # GOLD LAYER 

@dp.table(
    name=f"{catalog}.{schema}.fact_revenue_recognition",
    comment="Joins monthly stg_commit_revenue with stg_revenue_transactions aggregated by transaction month."
)
def fact_revenue_recognition():
    query = spark.sql(f"""
        SELECT
            COALESCE(rev_trx.period_month, commit.amortization_month) AS period_month,
            COALESCE(rev_trx.third_party_id, commit.third_party_id) AS third_party_id,
            COALESCE(rev_trx.customer_name, commit.third_party_name) AS customer_name,
            COALESCE(rev_trx.legal_entity_id, commit.legal_entity_id) AS legal_entity_id,
            COALESCE(rev_trx.legal_entity_name, commit.legal_entity_name) AS legal_entity_name,
            IF(
                rev_trx.contract_id IS NOT NULL OR period_transaction_count IS NOT NULL
                ,rev_trx.rev_sku_all
                ,commit.monthly_amortized_value 
                ) AS rev_recognized_amount,
            CASE 
                WHEN rev_trx.contract_id IS NOT NULL OR period_transaction_count IS NOT NULL THEN 'consumption_revenue'
                WHEN rev_trx.contract_id IS NULL AND period_transaction_count IS NULL THEN 'commit_revenue'
                END AS rev_recognition_indicator,
            commit.contract_id,
            commit.contract_number,
            commit.monthly_amortized_value AS contract_amortization_value,
            rev_trx.period_transaction_count,
            rev_trx.rev_sku_all,
            rev_trx.rev_sku_labor,
            rev_trx.rev_sku_compute,
            rev_trx.rev_sku_materials,
            rev_trx.rev_sku_subcontractor,
            rev_trx.rev_sku_equipment,
            rev_trx.rev_sku_overhead,
            rev_trx.rev_billed_amount,
            rev_trx.rev_received_amount,
            rev_trx.rev_linear_payments,
            rev_trx.rev_nonlinear_payments

        FROM stg_commit_revenue AS commit
        FULL JOIN (
            SELECT
                contract_id
                ,third_party_id
                ,customer_name
                ,coa_meta.legal_entity_id
                ,coa_meta.legal_entity_name
                ,DATE_TRUNC(transaction_date, 'month') AS period_month
                ,COUNT(rev_trx_id) AS period_transaction_count
                ,SUM(amount) AS rev_sku_all
                ,SUM(amount) FILTER (WHERE rev_category = 'labor') AS rev_sku_labor
                ,SUM(amount) FILTER (WHERE rev_category = 'compute') AS rev_sku_compute
                ,SUM(amount) FILTER (WHERE rev_category = 'materials') AS rev_sku_materials
                ,SUM(amount) FILTER (WHERE rev_category = 'subcontractor') AS rev_sku_subcontractor
                ,SUM(amount) FILTER (WHERE rev_category = 'equipment') AS rev_sku_equipment
                ,SUM(amount) FILTER (WHERE rev_category = 'overhead') AS rev_sku_overhead
                ,SUM(billed_amount) AS rev_billed_amount
                ,SUM(billed_amount) FILTER (WHERE payment_received_date IS NOT NULL) AS rev_received_amount
                ,SUM(amount) FILTER (WHERE DATE_TRUNC(transaction_date, 'month') = DATE_TRUNC(payment_received_date, 'month')) AS rev_linear_payments
                ,SUM(amount) FILTER (WHERE DATE_TRUNC(transaction_date, 'month') < DATE_TRUNC(payment_received_date, 'month')) AS rev_nonlinear_payments
            FROM stg_revenue_transactions
            GROUP BY 1, 2, 3, 4, 5, 6

            ) AS rev_trx
            ON commit.contract_id = rev_trx.contract_id
            AND commit.amortization_month = rev_trx.period_month
            AND commit.third_party_id = rev_trx.third_party_id
            AND commit.legal_entity_id = rev_trx.legal_entity_id 
        """)
    return query





