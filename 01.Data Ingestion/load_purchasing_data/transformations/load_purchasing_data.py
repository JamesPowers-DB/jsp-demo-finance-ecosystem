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

catalog = 'fin_demo'
schema = 'spend'

############################################################
# BRONZE LAYER



@dp.table(
    name=f"{catalog}.{schema}.raw_purchase_orders",
    comment="Raw data for the purchase orders table"
)
def load_raw_purchase_order_data():
    purchase_schema = StructType([
        StructField("purchase_order_id", LongType(), nullable=False),
        StructField("contract_id", LongType(), nullable=True),
        StructField("purchase_order_number", StringType(), nullable=False),
        StructField("purchase_order_date", LongType(), nullable=False),
        StructField("purchase_order_status", StringType(), nullable=False),
        StructField("supplier_id", LongType(), nullable=False),
        StructField("purchase_order_currency", StringType(), nullable=False),
        StructField("purchase_order_amount", DoubleType(), nullable=False),
        StructField("total_purchase_order_value", DoubleType(), nullable=False),
        StructField("coa_id", LongType(), nullable=False)
    ])

    return spark.read\
        .option("recursiveFileLookup","true")\
        .csv(
            path=f"/Volumes/{catalog}/fin/data_gen_outputs/purchase_orders/",
            header=True,
            inferSchema=True, 
        )
    

@dp.table(
    name=f"{catalog}.{schema}.raw_spend_invoices",
)
def load_raw_raw_spend_invoice_data():

    invoice_schema = StructType([
        StructField('invoice_id', LongType(), nullable=False), 
        StructField('purchase_order_id', LongType(), nullable=False), 
        StructField('invoice_number',  StringType(), nullable=True),
        StructField('invoice_date', LongType(), nullable=False), 
        StructField('invoice_status',  StringType(), nullable=True),
        StructField('invoice_amount', DoubleType(), nullable=True),
        StructField('tax_amount', DoubleType(), nullable=True),
        StructField('discount_amount', DoubleType(), nullable=True),
        StructField('total_invoice_amount', DoubleType(), nullable=True),
        StructField('amount_paid', DoubleType(), nullable=True),
        StructField('payment_due_date',  LongType(), nullable=False),
        StructField('invoice_category', StringType(), nullable=True),
        StructField('payment_date',  LongType(), nullable=False),
        StructField('payment_method', StringType(), nullable=True),
        StructField('payment_reference', StringType(), nullable=True),
        StructField('receipt_matched', BooleanType(), nullable=True),
        StructField('three_way_match_status', StringType(), nullable=True),
        StructField('goods_received_date',  LongType(), nullable=False),
        StructField('invoice_year',  LongType(), nullable=False),
        StructField('invoice_month',  LongType(), nullable=False),
    ])

    return spark.read\
        .option("recursiveFileLookup", "true")\
        .csv(
            path=f'/Volumes/{catalog}/fin/data_gen_outputs/spend_invoices/',
            header=True,
            schema=invoice_schema,
        )


# ############################################################
# # SILVER LAYER

@dp.table(
    name=f"{catalog}.{schema}.stg_spend_transactions",
    comment="Staging table joining purchase orders and spend invoices"
)
def stg_spend_transactions():
    return spark.sql(f"""
        SELECT
            po.purchase_order_id
            ,po.contract_id
            ,po.purchase_order_number
            ,po.purchase_order_date
            ,po.purchase_order_status
            ,party.third_party_id AS third_party_id
            ,po.supplier_id
            ,party.third_party_name AS supplier_name
            ,po.purchase_order_currency
            ,po.purchase_order_amount
            ,po.total_purchase_order_value
            ,struct(
                po.coa_id,
                acct.cost_center_id,
                acct.cost_center_name,
                acct.legal_entity_id,
                acct.legal_entity_name
                ) AS coa_meta
            ,inv.invoice_id
            ,inv.invoice_number
            ,inv.invoice_date
            ,inv.invoice_status
            ,inv.invoice_amount
            ,inv.tax_amount
            ,inv.discount_amount
            ,inv.total_invoice_amount
            ,inv.amount_paid
            ,inv.payment_due_date
            ,inv.invoice_category
            ,inv.payment_date
            ,inv.payment_method
            ,inv.payment_reference
            ,inv.receipt_matched
            ,inv.three_way_match_status
            ,inv.goods_received_date
            ,inv.invoice_year
            ,inv.invoice_month

        FROM {catalog}.{schema}.raw_purchase_orders po
        LEFT JOIN {catalog}.party.dim_third_party AS party
            ON po.supplier_id = party.supplier_id
        LEFT JOIN {catalog}.acct.raw_coa_hierarchy AS acct
            ON po.coa_id = acct.coa_id
        LEFT JOIN {catalog}.{schema}.raw_spend_invoices AS inv
            ON po.purchase_order_id = inv.purchase_order_id

    """)



@dp.table(
    name=f"{catalog}.{schema}.stg_commit_spend",
    comment="Amortizes total_contract_value by month between contract_start_date and estimated_completion_date, including all contract dimensions."
)
def load_stg_commit_revenue():
    contracts = spark.read.table("fin_demo.legal.dim_all_contracts").filter(col("agreement_type") == "Inbound")
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
    name=f"{catalog}.{schema}.fact_spend_recognition",
    comment="Joins monthly stg_commit_spend with stg_spend_transactions aggregated by transaction month."
)
def fact_revenue_recognition():
    query = spark.sql(f"""
        SELECT
            COALESCE(spend_trx.period_month, commit.amortization_month) AS period_month,
            COALESCE(spend_trx.third_party_id, commit.third_party_id) AS third_party_id,
            COALESCE(spend_trx.supplier_name, commit.third_party_name) AS customer_name,
            COALESCE(spend_trx.legal_entity_id, commit.legal_entity_id) AS legal_entity_id,
            COALESCE(spend_trx.legal_entity_name, commit.legal_entity_name) AS legal_entity_name,
            IF(
                spend_trx.contract_id IS NOT NULL OR period_transaction_count IS NOT NULL
                ,spend_trx.spend_amount
                ,commit.monthly_amortized_value 
                ) AS rev_recognized_amount,
            CASE 
                WHEN spend_trx.contract_id IS NOT NULL OR period_transaction_count IS NOT NULL THEN 'consumption_revenue'
                WHEN spend_trx.contract_id IS NULL AND period_transaction_count IS NULL THEN 'commit_revenue'
                END AS rev_recognition_indicator,
            commit.contract_id,
            commit.contract_number,
            commit.monthly_amortized_value AS contract_amortization_value,
            spend_trx.period_transaction_count,
            spend_trx.spend_amount,
            spend_trx.spend_linear_payments,
            spend_trx.spend_nonlinear_payments
        FROM fin_demo.spend.stg_commit_spend AS commit
        FULL JOIN (
            SELECT
                contract_id
                ,third_party_id
                ,supplier_name
                ,coa_meta.legal_entity_id
                ,coa_meta.legal_entity_name
                ,DATE_TRUNC(purchase_order_date, 'month') AS period_month
                ,COUNT(purchase_order_id) AS period_transaction_count
                ,SUM(purchase_order_amount) AS spend_amount
                ,SUM(purchase_order_amount) FILTER (WHERE DATE_TRUNC(purchase_order_date, 'month') = DATE_TRUNC(payment_date, 'month')) AS spend_linear_payments
                ,SUM(purchase_order_amount) FILTER (WHERE DATE_TRUNC(purchase_order_date, 'month') < DATE_TRUNC(payment_date, 'month')) AS spend_nonlinear_payments
            FROM fin_demo.spend.stg_spend_transactions
            GROUP BY 1, 2, 3, 4, 5, 6

            ) AS spend_trx
            ON commit.contract_id = spend_trx.contract_id
            AND commit.amortization_month = spend_trx.period_month
            AND commit.third_party_id = spend_trx.third_party_id
            AND commit.legal_entity_id = spend_trx.legal_entity_id 
        """)
    return query

