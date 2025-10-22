
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DecimalType, IntegerType, DateType
from datetime import datetime

catalog = 'fin_demo'
schema = 'plan'


############################################################
# -- BRONZE LAYER -- #

############################################################
# -- BRONZE LAYER -- #

############################################################
# -- GOLD LAYER -- #

@dp.table(
    name=f"{catalog}.{schema}.dim_fpa_scenarios",
    comment="Scenario dimension table with quarters from Jan 2023 through Oct 2025",
)
def create_fpa_scenarios():
    """
    Create scenario dimension table for quarters starting in Jan 2023 through Oct 2025.
    Scenario is based on cost center and legal entity.
    Source: raw_spend_transactions table (schema from spend_transactions.csv)
    """
    # Get distinct cost centers and legal entities from the raw spend data
    raw_df = spark.table("fin_demo.spend.stg_spend_transactions")

    # Parse the coa_meta JSON field
    parsed_df = raw_df.select(
        F.col("coa_meta.cost_center_id").alias("cost_center_id"),
        F.col("coa_meta.cost_center_name").alias("cost_center_name"),
        F.col("coa_meta.legal_entity_id").alias("legal_entity_id"),
        F.col("coa_meta.legal_entity_name").alias("legal_entity_name")
    ).distinct()

    # Generate quarters from Jan 2023 (Q1 2023) through Oct 2025 (Q4 2025)
    quarters_data = []
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2025, 10, 1)

    current_date = start_date
    while current_date <= end_date:
        year = current_date.year
        quarter = (current_date.month - 1) // 3 + 1
        fiscal_quarter = f"FY{year}Q{quarter}"
        fiscal_year = year
        quarters_data.append({
            "fiscal_year": fiscal_year,
            "fiscal_quarter": quarter,
            "fiscal_quarter_name": fiscal_quarter,
            "quarter_start_date": current_date.strftime("%Y-%m-%d")
        })
        # Move to next quarter
        month = current_date.month + 3
        year = current_date.year
        if month > 12:
            month = month - 12
            year = year + 1
        current_date = datetime(year, month, 1)

    # Create quarters dataframe
    quarters_df = spark.createDataFrame(quarters_data)

    # Cross join cost centers/legal entities with quarters to create all scenario combinations
    scenarios_df = parsed_df.crossJoin(quarters_df)

    # Create scenario_key as combination of cost_center_id, legal_entity_id, and fiscal_quarter_name
    scenarios_df = scenarios_df.withColumn(
        "scenario_key",
        F.concat_ws("_",
                   F.col("cost_center_id").cast(StringType()),
                   F.col("legal_entity_id").cast(StringType()),
                   F.col("fiscal_quarter_name"))
    )

    # Add a scenario_id
    scenarios_df = scenarios_df.withColumn(
        "scenario_id",
        F.monotonically_increasing_id()
    )

    return scenarios_df.select(
        "scenario_id",
        "scenario_key",
        "cost_center_id",
        "cost_center_name",
        "legal_entity_id",
        "legal_entity_name",
        "fiscal_year",
        "fiscal_quarter",
        "fiscal_quarter_name",
        "quarter_start_date"
    )

@dp.table(
    name=f"{catalog}.{schema}.fact_fpa_actuals",
    comment="FPA actuals fact table with actual spend and amortized salary amounts by scenario",
)
def create_fpa_actuals():
    """
    Create FPA actuals table with typical columns for FPA actuals.
    Combines:
    1. Spend actuals from raw_spend_transactions (schema from spend_transactions.csv)
    2. Pre-aggregated salary data from fact_emp_quarterly_cost
    Joins by scenario (cost center, legal entity, fiscal quarter).
    """

    # ========== PART 1: Process and Aggregate Spend Transactions ==========
    raw_spend_df = spark.table("fin_demo.spend.stg_spend_transactions")

    # Parse the coa_meta JSON field and convert dates
    spend_df = raw_spend_df.select(
        F.col("coa_meta.cost_center_id").alias("cost_center_id"),
        F.col("coa_meta.cost_center_name").alias("cost_center_name"),
        F.col("coa_meta.legal_entity_id").alias("legal_entity_id"),
        F.col("coa_meta.legal_entity_name").alias("legal_entity_name"),
        F.col("invoice_date"),
        F.col("amount_paid").cast(DecimalType(18, 2)).alias("amount_paid")
    )

    # Convert Unix timestamps to dates and extract fiscal period info
    spend_df = spend_df.withColumn(
        "transaction_date",
        F.from_unixtime(F.col("invoice_date")).cast(DateType())
    )

    spend_df = spend_df.withColumn(
        "fiscal_year",
        F.year(F.col("transaction_date"))
    ).withColumn(
        "fiscal_quarter",
        F.quarter(F.col("transaction_date"))
    ).withColumn(
        "fiscal_quarter_name",
        F.concat(
            F.lit("FY"),
            F.year(F.col("transaction_date")),
            F.lit("Q"),
            F.quarter(F.col("transaction_date"))
        )
    )

    # Aggregate spend transactions by scenario grain
    spend_agg = spend_df.groupBy(
        "cost_center_id",
        "cost_center_name",
        "legal_entity_id",
        "legal_entity_name",
        "fiscal_year",
        "fiscal_quarter",
        "fiscal_quarter_name"
    ).agg(
        F.sum("amount_paid").alias("spend_amount"),
        F.count("*").alias("transaction_count"),
        F.avg("amount_paid").alias("avg_transaction_amount"),
        F.max("transaction_date").alias("latest_transaction_date"),
        F.min("transaction_date").alias("earliest_transaction_date")
    )

    # ========== PART 2: Read Pre-Aggregated Salary Data ==========
    salary_df = spark.table("fin_demo.hr.fact_emp_quarterly_cost")

    # Select and rename columns to match join grain
    salary_agg = salary_df.select(
        F.col("cost_center_id"),
        F.col("legal_entity_id"),
        F.col("employment_year").alias("fiscal_year"),
        F.col("employment_quarter").alias("fiscal_quarter"),
        F.col("agg_qtr_salary").cast(DecimalType(18, 2)).alias("salary_amount")
    )

    # ========== PART 3: Join Spend and Salary Actuals ==========
    # Left join spend with salary on scenario grain
    actuals_agg = spend_agg.join(
        salary_agg,
        on=["cost_center_id", "legal_entity_id", "fiscal_year", "fiscal_quarter"],
        how="left"
    )

    # Calculate total actual amount and handle nulls
    actuals_agg = actuals_agg.withColumn(
        "salary_amount",
        F.coalesce(F.col("salary_amount"), F.lit(0).cast(DecimalType(18, 2)))
    ).withColumn(
        "actual_amount",
        (F.col("spend_amount") + F.col("salary_amount")).cast(DecimalType(18, 2))
    ).withColumn(
        "actual_paid_amount",
        F.col("spend_amount")  # Only spend has paid amounts
    )

    # Create scenario_key to match with dim_fpa_scenarios
    actuals_agg = actuals_agg.withColumn(
        "scenario_key",
        F.concat_ws("_",
                   F.col("cost_center_id").cast(StringType()),
                   F.col("legal_entity_id").cast(StringType()),
                   F.col("fiscal_quarter_name"))
    )

    # Add additional typical FPA columns
    actuals_agg = actuals_agg.withColumn(
        "actual_id",
        F.monotonically_increasing_id()
    ).withColumn(
        "record_type",
        F.lit("ACTUAL")
    ).withColumn(
        "currency",
        F.lit("USD")
    ).withColumn(
        "created_date",
        F.current_timestamp()
    )

    return actuals_agg.select(
        "actual_id",
        "scenario_key",
        "cost_center_id",
        "cost_center_name",
        "legal_entity_id",
        "legal_entity_name",
        "fiscal_year",
        "fiscal_quarter",
        "fiscal_quarter_name",
        "record_type",
        "actual_amount",
        "spend_amount",
        "salary_amount",
        "actual_paid_amount",
        "transaction_count",
        "avg_transaction_amount",
        "earliest_transaction_date",
        "latest_transaction_date",
        "currency",
        "created_date"
    )

@dp.table(
    name="fin_demo.plan.fact_fpa_budgets",
    comment="FPA budgets fact table - budgets are between 20% higher and 10% lower than actuals",
)
def create_fpa_budgets():
    """
    Create FPA budgets table with typical columns for FPA budgets.
    Budgets are generated to be between 20% higher and 10% lower than actuals for each scenario.
    """
    actuals_df = spark.table("fin_demo.plan.fact_fpa_actuals")

    # Generate random variance between -0.20 and 0.30 (20% lower to 30% higher)
    budgets_df = actuals_df.select(
        F.col("scenario_key"),
        F.col("cost_center_id"),
        F.col("cost_center_name"),
        F.col("legal_entity_id"),
        F.col("legal_entity_name"),
        F.col("fiscal_year"),
        F.col("fiscal_quarter"),
        F.col("fiscal_quarter_name"),
        F.col("actual_amount"),
        F.col("currency")
    )

    # Apply random variance: budget = actual * (1 + random(-0.20, 0.30))
    budgets_df = budgets_df.withColumn(
        "variance_factor",
        F.lit(1.0) + (F.rand() * 0.50 - 0.20)  # Random between -0.20 and 0.30
    ).withColumn(
        "budget_amount",
        (F.col("actual_amount") * F.col("variance_factor")).cast(DecimalType(18, 2))
    ).withColumn(
        "variance_amount",
        (F.col("budget_amount") - F.col("actual_amount")).cast(DecimalType(18, 2))
    ).withColumn(
        "variance_percent",
        (F.col("variance_amount") / F.col("actual_amount") * 100).cast(DecimalType(10, 2))
    )

    # Add typical budget columns
    budgets_df = budgets_df.withColumn(
        "budget_id",
        F.monotonically_increasing_id()
    ).withColumn(
        "record_type",
        F.lit("BUDGET")
    ).withColumn(
        "budget_version",
        F.lit("V1")
    ).withColumn(
        "approved_date",
        F.date_sub(F.concat(F.col("fiscal_year").cast(StringType()), F.lit("-01-01")).cast(DateType()), 30)
    ).withColumn(
        "created_date",
        F.current_timestamp()
    )

    return budgets_df.select(
        "budget_id",
        "scenario_key",
        "cost_center_id",
        "cost_center_name",
        "legal_entity_id",
        "legal_entity_name",
        "fiscal_year",
        "fiscal_quarter",
        "fiscal_quarter_name",
        "record_type",
        "budget_amount",
        "actual_amount",
        "variance_amount",
        "variance_percent",
        "budget_version",
        "approved_date",
        "currency",
        "created_date"
    )

@dp.table(
    name="fin_demo.plan.fact_fpa_forecasts",
    comment="FPA forecasts fact table - forecasts are between 30% higher and 20% lower than actuals",
)
def create_fpa_forecasts():
    """
    Create FPA forecasts table with typical columns for FPA forecasts.
    Forecasts are generated to be between 30% higher and 20% lower than actuals for each scenario.
    """
    actuals_df = spark.table("fin_demo.plan.fact_fpa_actuals")

    # Generate random variance between -0.35 and 0.50 (35% lower to 50% higher)
    forecasts_df = actuals_df.select(
        F.col("scenario_key"),
        F.col("cost_center_id"),
        F.col("cost_center_name"),
        F.col("legal_entity_id"),
        F.col("legal_entity_name"),
        F.col("fiscal_year"),
        F.col("fiscal_quarter"),
        F.col("fiscal_quarter_name"),
        F.col("actual_amount"),
        F.col("currency")
    )

    # Apply random variance: forecast = actual * (1 + random(-0.35, 0.50))
    forecasts_df = forecasts_df.withColumn(
        "variance_factor",
        F.lit(1.0) + (F.rand() * 0.85 - 0.35)  # Random between -0.35 and 0.50
    ).withColumn(
        "forecast_amount",
        (F.col("actual_amount") * F.col("variance_factor")).cast(DecimalType(18, 2))
    ).withColumn(
        "variance_to_actual",
        (F.col("forecast_amount") - F.col("actual_amount")).cast(DecimalType(18, 2))
    ).withColumn(
        "variance_to_actual_percent",
        (F.col("variance_to_actual") / F.col("actual_amount") * 100).cast(DecimalType(10, 2))
    )

    # Add typical forecast columns
    forecasts_df = forecasts_df.withColumn(
        "forecast_id",
        F.monotonically_increasing_id()
    ).withColumn(
        "record_type",
        F.lit("FORECAST")
    ).withColumn(
        "forecast_version",
        F.lit("V1")
    ).withColumn(
        "forecast_type",
        F.lit("ROLLING")
    ).withColumn(
        "forecast_date",
        F.add_months(F.concat(F.col("fiscal_year").cast(StringType()), F.lit("-"),
                             F.lpad((F.col("fiscal_quarter") * 3 - 2).cast(StringType()), 2, "0"),
                             F.lit("-01")).cast(DateType()), 1)
    )

    return forecasts_df.select(
        "forecast_id",
        "scenario_key",
        "cost_center_id",
        "cost_center_name",
        "legal_entity_id",
        "legal_entity_name",
        "fiscal_year",
        "fiscal_quarter",
        "fiscal_quarter_name",
        "record_type",
        "forecast_amount",
        "forecast_version",
        "forecast_type",
        "forecast_date",
        "currency",
    )
