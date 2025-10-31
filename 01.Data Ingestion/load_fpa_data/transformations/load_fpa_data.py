
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DecimalType, IntegerType, DateType
from datetime import datetime

catalog = 'main'
schema = 'finance_lakehouse'


############################################################
# -- BRONZE LAYER -- #

############################################################
# -- BRONZE LAYER -- #

############################################################
# -- GOLD LAYER -- #

@dp.table(
    name=f"dim_fpa_scenarios",
    comment="Scenario dimension table with quarters from Jan 2023 through Oct 2025",
)
def create_fpa_scenarios():
    """
    Create scenario dimension table for quarters starting in Jan 2023 through Oct 2025.
    Scenario is based on cost center and legal entity.
    Source: raw_spend_transactions table (schema from spend_transactions.csv)
    """
    # Get distinct cost centers and legal entities from the raw spend data
    raw_df = spark.table("stg_spend_transactions")

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
    name=f"fact_fpa_actuals",
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
    raw_spend_df = spark.table("stg_spend_transactions")

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
    salary_df = spark.table("fact_emp_quarterly_cost")

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
    name="fact_fpa_budgets",
    comment="FPA budgets fact table - budgets are based on actuals with random variance, current quarter is 8% higher than previous quarter",
)
def create_fpa_budgets():
    """
    Create FPA budgets table with typical columns for FPA budgets.
    For past quarters: budgets are generated with random variance from actuals.
    For current quarter: budget is 8% higher than the previous quarter's budget.
    """
    actuals_df = spark.table("fact_fpa_actuals")

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

    # Get current date and fiscal period
    current_date = F.current_date()
    budgets_df = budgets_df.withColumn("current_date", current_date)
    budgets_df = budgets_df.withColumn("current_year", F.year(current_date))
    budgets_df = budgets_df.withColumn("current_quarter", F.quarter(current_date))

    # Determine if this is the current quarter
    budgets_df = budgets_df.withColumn(
        "is_future_quarter",
        ((F.col("fiscal_year") == F.col("current_year")) & (F.col("fiscal_quarter") == F.col("current_quarter"))) 
        | (F.col("fiscal_year") > F.col("current_year"))
    )

    # Calculate quarter start and end dates for percent complete calculation
    budgets_df = budgets_df.withColumn(
        "quarter_start_date",
        F.make_date(
            F.col("fiscal_year"),
            ((F.col("fiscal_quarter") - 1) * 3 + 1).cast(IntegerType()),
            F.lit(1)
        )
    ).withColumn(
        "quarter_end_date",
        F.last_day(F.make_date(
            F.col("fiscal_year"),
            (F.col("fiscal_quarter") * 3).cast(IntegerType()),
            F.lit(1)
        ))
    )

    # For past quarters: apply random variance between -0.20 and 0.30 (20% lower to 30% higher)
    # Special case: 2024 Q4 should be 15% lower than actuals
    budgets_df = budgets_df.withColumn(
        "variance_factor",
        F.when(
            (F.col("fiscal_year") == 2024) & (F.col("fiscal_quarter") == 4),
            F.lit(0.85)  # 15% lower than actuals
        ).otherwise(
            F.lit(1.0) + (F.rand() * 0.50 - 0.20)
        )
    ).withColumn(
        "budget_amount_base",
        (F.col("actual_amount") * F.col("variance_factor")).cast(DecimalType(18, 2))
    )

    # Create a window to get previous quarter's budget for each cost center/legal entity
    from pyspark.sql import Window
    window_spec = Window.partitionBy("cost_center_id", "legal_entity_id").orderBy("fiscal_year", "fiscal_quarter")

    budgets_df = budgets_df.withColumn(
        "prev_quarter_budget",
        F.lag("budget_amount_base", 1).over(window_spec)
    )

    # For current quarter: use 8% higher than previous quarter, else use base calculation
    budgets_df = budgets_df.withColumn(
        "budget_amount",
        F.when(
            F.col("is_future_quarter") & F.col("prev_quarter_budget").isNotNull(),
            (F.col("prev_quarter_budget") * 1.08).cast(DecimalType(18, 2))
        ).otherwise(F.col("budget_amount_base"))
    )

    # Calculate variance
    budgets_df = budgets_df.withColumn(
        "variance_amount",
        (F.col("budget_amount") - F.col("actual_amount")).cast(DecimalType(18, 2))
    ).withColumn(
        "variance_percent",
        F.when(
            F.col("actual_amount") != 0,
            (F.col("variance_amount") / F.col("actual_amount") * 100).cast(DecimalType(10, 2))
        ).otherwise(F.lit(0).cast(DecimalType(10, 2)))
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
    name="fact_fpa_forecasts",
    comment="FPA forecasts fact table - forecasts are based on budget data with variance",
)
def create_fpa_forecasts():
    """
    Create FPA forecasts table with typical columns for FPA forecasts.
    Forecasts are generated based on budget amounts with random variance applied.
    """
    budgets_df = spark.table("fact_fpa_budgets")

    # Generate random variance between -0.15 and 0.25 (15% lower to 25% higher than budget)
    forecasts_df = budgets_df.select(
        F.col("scenario_key"),
        F.col("cost_center_id"),
        F.col("cost_center_name"),
        F.col("legal_entity_id"),
        F.col("legal_entity_name"),
        F.col("fiscal_year"),
        F.col("fiscal_quarter"),
        F.col("fiscal_quarter_name"),
        F.col("budget_amount"),
        F.col("currency")
    )

    # Apply random variance: forecast = budget * (1 + random(-0.15, 0.25))
    forecasts_df = forecasts_df.withColumn(
        "variance_factor",
        F.lit(1.0) + (F.rand() * 0.40 - 0.15)  # Random between -0.15 and 0.25
    ).withColumn(
        "forecast_amount",
        (F.col("budget_amount") * F.col("variance_factor")).cast(DecimalType(18, 2))
    ).withColumn(
        "variance_to_budget",
        (F.col("forecast_amount") - F.col("budget_amount")).cast(DecimalType(18, 2))
    ).withColumn(
        "variance_to_budget_percent",
        (F.col("variance_to_budget") / F.col("budget_amount") * 100).cast(DecimalType(10, 2))
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
