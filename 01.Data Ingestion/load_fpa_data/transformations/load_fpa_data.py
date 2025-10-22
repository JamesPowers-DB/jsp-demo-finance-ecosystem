
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
    2. Amortized salary data from raw_emp_data (schema from emp_data.csv)
    Aggregates by scenario (cost center, legal entity, fiscal quarter).
    """

    # ========== PART 1: Process Spend Transactions ==========
    raw_spend_df = spark.table("fin_demo.spend.stg_spend_transactions")

    # Parse the coa_meta JSON field and convert dates
    spend_df = raw_spend_df.select(
        F.col("coa_meta.cost_center_id").alias("cost_center_id"),
        F.col("coa_meta.cost_center_name").alias("cost_center_name"),
        F.col("coa_meta.legal_entity_id").alias("legal_entity_id"),
        F.col("coa_meta.legal_entity_name").alias("legal_entity_name"),
        F.col("invoice_date"),
        F.col("invoice_amount").cast(DecimalType(18, 2)).alias("amount"),
        F.col("amount_paid").cast(DecimalType(18, 2)).alias("amount_paid"),
        F.lit("SPEND").alias("amount_type")
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
            F.quarter(F.col("transaction_date")), 
            )
    )

    # ========== PART 2: Process Employee Salary Data ==========
    raw_emp_df = spark.table("fin_demo.hr.dim_employees")

    # Set termination date to 24 months from hire date if not provided
    emp_df = raw_emp_df.withColumn(
        "hire_date_parsed",
        F.to_date(F.col("hire_date"), "yyyy-MM-dd")
    ).withColumn(
        "termination_date_parsed",
        F.coalesce(
            F.to_date(F.col("termination_date"), "yyyy-MM-dd"),
            F.add_months(F.to_date(F.col("hire_date"), "yyyy-MM-dd"), 24)
        )
    )

    # Generate quarters from Jan 2023 through Oct 2025 for salary amortization
    quarters_data = []
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2025, 10, 1)

    current_date = start_date
    while current_date <= end_date:
        year = current_date.year
        quarter = (current_date.month - 1) // 3 + 1

        # Calculate quarter start and end dates
        quarter_start = datetime(year, (quarter - 1) * 3 + 1, 1)
        # Calculate quarter end date
        if quarter == 4:
            quarter_end_day = 31
            quarter_end_month = 12
        else:
            quarter_end_month = quarter * 3
            quarter_end_day = [31, 30, 30][quarter - 1]  # Mar=31, Jun=30, Sep=30

        quarter_end = datetime(year, quarter_end_month, quarter_end_day)

        quarters_data.append({
            "fiscal_year": year,
            "fiscal_quarter": quarter,
            "fiscal_quarter_name": f"FY{year}Q{quarter}",
            "quarter_start_date": quarter_start.strftime("%Y-%m-%d"),
            "quarter_end_date": quarter_end.strftime("%Y-%m-%d")
        })

        # Move to next quarter
        month = current_date.month + 3
        year = current_date.year
        if month > 12:
            month = month - 12
            year = year + 1
        current_date = datetime(year, month, 1)

    quarters_df = spark.createDataFrame(quarters_data)

    # Cross join employees with quarters
    emp_quarters = emp_df.crossJoin(quarters_df)

    # Parse quarter dates
    emp_quarters = emp_quarters.withColumn(
        "quarter_start_parsed",
        F.to_date(F.col("quarter_start_date"), "yyyy-MM-dd")
    ).withColumn(
        "quarter_end_parsed",
        F.to_date(F.col("quarter_end_date"), "yyyy-MM-dd")
    )

    # Filter to only include quarters where the employee was employed
    # Include if quarter overlaps with employment period (hire_date to termination_date)
    emp_quarters = emp_quarters.filter(
        # Quarter start is before termination date
        (F.col("quarter_start_parsed") < F.col("termination_date_parsed")) &
        # Quarter end is on or after hire date
        (F.col("quarter_end_parsed") >= F.col("hire_date_parsed"))
    )

    # Calculate quarterly salary (annual salary / 4) allocated to employee's cost center
    salary_df = emp_quarters.withColumn(
        "quarterly_salary",
        (F.col("salary") / 4.0).cast(DecimalType(18, 2))
    )

    # Get cost center and legal entity name mappings from spend data
    cost_center_names = raw_spend_df.select(
        F.col("coa_meta.cost_center_id").alias("cc_id"),
        F.col("coa_meta.cost_center_name").alias("cc_name")
    ).distinct()

    legal_entity_names = raw_spend_df.select(
        F.col("coa_meta.legal_entity_id").alias("le_id"),
        F.col("coa_meta.legal_entity_name").alias("le_name")
    ).distinct()

    # Join to get cost center and legal entity names
    salary_df = salary_df.join(
        cost_center_names,
        salary_df.cost_center_id == cost_center_names.cc_id,
        "left"
    ).join(
        legal_entity_names,
        salary_df.legal_entity_id == legal_entity_names.le_id,
        "left"
    )

    # Select relevant columns for salary actuals
    salary_actuals = salary_df.select(
        F.col("cost_center_id"),
        F.coalesce(F.col("cc_name"), F.lit("Unknown")).alias("cost_center_name"),
        F.col("legal_entity_id"),
        F.coalesce(F.col("le_name"), F.lit("Unknown")).alias("legal_entity_name"),
        F.col("quarter_start_parsed").alias("transaction_date"),
        F.col("quarterly_salary").alias("amount"),
        F.col("quarterly_salary").alias("amount_paid"),
        F.lit("SALARY").alias("amount_type"),
        F.col("fiscal_year"),
        F.col("fiscal_quarter"),
        F.col("fiscal_quarter_name")
    )

    # ========== PART 3: Combine Spend and Salary Actuals ==========
    # Union spend and salary data
    combined_actuals = spend_df.select(
        "cost_center_id",
        "cost_center_name",
        "legal_entity_id",
        "legal_entity_name",
        "transaction_date",
        "amount",
        "amount_paid",
        "amount_type",
        "fiscal_year",
        "fiscal_quarter",
        "fiscal_quarter_name"
    ).union(salary_actuals)

    # Create scenario_key to match with dim_fpa_scenarios
    combined_actuals = combined_actuals.withColumn(
        "scenario_key",
        F.concat_ws("_",
                   F.col("cost_center_id").cast(StringType()),
                   F.col("legal_entity_id").cast(StringType()),
                   F.col("fiscal_quarter_name"))
    )

    # Aggregate actuals by scenario
    actuals_agg = combined_actuals.groupBy(
        "scenario_key",
        "cost_center_id",
        "cost_center_name",
        "legal_entity_id",
        "legal_entity_name",
        "fiscal_year",
        "fiscal_quarter",
        "fiscal_quarter_name"
    ).agg(
        F.sum("amount").alias("actual_amount"),
        F.sum(F.when(F.col("amount_type") == "SPEND", F.col("amount")).otherwise(0)).alias("spend_amount"),
        F.sum(F.when(F.col("amount_type") == "SALARY", F.col("amount")).otherwise(0)).alias("salary_amount"),
        F.sum("amount_paid").alias("actual_paid_amount"),
        F.count("*").alias("transaction_count"),
        F.avg("amount").alias("avg_transaction_amount"),
        F.max("transaction_date").alias("latest_transaction_date"),
        F.min("transaction_date").alias("earliest_transaction_date")
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
