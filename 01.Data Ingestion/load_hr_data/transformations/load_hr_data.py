from pyspark import pipelines as dp
from pyspark.sql.functions import to_date, col, from_unixtime, expr

catalog = 'fin_demo'
schema = 'hr'

############################################################
# BRONZE LAYER

@dp.table(
    name=f"{catalog}.{schema}.raw_employee_data",
)
def load_raw_rev_transaction_data():

    return spark.read\
        .json(
            path=f'/Volumes/fin_demo/fin/data_gen_outputs/employees/'
        )

############################################################
# SILVER LAYER

############################################################
# GOLD LAYER

@dp.table(
    name=f"{catalog}.{schema}.dim_employees",
)
def fact_employee():
    df = spark.table(f"{catalog}.{schema}.raw_employee_data")

    return df.select(
        col("employee_id").cast("long"),
        col("employee_name").cast("string"),
        col("salary").cast("double"),
        col("cost_center_id").cast("string"),
        col("legal_entity_id").cast("long"),
        # For data story, shift the hire date back 1 year
        (to_date(from_unixtime(col("hire_date"))) - expr("INTERVAL 1 YEAR")).alias("hire_date"),
        # For data story, use the hire date as the termination date
        to_date(from_unixtime(col("hire_date"))).alias("termination_date"),
    
    )