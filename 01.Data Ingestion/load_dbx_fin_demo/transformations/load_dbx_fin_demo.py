from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DoubleType,
    BooleanType
    )
from pyspark import pipelines as dp


############################################################
# BRONZE LAYER

#TODO : JSP - This seems bad and I hate this
catalog = 'fin_demo'
schema = 'dbx'

# -- THIRD PARTY DATA -- #
@dp.table(
    name=f"{catalog}.{schema}.raw_customer_matrix",
    comment="Raw customer mapping"
)
def load_raw_customer():

    from pyspark.sql.functions import rank
    from pyspark.sql.window import Window

    
    
    schema = StructType([
        StructField("anon_name", StringType(), nullable=False),
        StructField("industry_vertical", StringType(), nullable=False),
        StructField("business_unit", StringType(), nullable=False),
        StructField("sub_business_unit", StringType(), nullable=False),
        StructField("platform", StringType(), nullable=False),
        ])
    
    customer_df = spark.read\
        .csv(
            path=f'/Volumes/fin_demo/fin/data_gen_outputs/dbx_anon/anon_dbx_names.csv',
            header=True,
            schema=schema,
        )\
        .withColumn("account_id", 
            (rank().over(Window.orderBy("anon_name")) + 10000)
        )

    return customer_df
    
        

