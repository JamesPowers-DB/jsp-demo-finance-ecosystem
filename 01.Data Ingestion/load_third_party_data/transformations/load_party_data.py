from pyspark import pipelines as dp

############################################################
# BRONZE LAYER

#TODO : JSP - This seems bad and I hate this
catalog = 'main'
schema = 'finance_lakehouse'

# -- THIRD PARTY DATA -- #
@dp.table(
    name=f"{catalog}.{schema}.raw_third_parties",
    comment="Materialized view loaded from JSON files in third party data volume"
)
def load_raw_():
    return spark.read.format("json").load("/Volumes/main/finance_lakehouse/data_gen_outputs/third_parties/")

# -- SUPPLIER DATA -- #
@dp.table(
    name=f"{catalog}.{schema}.raw_suppliers",
    comment="Materialized view loaded from JSON files in supplier data volume"
)
def load_raw_suppliers():
    return spark.read.format("json").load("/Volumes/main/finance_lakehouse/data_gen_outputs/suppliers/")

# -- CUSTOMER DATA -- #
@dp.table(
    name=f"{catalog}.{schema}.raw_customers",
    comment="Materialized view loaded from JSON files in customer data volume"
)
def load_raw_customers():
    return spark.read.format("json").load("/Volumes/main/finance_lakehouse/data_gen_outputs/customers/")

############################################################
# SILVER LAYER

############################################################
# GOLD LAYER 



@dp.table(
    name=f"{catalog}.{schema}.dim_third_party",
    comment="Materialized view of third party data"
)
def load_dim_third_party():
    
    query = spark.sql(
        f"""
        SELECT
            tp.*,
            supp.supplier_id,
            cust.customer_id,
            cust.customer_industry
        FROM {catalog}.{schema}.raw_third_parties AS tp        
        LEFT JOIN {catalog}.{schema}.raw_suppliers AS supp
            ON tp.third_party_id = supp.third_party_id
        LEFT JOIN {catalog}.{schema}.raw_customers AS cust
            ON tp.third_party_id = cust.third_party_id
        """
    )

    return query