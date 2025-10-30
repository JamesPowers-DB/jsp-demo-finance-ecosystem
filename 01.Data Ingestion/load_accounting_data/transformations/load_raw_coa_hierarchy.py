from pyspark import pipelines as dp

@dp.table(
    name=f"raw_coa_hierarchy",
    comment="Materialized view loaded from JSON files in coa_hierarchy"
)
def load_raw_coa_hierarchy():
    return spark.read.format("json").load(f"/Volumes/finance_lakehouse/data_gen_outputs/coa_hierarchy/")