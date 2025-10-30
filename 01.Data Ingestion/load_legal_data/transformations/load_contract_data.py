from pyspark import pipelines as dp


############################################################
# BRONZE LAYER

#TODO : JSP - This seems bad and I hate this
catalog = 'main'
schema = 'finance_lakehouse'


@dp.table(
    name=f"{catalog}.{schema}.raw_outbound_contracts",
    comment="Materialized view loaded from JSON files in third party data volume"
)
def load_raw_():
    return spark.read.format("json").load("/Volumes/main/finance_lakehouse/data_gen_outputs/outbound_contracts/")

@dp.table(
    name=f"{catalog}.{schema}.raw_inbound_contracts",
    comment="Materialized view loaded from JSON files in supplier data volume"
)
def load_raw_suppliers():
    return spark.read.format("json").load("/Volumes/main/finance_lakehouse/data_gen_outputs/inbound_contracts/")



############################################################
# SILVER LAYER


############################################################
# GOLD LAYER 


@dp.table(
    name=f"{catalog}.{schema}.dim_all_contracts",
    comment="Materialized view of third party data"
)
def load_dim_third_party():
    
    universal_contract_cols = [
        "contract_id",
        "contract_number",
        "legal_entity_id",
        "contract_currency",
        "total_contract_value",
        "contract_start_date",
        "estimated_completion_date",
        "contract_status",
        "contract_type",
        "contract_description",
        "payment_terms",
        ]
        
    query = spark.sql(
        f"""
        WITH base_contracts AS (
            SELECT 
                /*+ BROADCAST(party) */
                {','.join(universal_contract_cols)}
                ,'Outbound' AS agreement_type 
                ,party.third_party_id
                ,party.third_party_name
            FROM {catalog}.{schema}.raw_outbound_contracts AS outbound
            LEFT JOIN {catalog}.party.dim_third_party AS party
                ON outbound.customer_id = party.customer_id
            UNION ALL
            SELECT 
                /*+ BROADCAST(party) */
                {','.join(universal_contract_cols)}
                ,'Inbound' AS agreement_type 
                ,party.third_party_id
                ,party.third_party_name
            FROM {catalog}.{schema}.raw_inbound_contracts AS inbound
            LEFT JOIN {catalog}.party.dim_third_party AS party
                ON inbound.supplier_id = party.supplier_id

        )
        SELECT 
            /*+ BROADCAST(le) */
            base_contracts.contract_id
            ,base_contracts.contract_number
            ,base_contracts.third_party_id
            ,base_contracts.third_party_name
            ,base_contracts.agreement_type
            ,base_contracts.contract_type
            ,base_contracts.contract_description
            ,base_contracts.contract_currency
            ,base_contracts.total_contract_value
            ,FROM_UNIXTIME(base_contracts.contract_start_date, 'yyyy-MM-dd') AS contract_start_date
            ,FROM_UNIXTIME(base_contracts.estimated_completion_date, 'yyyy-MM-dd') AS estimated_completion_date
            ,base_contracts.contract_status
            ,base_contracts.legal_entity_id
            ,le.legal_entity_name
            ,base_contracts.payment_terms

        FROM base_contracts

        LEFT JOIN (
            SELECT DISTINCT
                legal_entity_id
                ,legal_entity_name
            FROM {catalog}.acct.raw_coa_hierarchy 
            )AS le
            ON base_contracts.legal_entity_id = le.legal_entity_id
        """
    )

    return query