USE CATALOG main;
USE SCHEMA finance_lakehouse;

CREATE OR REPLACE VIEW metric_growth_revenue
WITH METRICS
LANGUAGE YAML
AS
$$
version: 1.1
source: fact_revenue_recognition
comment: "Revenue growth and performance metrics analyzing ARR, MRR, growth trends, and revenue at risk"

joins:
  - name: contracts
    source: dim_all_contracts
    on: source.contract_id = contracts.contract_id

dimensions:
  - name: revenue_year
    expr: YEAR(period_month)
    comment: "Revenue recognition year"
    display_name: "Revenue Year"
    format:
      type: number
      decimal_places:
        type: all
      hide_group_separator: true

  - name: revenue_quarter
    expr: QUARTER(period_month)
    comment: "Revenue recognition quarter (1-4)"
    display_name: "Revenue Quarter"
    synonyms:
      - quarter
      - qtr

  - name: revenue_month
    expr: period_month
    comment: "Revenue recognition month"
    display_name: "Revenue Month"
    format:
      type: date
      date_format: year_month_day
      leading_zeros: true

  - name: customer
    expr: customer_name
    comment: "Customer name"
    display_name: "Customer Name"
    synonyms:
      - client

  - name: legal_entity
    expr: legal_entity_id
    comment: "Legal entity identifier"
    display_name: "Legal Entity ID"

  - name: legal_entity_name
    expr: legal_entity_name
    comment: "Legal entity name"
    display_name: "Legal Entity Name"

  - name: contract_status
    expr: contracts.contract_status
    comment: "Contract status"
    display_name: "Contract Status"

  - name: agreement_type
    expr: contracts.agreement_type
    comment: "Agreement type (Inbound/Outbound)"
    display_name: "Agreement Type"

measures:
  - name: recognized_revenue
    expr: SUM(rev_recognized_amount)
    comment: "Total recognized revenue"
    display_name: "Recognized Revenue"
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2
      abbreviation: compact
    synonyms:
      - revenue
      - rev
      - actuals

  - name: monthly_recurring_revenue
    expr: SUM(rev_recognized_amount) / COUNT(DISTINCT period_month)
    comment: "Monthly Recurring Revenue (MRR) - average monthly revenue"
    display_name: "Monthly Recurring Revenue (MRR)"
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2
      abbreviation: compact
    synonyms:
      - mrr
      - monthly revenue

  - name: annual_recurring_revenue
    expr: (SUM(rev_recognized_amount) / COUNT(DISTINCT period_month)) * 12
    comment: "Annual Recurring Revenue (ARR) - annualized monthly revenue"
    display_name: "Annual Recurring Revenue (ARR)"
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2
      abbreviation: compact
    synonyms:
      - arr
      - annual revenue

  - name: billed_amount
    expr: SUM(rev_billed_amount)
    comment: "Total billed amount"
    display_name: "Billed Amount"
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2
      abbreviation: compact
    synonyms:
      - billings
      - invoiced

  - name: received_amount
    expr: SUM(rev_received_amount)
    comment: "Total received amount"
    display_name: "Received Amount"
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2
      abbreviation: compact
    synonyms:
      - collections
      - cash received

  - name: transaction_count
    expr: SUM(period_transaction_count)
    comment: "Total number of revenue transactions"
    display_name: "Transaction Count"
    format:
      type: number
      decimal_places:
        type: all
      hide_group_separator: true
    synonyms:
      - transactions
      - transaction volume

  - name: contract_revenue_at_risk
    expr: |
      CASE
        WHEN contracts.agreement_type = 'Outbound' AND contracts.contract_status IN ('Active', 'Completed')
        THEN SUM(rev_recognized_amount)
        ELSE NULL
      END
    comment: "Revenue from outbound contracts that are active or completed (shows revenue up for renewal)"
    display_name: "Contract Revenue at Risk"
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2
      abbreviation: compact
    synonyms:
      - revenue at risk
      - renewal revenue
      - churn risk
$$;
