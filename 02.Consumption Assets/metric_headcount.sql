USE CATALOG main;
USE SCHEMA finance_lakehouse;

CREATE OR REPLACE VIEW metric_headcount
WITH METRICS
LANGUAGE YAML
AS
$$
version: 1.1
source: fact_emp_quarterly_cost
comment: "Headcount productivity metrics analyzing revenue and spend efficiency per employee"

joins:
  - name: revenue
    source: fact_revenue_recognition
    on: source.employment_year = YEAR(revenue.period_month) AND source.employment_quarter = QUARTER(revenue.period_month) AND source.legal_entity_id = revenue.legal_entity_id

  - name: spend
    source: fact_spend_recognition
    on: source.employment_year = YEAR(spend.period_month) AND source.employment_quarter = QUARTER(spend.period_month) AND source.legal_entity_id = spend.legal_entity_id

dimensions:
  - name: year
    expr: employment_year
    comment: "Employment year"
    display_name: "Year"
    format:
      type: number
      decimal_places:
        type: all
      hide_group_separator: true

  - name: quarter
    expr: employment_quarter
    comment: "Employment quarter (1-4)"
    display_name: "Quarter"
    synonyms:
      - fiscal quarter
      - qtr

  - name: cost_center
    expr: cost_center_id
    comment: "Cost center identifier"
    display_name: "Cost Center ID"

  - name: legal_entity
    expr: legal_entity_id
    comment: "Legal entity identifier"
    display_name: "Legal Entity ID"

measures:
  - name: quarterly_headcount_cost
    expr: SUM(agg_qtr_salary)
    comment: "Total quarterly salary costs across all employees"
    display_name: "Quarterly Headcount Cost"
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2
      abbreviation: compact
    synonyms:
      - headcount cost
      - salary expense
      - labor cost

  - name: quarterly_revenue
    expr: SUM(revenue.rev_recognized_amount)
    comment: "Total quarterly revenue recognized"
    display_name: "Quarterly Revenue"
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2
      abbreviation: compact
    synonyms:
      - revenue
      - recognized revenue

  - name: revenue_per_employee
    expr: SUM(revenue.rev_recognized_amount) / NULLIF(SUM(agg_qtr_salary), 0) * 100000
    comment: "Quarterly revenue per $100K of employee cost (productivity ratio)"
    display_name: "Revenue per $100 Employee Cost"
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2
    synonyms:
      - revenue efficiency
      - employee productivity
      - revenue per headcount

  - name: revenue_per_sales_employee
    expr: SUM(revenue.rev_recognized_amount) / NULLIF(SUM(agg_qtr_salary) FILTER (WHERE source.cost_center_id = 1002), 0) * 100000
    comment: "Quarterly revenue per $100K of sales department employee cost (cost center 1002)"
    display_name: "Revenue per $100K Sales Employee Cost"
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2
    synonyms:
      - sales productivity
      - sales efficiency
      - sales revenue per headcount

  - name: quarterly_spend
    expr: SUM(spend.spend_amount)
    comment: "Total quarterly spend"
    display_name: "Quarterly Spend"
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2
      abbreviation: compact
    synonyms:
      - spend
      - expenditure

  - name: spend_per_employee
    expr: SUM(spend.spend_amount) / NULLIF(SUM(agg_qtr_salary), 0) * 100000
    comment: "Quarterly spend per $100K of employee cost"
    display_name: "Spend per $100K Employee Cost"
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2
    synonyms:
      - spend efficiency
      - spend per headcount
$$;
