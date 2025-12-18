USE CATALOG main;
USE SCHEMA finance_lakehouse;

CREATE OR REPLACE VIEW metric_planning
WITH METRICS
LANGUAGE YAML
AS
$$
version: 1.1
source: dim_fpa_scenarios
comment: "Budget variance and forecast performance metrics comparing planned vs actual financial performance"

joins:
  - name: budgets
    source: fact_fpa_budgets
    on: source.scenario_key = budgets.scenario_key


  - name: actuals
    source: fact_fpa_actuals
    on: source.scenario_key = actuals.scenario_key

  - name: forecast
    source: fact_fpa_forecasts
    on: source.scenario_key = forecast.scenario_key

dimensions:
  - name: fiscal_year
    expr: fiscal_year
    comment: "Fiscal year"
    display_name: "Fiscal Year"
    format:
      type: number
      decimal_places:
        type: all
      hide_group_separator: true

  - name: fiscal_quarter
    expr: fiscal_quarter
    comment: "Fiscal quarter (1-4)"
    display_name: "Fiscal Quarter"
    synonyms:
      - quarter
      - qtr

  - name: fiscal_quarter_name
    expr: CONCAT('FY', fiscal_year, 'Q', fiscal_quarter)
    comment: Readable name for fiscal year and fiscal quarter combination
    display_name: Fiscal Quarter Name

  - name: cost_center_id
    expr: cost_center_id
    comment: "Cost center identifier"
    display_name: "Cost Center ID"

  - name: cost_center
    expr: cost_center_name
    comment: "Cost center name"
    display_name: "Cost Center Name"

  - name: legal_entity_id
    expr: legal_entity_id
    comment: "Legal entity identifier"
    display_name: "Legal Entity ID"

  - name: legal_entity
    expr: legal_entity_name
    comment: "Legal entity name"
    display_name: "Legal Entity Name"

measures:
  - name: budget_amount
    expr: SUM(budgets.budget_amount)
    comment: "Total budgeted amount"
    display_name: "Budget Amount"
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2
      abbreviation: compact
    synonyms:
      - budget
      - planned amount

  - name: actual_amount
    expr: SUM(actuals.actual_amount)
    comment: "Total actual amount"
    display_name: "Actual Amount"
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2
      abbreviation: compact
    synonyms:
      - actuals
      - actual spend

  - name: forecast_amount
    expr: SUM(forecast.forecast_amount)
    comment: "Total Forecast amount"
    display_name: "Forecast Amount"
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2
      abbreviation: compact
    synonyms:
      - forecast
      - forecasted amount
      - projection

  - name: variance_amount
    expr: SUM(budgets.budget_amount) - SUM(actuals.actual_amount)
    comment: "Budget variance (positive = under budget, negative = over budget)"
    display_name: "Budget vs Actual Variance"
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2
      abbreviation: compact
    synonyms:
      - variance
      - budget variance
      - bva

  - name: variance_percent
    expr: |
      CASE
        WHEN SUM(budgets.budget_amount) = 0 THEN NULL
        ELSE ((SUM(budgets.budget_amount) - SUM(actuals.actual_amount)) / SUM(budgets.budget_amount))
      END
    comment: "Budget variance percentage (positive = under budget, negative = over budget)"
    display_name: "Budget vs Actual Variance %"
    format:
      type: percentage
      decimal_places:
        type: exact
        places: 2
      hide_group_separator: false
    synonyms:
      - variance pct
      - variance percentage
      - bva percent
      
  - name: forecast_accuracy
    expr: forecast_amount / actual_amount
    comment: Forecast proximity to actual spend
    display_name: Forecast Accuracy
    format:
      type: percentage
      decimal_places:
        type: exact
        places: 2
      hide_group_separator: false
    synonyms:
      - forecast accuracy

  - name: actual_spend
    expr: SUM(actuals.spend_amount)
    comment: "Total actual spend amount"
    display_name: "Actual Spend"
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

  - name: actual_salary
    expr: SUM(actuals.salary_amount)
    comment: "Total actual salary amount"
    display_name: "Actual Salary"
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2
      abbreviation: compact
    synonyms:
      - salary
      - labor cost
$$;
