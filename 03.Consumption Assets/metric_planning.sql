ALTER VIEW fin_demo.plan.metrics_planning AS
$$
version: 1.1
source: fin_demo.plan.dim_fpa_scenarios
comment: Financial planning metrics comparing budgets, forecasts, and actuals across cost centers and legal entities

joins:
  - name: budgets
    source: fin_demo.plan.fact_fpa_budgets
    on: source.scenario_key = budgets.scenario_key

  - name: forecasts
    source: fin_demo.plan.fact_fpa_forecasts
    on: source.scenario_key = forecasts.scenario_key

  - name: actuals
    source: fin_demo.plan.fact_fpa_actuals
    on: source.scenario_key = actuals.scenario_key

dimensions:
  - name: fiscal_year
    expr: fiscal_year
    comment: Fiscal year for the planning period
    display_name: Fiscal Year
    format:
      type: number
      decimal_places:
        type: all
      hide_group_separator: true
    synonyms:
      - year
      - fy

  - name: fiscal_quarter
    expr: fiscal_quarter
    comment: Fiscal quarter name for the planning period
    display_name: Fiscal Quarter
    synonyms:
      - quarter
      - period

  - name: cost_center
    expr: cost_center_name
    comment: Cost center or department name
    display_name: Cost Center
    synonyms:
      - department
      - division

  - name: legal_entity
    expr: legal_entity_name
    comment: Legal entity or company name
    display_name: Legal Entity
    synonyms:
      - entity
      - company
      - organization

  - name: quarter_start
    expr: DATE(quarter_start_date)
    comment: Start date of the fiscal quarter
    display_name: Quarter Start Date
    format:
      type: date
      date_format: year_month_day
      leading_zeros: true
    synonyms:
      - period start
      - start date
    
  - name: fiscal_quarter_name
    expr: CONCAT('FY', fiscal_year, 'Q', fiscal_quarter)
    comment: Readable name for fiscal year and fiscal quarter combination
    display_name: Fiscal Quarter Name

measures:
  - name: total_budget
    expr: SUM(budgets.budget_amount)
    comment: Total budgeted amount for the planning period
    display_name: Total Budget
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2
      hide_group_separator: false
      abbreviation: compact
    synonyms:
      - budget
      - budgeted amount

  - name: total_forecast
    expr: SUM(forecasts.forecast_amount)
    comment: Total forecasted amount for the planning period
    display_name: Total Forecast
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2
      hide_group_separator: false
      abbreviation: compact
    synonyms:
      - forecast
      - forecasted amount
      - projection

  - name: total_actual
    expr: SUM(actuals.actual_amount)
    comment: Total actual spending for the planning period
    display_name: Total Actual
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2
      hide_group_separator: false
      abbreviation: compact
    synonyms:
      - actual
      - actual spending

  - name: budget_variance
    expr: SUM(budgets.budget_amount) - SUM(actuals.actual_amount)
    comment: Variance between budget and actual (positive means under budget)
    display_name: Budget Variance
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2
      hide_group_separator: false
      abbreviation: compact
    synonyms:
      - variance
      - budget vs actual

  - name: forecast_accuracy
    expr: (1 - ABS(SUM(forecasts.forecast_amount) - SUM(actuals.actual_amount)) / NULLIF(SUM(actuals.actual_amount), 0)) 
    comment: Forecast accuracy percentage (100% means perfect forecast)
    display_name: Forecast Accuracy
    format:
      type: percentage
      decimal_places:
        type: exact
        places: 2
      hide_group_separator: false
    synonyms:
      - accuracy
      - forecast precision

  - name: budget_utilization
    expr: (SUM(actuals.actual_amount) / NULLIF(SUM(budgets.budget_amount), 0))
    comment: Percentage of budget utilized (actual/budget * 100)
    display_name: Budget Utilization
    format:
      type: percentage
      decimal_places:
        type: exact
        places: 2
      hide_group_separator: false
    synonyms:
      - utilization
      - spend rate

  - name: transaction_volume
    expr: SUM(actuals.transaction_count)
    comment: Total number of transactions across all actuals
    display_name: Transaction Volume
    format:
      type: number
      decimal_places:
        type: exact
        places: 2
      hide_group_separator: true
    synonyms:
      - transactions
      - transaction count


$$
