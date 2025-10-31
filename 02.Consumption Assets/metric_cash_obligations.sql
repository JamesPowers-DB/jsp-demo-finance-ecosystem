USE CATALOG main;
USE SCHEMA finance_lakehouse;

CREATE OR REPLACE VIEW metric_cash_obligations
WITH METRICS
LANGUAGE YAML
AS
$$
version: 1.1
source: stg_spend_transactions
comment: "Cash flow and contractual obligation metrics analyzing payment cycles and committed spend"

joins:
  - name: contracts
    source: dim_all_contracts
    on: source.contract_id = contracts.contract_id

dimensions:
  - name: invoice_year
    expr: YEAR(invoice_date)
    comment: "Invoice year"
    display_name: "Invoice Year"
    format:
      type: number
      decimal_places:
        type: all
      hide_group_separator: true

  - name: invoice_month
    expr: MONTH(invoice_date)
    comment: "Invoice month (1-12)"
    display_name: "Invoice Month"

  - name: invoice_status
    expr: invoice_status
    comment: "Invoice status (paid, pending, cancelled, etc.)"
    display_name: "Invoice Status"
    synonyms:
      - status
      - payment status

  - name: supplier
    expr: supplier_name
    comment: "Supplier name"
    display_name: "Supplier Name"
    synonyms:
      - vendor

  - name: legal_entity
    expr: legal_entity_id
    comment: "Legal entity identifier"
    display_name: "Legal Entity ID"

  - name: contract_status
    expr: contracts.contract_status
    comment: "Contract status"
    display_name: "Contract Status"

  - name: agreement_type
    expr: contracts.agreement_type
    comment: "Agreement type (Inbound/Outbound)"
    display_name: "Agreement Type"

measures:
  - name: total_invoice_amount
    expr: SUM(total_invoice_amount)
    comment: "Total invoice amount including taxes and discounts"
    display_name: "Total Invoice Amount"
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2
      abbreviation: compact
    synonyms:
      - invoice total
      - invoice value

  - name: amount_paid
    expr: SUM(amount_paid)
    comment: "Total amount paid"
    display_name: "Amount Paid"
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2
      abbreviation: compact
    synonyms:
      - paid amount
      - payments

  - name: outstanding_payables
    expr: SUM(total_invoice_amount) - SUM(amount_paid)
    comment: "Outstanding payables (invoiced but not yet paid)"
    display_name: "Outstanding Payables"
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2
      abbreviation: compact
    synonyms:
      - payables
      - unpaid invoices
      - accounts payable

  - name: days_payable_outstanding
    expr: |
      CASE
        WHEN SUM(total_invoice_amount) = 0 THEN NULL
        ELSE (SUM(total_invoice_amount) - SUM(amount_paid)) / NULLIF(SUM(total_invoice_amount), 0) * 365 / 12
      END
    comment: "Days Payable Outstanding - average number of days to pay invoices"
    display_name: "Days Payable Outstanding (DPO)"
    format:
      type: number
      decimal_places:
        type: exact
        places: 1
      hide_group_separator: false
    synonyms:
      - dpo
      - payment days
      - average payment period

  - name: total_contracted_value
    expr: SUM(contracts.total_contract_value)
    comment: "Total value of contracts"
    display_name: "Total Contracted Value"
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2
      abbreviation: compact
    synonyms:
      - contract value
      - committed amount

  - name: committed_vs_spent
    expr: SUM(contracts.total_contract_value) - SUM(total_invoice_amount)
    comment: "Committed but not yet invoiced (shows what's contracted but not yet spent)"
    display_name: "Committed vs Spent"
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2
      abbreviation: compact
    synonyms:
      - committed not invoiced
      - uninvoiced commitments
      - remaining contract value

  - name: invoice_count
    expr: COUNT(DISTINCT invoice_id)
    comment: "Number of unique invoices"
    display_name: "Invoice Count"
    format:
      type: number
      decimal_places:
        type: all
      hide_group_separator: true
    synonyms:
      - number of invoices
      - invoice volume
$$;
