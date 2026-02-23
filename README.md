# P&C Insurance Data Harmonization

A dbt project demonstrating end-to-end data harmonization across two fictional P&C insurance carriers using Guidewire-style PolicyCenter and ClaimCenter data. The pipeline transforms raw, inconsistently structured source data into a unified, modeling-ready claims dataset.

## Problem

Insurance carriers configure Guidewire differently — column naming conventions, code values, status indicators, and data quality all vary by implementation. Before any predictive modeling can happen, this data must be profiled, standardized, and unified into a common schema.

## Data Sources

Two simulated Guidewire customers with intentionally different conventions:

| Dimension | Customer A (Keystone Mutual) | Customer B (Great Lakes Insurance) |
|---|---|---|
| Column naming | CamelCase | snake_case |
| LOB codes | `WC`, `CA`, `GL` | `WorkersComp`, `CommAuto`, `GenLiab` |
| State format | Abbreviations (`PA`, `NJ`) | Full names (`Illinois`, `Wisconsin`) |
| Claim status | `Open`, `Closed`, `Reopened` | `O`, `C`, `R` |
| Cause of loss | Text descriptions | Numeric/alphanumeric codes |
| Boolean flags | `Y` / `N` | `1` / `0`, `True` / `False` |
| Null handling | Standard nulls | Mix of nulls, empty strings, sentinel values |

Each customer has four source tables: policies, claims, claimants, and transactions. Three shared reference tables (seeds) provide lookup mappings.

## Project Structure

```
models/
├── staging/           # Light cleanup: renaming, type casting, null standardization
│   ├── customer_a/    # 4 models (policies, claims, claimants, transactions)
│   ├── customer_b/    # 4 models
│   └── reference/     # 3 models (states, cause_of_loss, wc_class_codes)
├── intermediate/      # Harmonization: value mapping, unioning, enrichment
│   ├── int_policies_unified.sql
│   ├── int_claims_unified.sql
│   ├── int_claimants_unified.sql
│   └── int_transactions_unified.sql
└── marts/             # Modeling-ready output
    └── fct_claims_modeling.sql
seeds/                 # Reference/lookup CSVs managed by dbt
```

## Lineage

![DAG](assets/lineage.png)

## Transformation Layers

**Staging** - One model per source table. Renames columns to a consistent snake_case schema, casts types, standardizes nulls (empty strings and sentinel values like `99` converted to null), and tags each row with `source_customer`.

**Intermediate** — Unions Customer A and Customer B into a single table per entity. Maps differing code values to a common standard (LOB codes, statuses, state abbreviations, transaction types, cost categories, injury descriptions). Joins reference tables for cause of loss lookups and state mapping. Computes report lag.

**Mart** — Joins all unified tables into a single claim-level fact table with features for severity modeling:
- Policy attributes (LOB, class code, premium, renewal count)
- Claim attributes (status, cause of loss, litigation flag, report lag)
- Claimant features (count, injury indicators, body part flags)
- Financial features (total paid, reserves, recoveries, incurred, cost breakdowns by type)
- Derived features (days to first payment, claim duration, legal/medical cost ratios, reserve adequacy ratio)

## Testing

Schema tests are defined at each layer covering:
- Primary key uniqueness and not-null constraints
- Accepted values for standardized fields (LOB codes, claim statuses, transaction types)
- Referential integrity between policies and claims

## Production Considerations

In a production environment, this project would benefit from additional dbt features:

- **Snapshots**: Implementing snapshots on claim status and reserve amounts would enable tracking how claims develop over time — critical for loss development analysis and reserve adequacy monitoring.
- **Macros**: As the number of customers grows beyond two, repeated mapping logic (LOB codes, status values, state formats) would be extracted into reusable macros to keep the codebase DRY and maintainable.
- **Incremental models**: The transaction and claims models would be materialized as incremental rather than full table rebuilds, improving performance as data volumes scale.

## How to Run

1. Clone this repo
2. Configure a BigQuery connection in your dbt profile
3. Load source CSVs into `raw_customer_a` and `raw_customer_b` datasets
4. `dbt seed` to load reference tables
5. `dbt run` to build all models
6. `dbt test` to validate
7. `dbt docs generate && dbt docs serve` to view documentation and lineage
