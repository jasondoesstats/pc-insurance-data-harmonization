# Synthetic P&C Insurance Dataset — Multi-Customer Harmonization

## Overview

This dataset simulates data from **two fictional P&C insurance carriers** using Guidewire PolicyCenter and ClaimCenter, each with different naming conventions, coding systems, and data quality characteristics. It's designed to practice **data harmonization, profiling, feature engineering, and modeling dataset creation** — the core work of building unified analytics across multiple Guidewire customers.

## Structure

```
insurance_data/
├── customer_a/          # "Keystone Mutual" — PA-based regional carrier
│   ├── policies.csv     # 2,000 policies (WC, Commercial Auto, GL)
│   ├── claims.csv       # 800 claims
│   ├── claimants.csv    # 900 claimants
│   └── transactions.csv # 2,500 financial transactions
├── customer_b/          # "Great Lakes Insurance" — IL-based regional carrier
│   ├── policies.csv     # 1,500 policies (same lines, different conventions)
│   ├── claims.csv       # 650 claims
│   ├── claimants.csv    # 720 claimants
│   └── transactions.csv # 2,000 financial transactions
└── reference/           # Shared lookup/mapping tables
    ├── cause_of_loss.csv
    ├── states.csv
    └── wc_class_codes.csv
```

## Harmonization Challenges (by design)

These inconsistencies between Customer A and Customer B are intentional and mirror real-world Guidewire implementation differences:

| Dimension | Customer A (Keystone Mutual) | Customer B (Great Lakes Insurance) |
|---|---|---|
| **Column naming** | CamelCase (`PolicyNumber`, `ClaimStatus`) | snake_case (`pol_id`, `claim_status`) |
| **LOB codes** | Short codes: `WC`, `CA`, `GL` | Descriptive: `WorkersComp`, `CommAuto`, `GenLiab` |
| **State format** | Abbreviations: `PA`, `NJ` | Full names: `Illinois`, `Wisconsin` |
| **Claim status** | Descriptive: `Open`, `Closed`, `Reopened` | Single-letter: `O`, `C`, `R` |
| **Cause of loss** | Text descriptions | Numeric/alphanumeric codes (needs reference table) |
| **Gender** | `M` / `F` | `Male` / `Female` / `Unknown` |
| **Litigation flag** | `Y` / `N` | `1` / `0` |
| **Fraud/SIU flag** | `Y` / `N` | `True` / `False` |
| **Null handling** | Standard nulls | Mix of nulls, empty strings, and `99`/`Unknown` sentinels |
| **Injury descriptions** | Mixed case | ALL CAPS |
| **Body part codes** | Full names | Abbreviated + numeric codes |
| **Transaction types** | Descriptive: `Payment`, `Reserve` | Abbreviated: `PMT`, `RSV` |
| **Cost types** | Full names: `Indemnity`, `Medical` | Codes: `IND`, `MED`, `DCC`, `AO` |

## Data Quality Issues

- **Customer A**: Relatively clean. Nulls in `FraudIndicator`, `InjuryType`, `BodyPart`, `Gender`.
- **Customer B**: More data quality issues — nulls in `total_premium`, `producer_code`, `uw_id`, `adjuster`. Empty strings mixed with nulls in `injury_desc`, `body_part_code`, `check_num`. Sentinel value `99` in body part codes.

## Table Relationships

```
policies (1) ──→ (many) claims
claims   (1) ──→ (many) claimants
claims   (1) ──→ (many) transactions
```

Join keys:
- Customer A: `PolicyNumber` → `ClaimNumber`
- Customer B: `pol_id` → `claim_id`

## Lines of Business

- **Workers' Compensation (WC)**: ~40-45% of policies. NCCI class codes.
- **Commercial Auto (CA)**: ~30-35% of policies. Vehicle class codes.
- **General Liability (GL)**: ~20-30% of policies. GL class codes.

## Suggested Project Scope

1. **Stage** raw CSVs from both customers
2. **Profile** data quality and document findings
3. **Harmonize** into a unified schema (standardize column names, code values, null handling)
4. **Build features** for a claims severity model (e.g., claim duration, report lag, payment velocity, litigation rate by class code)
5. **Create** a final modeling-ready dataset joining policy, claim, claimant, and aggregated transaction data

## Date Ranges

- Policy effective dates: 2021–2024
- Claims and transactions: 2021–2024

## Production Considerations

In a production environment, this project would benefit from additional dbt features:

- **Snapshots**: Implementing snapshots on claim status and reserve amounts would enable tracking how claims develop over time — critical for loss development analysis and reserve adequacy monitoring.
- **Macros**: As the number of customers grows beyond two, repeated mapping logic (LOB codes, status values, state formats) would be extracted into reusable macros to keep the codebase DRY and maintainable.
- **Incremental models**: The transaction and claims models would be materialized as incremental rather than full table rebuilds, improving performance as data volumes scale.