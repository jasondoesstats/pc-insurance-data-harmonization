with claims as (

    select * from {{ ref('int_claims_unified') }}

),

policies as (

    select * from {{ ref('int_policies_unified') }}

),

claimants as (

    select
        claim_id,
        count(*) as claimant_count_actual,
        countif(injury_type is not null) as claimants_with_injury,
        countif(body_part = 'Back') as back_injury_count,
        countif(body_part = 'Head') as head_injury_count,
        min(date_of_birth) as oldest_claimant_dob,
        max(date_of_birth) as youngest_claimant_dob

    from {{ ref('int_claimants_unified') }}
    group by 1

),

transactions as (

    select
        claim_id,

        sum(case when transaction_type = 'Payment' then amount else 0 end) as total_paid,
        sum(case when transaction_type = 'Reserve' then amount else 0 end) as total_reserves,
        sum(case when transaction_type = 'Recovery' then amount else 0 end) as total_recoveries,

        sum(case when transaction_type in ('Payment', 'Reserve') then amount else 0 end)
            + sum(case when transaction_type = 'Recovery' then amount else 0 end) as total_incurred,

        sum(case when cost_type = 'Indemnity' and transaction_type = 'Payment' then amount else 0 end) as indemnity_paid,
        sum(case when cost_type = 'Medical' and transaction_type = 'Payment' then amount else 0 end) as medical_paid,
        sum(case when cost_type = 'Legal' and transaction_type = 'Payment' then amount else 0 end) as legal_paid,
        sum(case when cost_type = 'Expense' and transaction_type = 'Payment' then amount else 0 end) as expense_paid,

        count(case when transaction_type = 'Payment' then 1 end) as payment_count,
        count(case when transaction_type = 'Reserve Change' then 1 end) as reserve_change_count,

        min(case when transaction_type = 'Payment' then transaction_date end) as first_payment_date,
        max(transaction_date) as last_transaction_date

    from {{ ref('int_transactions_unified') }}
    group by 1

),

final as (

    select
        c.claim_id,
        c.policy_id,
        c.source_customer,

        -- policy attributes
        p.line_of_business,
        p.state,
        p.class_code,
        p.written_premium,
        p.renewal_count,
        p.policy_status,

        -- claim attributes
        c.claim_status,
        c.loss_date,
        c.report_date,
        c.report_lag_days,
        c.cause_of_loss,
        c.is_litigated,
        c.is_fraud_indicated,

        -- claimant features
        cl.claimant_count_actual,
        cl.claimants_with_injury,
        cl.back_injury_count,
        cl.head_injury_count,

        -- financial features
        t.total_paid,
        t.total_reserves,
        t.total_recoveries,
        t.total_incurred,
        t.indemnity_paid,
        t.medical_paid,
        t.legal_paid,
        t.expense_paid,
        t.payment_count,
        t.reserve_change_count,

        -- derived features
        date_diff(t.first_payment_date, c.loss_date, day) as days_to_first_payment,
        date_diff(t.last_transaction_date, c.loss_date, day) as claim_duration_days,

        case when t.total_paid > 0
            then t.legal_paid / t.total_paid
            else 0
        end as legal_cost_ratio,

        case when t.total_paid > 0
            then t.medical_paid / t.total_paid
            else 0
        end as medical_cost_ratio,

        case when t.total_reserves > 0
            then t.total_paid / t.total_reserves
            else 0
        end as reserve_adequacy_ratio

    from claims c
    left join policies p
        on c.policy_id = p.policy_id
        and c.source_customer = p.source_customer
    left join claimants cl
        on c.claim_id = cl.claim_id
    left join transactions t
        on c.claim_id = t.claim_id

)

select * from final