with customer_a as (

    select
        claim_id,
        policy_id,
        case claim_status
            when 'Open' then 'O'
            when 'Closed' then 'C'
            when 'Reopened' then 'R'
        end as claim_status,
        loss_date,
        report_date,
        cause_of_loss as cause_of_loss_raw,
        loss_state as state,
        claimant_count,
        adjuster_id,
        litigation_flag as is_litigated,
        fraud_indicator as is_fraud_indicated,
        source_customer

    from {{ ref('stg_customer_a__claims') }}

),

customer_b as (

    select
        claim_id,
        policy_id,
        claim_status,
        loss_date,
        report_date,
        cause_of_loss as cause_of_loss_raw,
        s.state_abbrev as state,
        claimant_count,
        adjuster as adjuster_id,
        case litigation_flag
            when '1' then true
            when '0' then false
        end as is_litigated,
        case fraud_indicator
            when 'true' then true
            when 'false' then false
        end as is_fraud_indicated,
        p.source_customer

    from {{ ref('stg_customer_b__claims') }} p
    left join {{ ref('stg_states') }} s
        on p.loss_state = s.state_name

),

unioned as (

    select * from customer_a
    union all
    select * from customer_b

),

enriched as (

    select
        u.*,
        c.cause_description as cause_of_loss,
        date_diff(u.report_date, u.loss_date, day) as report_lag_days

    from unioned u
    left join {{ ref('stg_cause_of_loss') }} c
        on u.cause_of_loss_raw = c.cause_code

)

select * from enriched