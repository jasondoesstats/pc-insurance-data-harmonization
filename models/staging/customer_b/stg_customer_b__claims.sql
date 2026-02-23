with source as (

    select * from {{ source('raw_customer_b', 'claims') }}

),

renamed as (

    select
        claim_id,
        pol_id as policy_id,
        claim_status,
        cast(date_of_loss as date) as loss_date,
        cast(reported_date as date) as report_date,
        cause_code as cause_of_loss,
        loss_jurisdiction as loss_state,
        num_claimants as claimant_count,
        adjuster,
        cast(litigated as string) as litigation_flag,
        cast(siu_referral as string) as fraud_indicator,
        'customer_b' as source_customer

    from source

)

select * from renamed