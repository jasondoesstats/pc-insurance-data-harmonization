with source as (

    select * from {{ source('raw_customer_a', 'claims') }}

),

renamed as (

    select
        ClaimNumber as claim_id,
        PolicyNumber as policy_id,
        ClaimStatus as claim_status,
        cast(LossDate as date) as loss_date,
        cast(ReportDate as date) as report_date,
        CauseOfLoss as cause_of_loss,
        LossState as loss_state,
        ClaimantCount as claimant_count,
        AdjusterID as adjuster_id,
        LitigationFlag as litigation_flag,
        FraudIndicator as fraud_indicator,
        'customer_a' as source_customer

    from source

)

select * from renamed