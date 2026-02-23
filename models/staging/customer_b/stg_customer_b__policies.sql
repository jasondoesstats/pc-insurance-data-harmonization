with source as (

    select * from {{ source('raw_customer_b', 'policies') }}

),

renamed as (

    select
        pol_id as policy_id,
        line_of_business,
        status as policy_status,
        cast(eff_dt as date) as effective_date,
        cast(exp_dt as date) as expiration_date,
        jurisdiction as state_name,
        class_cd as class_code,
        cast(total_premium as numeric) as written_premium,
        named_insured as insured_name,
        producer_code as agency_code,
        uw_id as underwriter_id,
        term_number as renewal_count,
        'customer_b' as source_customer

    from source

)

select * from renamed