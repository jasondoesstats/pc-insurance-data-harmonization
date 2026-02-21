with source as (

    select * from {{ source('raw_customer_a', 'policies') }}

),

renamed as (

    select
        PolicyNumber as policy_id,
        LineOfBusiness as line_of_business,
        PolicyStatus as policy_status,
        cast(EffectiveDate as date) as effective_date,
        cast(ExpirationDate as date) as expiration_date,
        State as state_code,
        ClassCode as class_code,
        cast(WrittenPremium as numeric) as written_premium,
        InsuredName as insured_name,
        AgencyCode as agency_code,
        UnderwriterID as underwriter_id,
        RenewalCount as renewal_count,
        'customer_a' as source_customer

    from source

)

select * from renamed