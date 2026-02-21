with source as (

    select * from {{ source('raw_customer_a', 'claimants') }}

),

renamed as (

    select
        ClaimantID as claimant_id,
        ClaimNumber as claim_id,
        ClaimantType as claimant_type,
        InjuryType as injury_type,
        BodyPart as body_part,
        cast(DateOfBirth as date) as date_of_birth,
        Gender as gender,
        'customer_a' as source_customer

    from source

)

select * from renamed