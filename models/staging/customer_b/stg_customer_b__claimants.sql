with source as (

    select * from {{ source('raw_customer_b', 'claimants') }}

),

renamed as (

    select
        clmt_id as claimant_id,
        claim_id,
        claimant_role as claimant_type,
        nullif(injury_desc, '') as injury_type,
        nullif(nullif(body_part_code, ''), '99') as body_part,
        cast(dob as date) as date_of_birth,
        sex as gender,
        'customer_b' as source_customer

    from source

)

select * from renamed