with customer_a as (

    select
        claimant_id,
        claim_id,
        claimant_type,
        injury_type,
        body_part,
        date_of_birth,
        case gender
            when 'M' then 'Male'
            when 'F' then 'Female'
        end as gender,
        source_customer

    from {{ ref('stg_customer_a__claimants') }}

),

customer_b as (

    select
        claimant_id,
        claim_id,
        case claimant_type
            when 'Injured Worker' then 'Employee'
            when '3rd Party' then 'Third Party'
            else claimant_type
        end as claimant_type,
        initcap(injury_type) as injury_type,
        case body_part
            when 'SHLD' then 'Shoulder'
            when 'ANKL' then 'Ankle'
            when 'MULT' then 'Multiple'
            else initcap(body_part)
        end as body_part,
        date_of_birth,
        case gender
            when 'Unknown' then null
            else gender
        end as gender,
        source_customer

    from {{ ref('stg_customer_b__claimants') }}

),

unioned as (

    select * from customer_a
    union all
    select * from customer_b

)

select * from unioned