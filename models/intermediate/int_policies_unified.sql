with customer_a as (

    select
        policy_id,
        line_of_business,
        policy_status,
        effective_date,
        expiration_date,
        state_code as state,
        class_code,
        written_premium,
        insured_name,
        agency_code,
        underwriter_id,
        renewal_count,
        source_customer

    from {{ ref('stg_customer_a__policies') }}

),

customer_b as (

    select
        policy_id,

        case line_of_business
            when 'WorkersComp' then 'WC'
            when 'CommAuto' then 'CA'
            when 'GenLiab' then 'GL'
        end as line_of_business,

        case policy_status
            when 'Active' then 'In Force'
            when 'Non-Renewed' then 'Expired'
            else policy_status
        end as policy_status,

        effective_date,
        expiration_date,

        s.state_abbrev as state,

        class_code,
        written_premium,
        insured_name,
        agency_code,
        underwriter_id,
        renewal_count,
        source_customer

    from {{ ref('stg_customer_b__policies') }} p
    left join {{ ref('stg_states') }} s
        on p.state_name = s.state_name

),

unioned as (

    select * from customer_a
    union all
    select * from customer_b

)

select * from unioned