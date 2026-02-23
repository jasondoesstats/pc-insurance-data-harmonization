with customer_a as (

    select
        transaction_id,
        claim_id,
        transaction_type,
        cost_type,
        amount,
        transaction_date,
        check_number,
        source_customer

    from {{ ref('stg_customer_a__transactions') }}

),

customer_b as (

    select
        transaction_id,
        claim_id,
        case transaction_type
            when 'PMT' then 'Payment'
            when 'RSV' then 'Reserve'
            when 'REC' then 'Recovery'
            when 'RSV_CHG' then 'Reserve Change'
        end as transaction_type,
        case cost_type
            when 'IND' then 'Indemnity'
            when 'MED' then 'Medical'
            when 'EXP' then 'Expense'
            when 'DCC' then 'Legal'
            when 'AO' then 'Expense'
            when 'SUBRO' then 'Subrogation'
            when 'SALV' then 'Salvage'
            when 'DED' then 'Deductible'
        end as cost_type,
        amount,
        transaction_date,
        check_number,
        source_customer

    from {{ ref('stg_customer_b__transactions') }}

),

unioned as (

    select * from customer_a
    union all
    select * from customer_b

)

select * from unioned