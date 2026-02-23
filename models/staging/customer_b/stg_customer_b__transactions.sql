with source as (

    select * from {{ source('raw_customer_b', 'transactions') }}

),

renamed as (

    select
        txn_id as transaction_id,
        claim_id,
        txn_type as transaction_type,
        cost_category as cost_type,
        cast(txn_amount as numeric) as amount,
        cast(txn_dt as date) as transaction_date,
        cast(check_num as string) as check_number,
        'customer_b' as source_customer

    from source

)

select * from renamed