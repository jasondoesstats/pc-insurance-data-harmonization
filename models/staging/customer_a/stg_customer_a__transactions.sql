with source as (

    select * from {{ source('raw_customer_a', 'transactions') }}

),

renamed as (

    select
        TransactionID as transaction_id,
        ClaimNumber as claim_id,
        TransactionType as transaction_type,
        CostType as cost_type,
        cast(Amount as numeric) as amount,
        cast(TransactionDate as date) as transaction_date,
        CheckNumber as check_number,
        'customer_a' as source_customer

    from source

)

select * from renamed