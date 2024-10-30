with 

source as (

    select * from {{ source('raw', 'sales') }}

),

renamed as (

    select
        CONCAT(orders_id,"_",pdt_id) AS sales_key,
        date_date,
        orders_id,
        pdt_id AS products_id,
        revenue,
        quantity

    from source

)

select * from renamed
