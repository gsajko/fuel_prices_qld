{{ config(
    materialized = 'view'
) }}

SELECT
    cast(siteid as integer) as siteid,
    cast(fuel_type as string) as fuel_type,
    cast(price as numeric) as price,
    cast(transactiondateutc as timestamp) as transactiondateutc
FROM
    {{ source(
        "staging",
        "external_fuel_month"
    ) }}
LIMIT
    10