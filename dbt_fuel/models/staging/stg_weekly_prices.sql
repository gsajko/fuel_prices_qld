{{ config(
    materialized = 'view'
) }}

SELECT
    cast(siteid as integer) as siteid,
    cast(fuelid as integer) as fuelid,
    cast(price as numeric) as price,
    cast(transactiondateutc as timestamp) as transactiondateutc
FROM
    {{ source(
        "staging",
        "external_fuel_week"
    ) }}
LIMIT
    10
