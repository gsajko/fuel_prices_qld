{% snapshot stations_snapshot %}
    {{ config(
        unique_key = 'siteid',
        target_schema = 'snapshots',
        strategy = 'check',
        updated_at = 'transactiondateutc',
        check_cols = ['siteid', 'site_name', 'site_brand', 'sites_address_line_1', 'site_suburb', 'site_state', 'site_post_code', 'site_latitude', 'site_longitude'],
    ) }}

SELECT
    cast(siteid as integer) as  siteid,
    cast(site_name as string) as  site_name,
    cast(site_brand as string) as  site_brand, 
    -- this could be a dim table
    cast(sites_address_line_1 as string) as  sites_address_line_1,
    cast(site_suburb as string) as  site_suburb,
    -- this could be a dim table
    cast(site_state as string) as  site_state,
    -- this could be a dim table
    cast(site_post_code as integer) as  site_post_code,
    cast(site_latitude as numeric) as  site_latitude,
    cast(site_longitude as numeric) as  site_longitude,
    cast(transactiondateutc as timestamp) as transactiondateutc
FROM
    (
        SELECT
            *,
            ROW_NUMBER() over (
                PARTITION BY siteid
                ORDER BY
                    transactiondateutc DESC
            ) AS row_num
        FROM
            {{ source(
                "staging",
                "external_fuel_month"
            ) }}
    ) AS subq
WHERE
    row_num = 1

{% endsnapshot %}
