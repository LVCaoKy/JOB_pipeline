{{ config(order_by='City', engine='MergeTree()') }}
SELECT DISTINCT ON (City,District,Ward,Street)
    generateUUIDv4() AS Add_id,
    City,
    District,
    Street,
    Ward
FROM {{ ref("Processed_Data") }}
