{{ config(order_by='More', engine='MergeTree()') }}
SELECT DISTINCT ON (phap_ly, More)
    generateUUIDv4() AS Info_id,  -- Tạo ID duy nhất
    phap_ly,
    More
FROM {{ ref("Processed_Data") }}

