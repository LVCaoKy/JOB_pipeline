{{ config(order_by='loai_tin', engine='MergeTree()') }}
SELECT DISTINCT ON (loai_tin)
    generateUUIDv4() AS News_id,  -- Tạo ID duy nhất
    loai_tin
FROM {{ ref("Processed_Data") }}

