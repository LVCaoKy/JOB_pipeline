{{ config(order_by='(so_phong_ngu,so_toilet)', engine='MergeTree()') }}
SELECT DISTINCT ON (so_phong_ngu,so_toilet,noi_that,huong_ban_cong)
    generateUUIDv4() AS In_id,  -- Tạo ID duy nhất
    so_phong_ngu,
    so_toilet,
    noi_that,
    huong_ban_cong
FROM {{ ref("Processed_Data") }}
