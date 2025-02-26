{{ config(order_by='(so_tang,mat_tien)', engine='MergeTree()') }}
SELECT DISTINCT ON (huong_nha,so_tang,mat_tien)
    generateUUIDv4() AS Out_id,  -- Tạo ID duy nhất
    huong_nha,
    duong_vao,
    mat_tien,
    so_tang
FROM {{ ref("Processed_Data") }}


