SELECT DISTINCT
    huong_nha,
    duong_vao,
    mat_tien,
    so_tang
FROM {{ref("Processed_Data")}}