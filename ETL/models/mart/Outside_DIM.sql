SELECT 
    ROW_NUMBER() OVER () AS Out_id,
    huong_nha,
    duong_vao,
    mat_tien,
    so_tang
FROM {{ref("House_Outside")}}