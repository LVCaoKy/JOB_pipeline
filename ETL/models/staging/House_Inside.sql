SELECT DISTINCT
    so_phong_ngu,
    so_toilet,
    noi_that,
    huong_ban_cong
FROM {{ref("Processed_Data")}}