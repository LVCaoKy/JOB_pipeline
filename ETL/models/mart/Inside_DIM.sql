SELECT
    ROW_NUMBER() OVER () AS In_id,
    so_phong_ngu,
    so_toilet,
    noi_that,
    huong_ban_cong
FROM {{ref("House_Inside")}}