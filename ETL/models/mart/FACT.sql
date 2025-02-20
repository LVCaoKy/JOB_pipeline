SELECT
    f.muc_gia,
    f.dien_tich,
    a.Add_id,
    d.ngay_dang,
    info.Info_id,
    inside.In_id,
    n.News_id,
    outside.Out_id
FROM {{ref("Processed_Data")}} as f
LEFT JOIN {{ref("Address_DIM")}} as a ON f.City IS NOT DISTINCT FROM a.City AND f.District IS NOT DISTINCT FROM a.District AND f.Street IS NOT DISTINCT FROM a.Street AND f.More IS NOT DISTINCT FROM a.More AND f.Ward IS NOT DISTINCT FROM a.Ward
LEFT JOIN {{ref("Date_DIM")}} as d ON f.ngay_dang IS NOT DISTINCT FROM d.ngay_dang
LEFT JOIN {{ref("Info_DIM")}} as info ON f.phap_ly IS NOT DISTINCT FROM info.phap_ly
LEFT JOIN {{ref("Inside_DIM")}} as inside ON f.so_phong_ngu IS NOT DISTINCT FROM inside.so_phong_ngu AND f.so_toilet IS NOT DISTINCT FROM inside.so_toilet AND f.noi_that IS NOT DISTINCT FROM inside.noi_that AND f.huong_ban_cong IS NOT DISTINCT FROM inside.huong_ban_cong
LEFT JOIN {{ref("News_DIM")}} as n ON f.loai_tin IS NOT DISTINCT FROM n.loai_tin
LEFT JOIN {{ref("Outside_DIM")}} as outside ON f.huong_nha IS NOT DISTINCT FROM outside.huong_nha AND f.duong_vao IS NOT DISTINCT FROM outside.duong_vao AND f.mat_tien IS NOT DISTINCT FROM outside.mat_tien AND f.so_tang IS NOT DISTINCT FROM outside.so_tang