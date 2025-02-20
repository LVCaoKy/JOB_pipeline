SELECT
    
    -- Xprocess phap_ly
    CASE 
        WHEN phap_ly LIKE '%sổ%' THEN 'có sổ'
        WHEN phap_ly LIKE '%Hợp%' OR phap_ly LIKE '%HĐMB%' THEN 'hợp đồng'
        ELSE 'không có'
    END AS phap_ly,

    loai_tin,

    -- process muc_gia
    CASE
        WHEN muc_gia IS NULL OR muc_gia = '' THEN NULL
        WHEN muc_gia LIKE '%triệu/m²%' THEN 
            COALESCE(NULLIF(replace(regexp_replace(muc_gia, '[^0-9,]', '', 'g'), ',', '.'), ''), '0')::DOUBLE PRECISION 
            * COALESCE(NULLIF(replace(regexp_replace(dien_tich, '[^0-9,]', '', 'g'), ',', '.'), ''), '0')::DOUBLE PRECISION 
            * 1_000_000
        WHEN muc_gia LIKE '%tỷ%' THEN 
            COALESCE(NULLIF(replace(regexp_replace(muc_gia, '[^0-9,]', '', 'g'), ',', '.'), ''), '0')::DOUBLE PRECISION 
            * 1_000_000_000
        WHEN muc_gia LIKE '%triệu%' THEN 
            COALESCE(NULLIF(replace(regexp_replace(muc_gia, '[^0-9,]', '', 'g'), ',', '.'), ''), '0')::DOUBLE PRECISION 
            * 1_000_000
        ELSE NULL
    END AS muc_gia,

    -- process dien_tich
    CASE
        WHEN dien_tich IS NULL OR dien_tich = '' THEN NULL
        ELSE 
            COALESCE(NULLIF(replace(regexp_replace(dien_tich, '[^0-9,]', '', 'g'), ',', '.'), ''), '0')::DOUBLE PRECISION
    END AS dien_tich,

    -- process address
    (string_to_array(TRIM(address), ','))[array_length(string_to_array(TRIM(address), ','), 1)] AS City,
    (string_to_array(TRIM(address), ','))[array_length(string_to_array(TRIM(address), ','), 1) - 1] AS District,
    COALESCE((string_to_array(TRIM(address), ','))[array_length(string_to_array(TRIM(address), ','), 1) - 2], NULL) AS Ward,
    CASE 
        WHEN array_length(string_to_array(TRIM(address), ','), 1) >= 4 
            AND (string_to_array(TRIM(address), ','))[array_length(string_to_array(TRIM(address), ','), 1) - 3] LIKE '%Dự án%' 
        THEN NULL
        ELSE COALESCE((string_to_array(TRIM(address), ','))[array_length(string_to_array(TRIM(address), ','), 1) - 3], NULL)
    END AS Street,
    CASE 
        WHEN array_length(string_to_array(TRIM(address), ','), 1) >= 4 
            AND (string_to_array(TRIM(address), ','))[array_length(string_to_array(TRIM(address), ','), 1) - 3] LIKE '%Dự án%' 
        THEN (string_to_array(TRIM(address), ','))[array_length(string_to_array(TRIM(address), ','), 1) - 3]
        ELSE COALESCE((string_to_array(TRIM(address), ','))[array_length(string_to_array(TRIM(address), ','), 1) - 4], NULL)
    END AS More,
    huong_nha,
    COALESCE(NULLIF(regexp_replace(so_phong_ngu, '\D+', '', 'g'), ''), '0')::INTEGER AS so_phong_ngu,
    COALESCE(NULLIF(regexp_replace(so_tang, '\D+', '', 'g'), ''), '0')::INTEGER AS so_tang,
    COALESCE(NULLIF(regexp_replace(so_toilet, '\D+', '', 'g'), ''), '0')::INTEGER AS so_toilet,
    ngay_dang,
    CASE
        WHEN ngay_dang ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN SPLIT_PART(ngay_dang, '/', 1)
        ELSE NULL
    END AS ngay,
    CASE
        WHEN ngay_dang ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN SPLIT_PART(ngay_dang, '/', 2)
        ELSE NULL
    END AS thang,
    CASE
        WHEN ngay_dang ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN SPLIT_PART(ngay_dang, '/', 3)
        ELSE NULL
    END AS nam,
    COALESCE(NULLIF(regexp_replace(duong_vao, '\D+', '', 'g'), ''), '0') AS duong_vao,
    COALESCE(NULLIF(regexp_replace(mat_tien, '\D+', '', 'g'), ''), '0') AS mat_tien,
    huong_ban_cong,
    -- process noi_that
    CASE 
        WHEN LOWER(noi_that) LIKE '%bản%' OR LOWER(noi_that) LIKE '%ntcb%' THEN 'Cơ bản'
        WHEN LOWER(noi_that) LIKE '%đủ%' OR LOWER(noi_that) LIKE '%cấp%' THEN 'Đầy đủ'
        WHEN noi_that IS NULL OR TRIM(noi_that) = '' THEN 'Không'
        ELSE 'Đặc biệt'
    END AS noi_that
FROM {{source("RAW","HOUSE_HCM")}}
