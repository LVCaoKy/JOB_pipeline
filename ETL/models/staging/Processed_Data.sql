SELECT
    loai_tin,
    ma_tin,
    huong_ban_cong,
    huong_nha,
    multiIf(
        muc_gia IS NULL OR muc_gia = '', NULL,
        positionCaseInsensitive(muc_gia, 'triệu/m²') > 0, 
            toFloat64OrZero(replaceAll(REGEXP_REPLACE(toString(muc_gia), '[^0-9,]', ''), ',', '.')) 
            * toFloat64OrZero(replaceAll(REGEXP_REPLACE(toString(dien_tich), '[^0-9,]', ''), ',', '.')) 
            * 1000000,
        positionCaseInsensitive(muc_gia, 'tỷ') > 0, 
            toFloat64OrZero(replaceAll(REGEXP_REPLACE(toString(muc_gia), '[^0-9,]', ''), ',', '.')) 
            * 1000000000,
        positionCaseInsensitive(muc_gia, 'triệu') > 0, 
            toFloat64OrZero(replaceAll(REGEXP_REPLACE(toString(muc_gia), '[^0-9,]', ''), ',', '.')) 
            * 1000000,
        NULL
    ) AS muc_gia,
    arrayElement(splitByString(',', trim(BOTH ' ' FROM toString(address))), -1) AS City,
    arrayElement(splitByString(',', trim(BOTH ' ' FROM toString(address))), -2) AS District,
    arrayElement(splitByString(',', trim(BOTH ' ' FROM toString(address))), -3) AS Ward,
    multiIf(
    length(splitByString(',', trim(toString(address)))) >= 4 
        AND like(splitByString(',', trim(toString(address)))[length(splitByString(',', trim(toString(address)))) - 3], '%Dự án%'), NULL,
    arrayElement(splitByString(',', trim(toString(address))), length(splitByString(',', trim(toString(address)))) - 3)
) AS Street,
    multiIf(
    length(splitByString(',', trim(toString(address)))) >= 4 
        AND like(splitByString(',', trim(toString(address)))[length(splitByString(',', trim(toString(address)))) - 3], '%Dự án%'),
    arrayElement(splitByString(',', trim(toString(address))), length(splitByString(',', trim(toString(address)))) - 3),
    arrayElement(splitByString(',', trim(toString(address))), length(splitByString(',', trim(toString(address)))) - 4)
) AS More,
    multiIf(
        positionCaseInsensitive(noi_that, 'bản') > 0 OR positionCaseInsensitive(noi_that, 'ntcb') > 0, 'Cơ bản',
        positionCaseInsensitive(noi_that, 'đủ') > 0 OR positionCaseInsensitive(noi_that, 'cấp') > 0, 'Đầy đủ',
        noi_that IS NULL OR TRIM(noi_that) = '', 'Không',
        'Đặc biệt'
    ) AS noi_that,
    toFloat64OrZero(replaceAll(REGEXP_REPLACE(toString(dien_tich), '[^0-9,]', ''), ',', '.')) AS dien_tich,
    toInt32OrZero(REGEXP_REPLACE(toString(so_phong_ngu), '\\D+', '')) AS so_phong_ngu, 
    toInt32OrZero(REGEXP_REPLACE(toString(so_tang), '\\D+', '')) AS so_tang, 
    toInt32OrZero(REGEXP_REPLACE(toString(so_toilet), '\\D+', '')) AS so_toilet,
    toFloat64OrZero(REGEXP_REPLACE(toString(duong_vao), '\\D+', '')) AS duong_vao, 
    toFloat64OrZero(REGEXP_REPLACE(toString(mat_tien), '\\D+', '')) AS mat_tien,
    multiIf(
        positionCaseInsensitive(phap_ly, 'sổ') > 0, 'có sổ',
        positionCaseInsensitive(phap_ly, 'Hợp') > 0 OR positionCaseInsensitive(phap_ly, 'HĐMB') > 0, 'hợp đồng',
        'không có'
    ) AS phap_ly,
    ngay_dang,
    toMonth(ngay_dang) as thang,
    toYear(ngay_dang) as nam,
    toDayOfMonth(ngay_dang) as ngay
FROM {{source("clickhouse","batdongsan_merged")}}