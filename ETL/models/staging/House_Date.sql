Select DISTINCT
    ngay_dang,
    ngay,
    thang,
    nam
FROM {{ref("Processed_Data")}}