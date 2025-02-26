{{ config(order_by='nam', engine='MergeTree()') }}
SELECT DISTINCT ON(ngay_dang)
    ngay_dang,
    ngay,
    thang,
    nam
FROM {{ref("Processed_Data")}}