CREATE TABLE default.batdongsan_merged
(
    `loai_tin` String,
    `ma_tin` Int64,
    `link` String,
    `dien_tich` String,
    `muc_gia` String,
    `huong_nha` String,
    `so_tang` String,
    `so_phong_ngu` String,
    `so_toilet` String,
    `phap_ly` String,
    `noi_that` String,
    `title` String,
    `address` String,
    `duong_vao` String,
    `mat_tien` String,
    `ngay_dang` DateTime,
    `ngay_het_han` DateTime,
    `huong_ban_cong` String
)
ENGINE = SharedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
ORDER BY tuple()
SETTINGS index_granularity = 8192