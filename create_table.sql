
CREATE TABLE HOUSE (
    id SERIAL PRIMARY KEY,
    loai_tin VARCHAR(255),
    ma_tin BIGINT UNIQUE,
    link TEXT,
    dien_tich VARCHAR(50),  -- Needs conversion to NUMERIC if storing as number
    muc_gia VARCHAR(50),    -- Needs conversion to NUMERIC
    huong_nha VARCHAR(50),
    so_tang VARCHAR(50),    -- Needs conversion if storing as INTEGER
    so_phong_ngu VARCHAR(50),
    so_toilet INT,
    phap_ly VARCHAR(255),
    noi_that VARCHAR(255),
    title TEXT,
    address TEXT,
    duong_vao VARCHAR(50),
    mat_tien VARCHAR(50),
    check_status VARCHAR(50),
    ngay_dang DATE,
    ngay_het_han DATE,
    so_phong_tam VARCHAR(50),
    huong_ban_cong VARCHAR(50)
);
