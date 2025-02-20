SELECT
    ROW_NUMBER() OVER () AS Add_id,
    City,
    District,
    Street,
    Ward,
    More
FROM {{ref("House_Address")}}