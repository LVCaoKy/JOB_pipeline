SELECT DISTINCT
    City,
    District,
    Street,
    Ward,
    More
FROM {{ref("Processed_Data")}}