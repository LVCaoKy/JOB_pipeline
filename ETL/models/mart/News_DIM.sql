SELECT 
    ROW_NUMBER() OVER () AS News_id,
    loai_tin
FROM {{ref("News_Info")}}