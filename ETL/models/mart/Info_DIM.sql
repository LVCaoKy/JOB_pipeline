SELECT
    ROW_NUMBER() OVER () AS Info_id,
    phap_ly
FROM {{ref("House_Info")}}