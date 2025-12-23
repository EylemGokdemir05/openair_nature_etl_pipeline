SELECT
    TIMESTAMP_SECONDS(CAST(list_item.dt AS INT64)) as measurement_time,
    CAST(list_item.main.aqi AS INT64) as air_quality_index,
    CAST(list_item.components.pm2_5 AS FLOAT64) as pm2_5,
    CAST(list_item.components.pm10 AS FLOAT64) as pm10,
    CAST(list_item.components.no2 AS FLOAT64) as no2,
    coord.lat as latitude,
    coord.lon as longitude
FROM 
    `openair-nature-pipeline.openair_nature_data.taunus_air_quality`,
    UNNEST(list) as list_item