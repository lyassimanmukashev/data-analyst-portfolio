` ` `

SELECT
  year,
  month,
  calcul_1.mean_ride_length,
  calcul_1.max_ride_length,
  FORMAT(
    '%d Days, %t',
    DIV(calcul_1.mean_ride_length, 86400),
    TIME(TIMESTAMP_SECONDS(MOD(calcul_1.mean_ride_length, 86400)))
    ) AS  mean_ride_length_t,
  FORMAT(
    '%d Days, %t',
    DIV(calcul_1.max_ride_length, 86400),
    TIME(TIMESTAMP_SECONDS(MOD(calcul_1.max_ride_length, 86400)))
    ) AS  max_ride_length_t,
  FORMAT(
    '%d Days, %t',
    DIV(calcul_1.min_ride_length, 86400),
    TIME(TIMESTAMP_SECONDS(MOD(calcul_1.min_ride_length, 86400)))
    ) AS  min_ride_length_t,
FROM (
  SELECT
    EXTRACT(YEAR FROM started_at) AS year,
    EXTRACT(MONTH FROM started_at) AS month,
    CAST(ROUND(AVG(ride_length_sec)) AS INT64) as mean_ride_length,
    MAX(ride_length_sec) as max_ride_length,
    MIN(ride_length_sec) as min_ride_length,
    FROM `eminent-card-429217-h8.trip_data.all_trip_clean_v2`
    GROUP BY year,month
) as calcul_1

GROUP BY
  calcul_1.max_ride_length,
  calcul_1.mean_ride_length,
  calcul_1.min_ride_length,
  year,
  month

ORDER BY
  year,
  month
