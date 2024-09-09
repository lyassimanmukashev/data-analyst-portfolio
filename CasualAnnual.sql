/*
  Data Analysis of Cyclistic's Annual Members and Casual Riders

  Using BigQuery
*/

-- Unite and Combine data from November 2023 to April 2024.

CREATE OR REPLACE TABLE `eminent-card-429217-h8.trip_data.all_trip` AS
SELECT *
FROM  `eminent-card-429217-h8.trip_data.trip_data_nov_2023`
UNION ALL
SELECT *
FROM `eminent-card-429217-h8.trip_data.trip_data_dec_2023`
UNION ALL
SELECT *
FROM `eminent-card-429217-h8.trip_data.trip_data_jan_2024`
UNION ALL
SELECT *
FROM `eminent-card-429217-h8.trip_data.trip_data_feb_2024`
UNION ALL
SELECT *
FROM `eminent-card-429217-h8.trip_data.trip_data_mar_2024`
UNION ALL
SELECT *
FROM `eminent-card-429217-h8.trip_data.trip_data_apr_2024`


-- Preparing Data for analysis

  WITH `trip_durations` AS
 (
  SELECT *,
  DATE_DIFF(ended_at,started_at,second) AS ride_length_sec
  FROM `eminent-card-429217-h8.trip_data.all_trip`
)
  
SELECT
  DISTINCT (trip_durations.ride_id),
  trip_durations.rideable_type,
  trip_durations.member_casual,
  trip_durations.started_at,
  `trip_durations`.ended_at,
  EXTRACT(DAYOFWEEK from started_at) as day_of_week,
  ride_length_sec,
  FORMAT(
    '%d Days, %t',
    DIV(ride_length_sec, 86400),
    TIME(TIMESTAMP_SECONDS(MOD(ride_length_sec, 86400)))
    ) AS ride_length
FROM `trip_durations`

-- Using functions like DISTINCT and WHERE clauses to correct and check data’s integrity

WHERE
  length(trip_durations.ride_id) = 16
  AND trip_durations.ride_length_sec > -1
  AND (trip_durations.member_casual = 'casual' OR trip_durations.member_casual = 'member')

ORDER BY
  trip_durations.ride_length_sec

-- Simple calculations, like mean, max, and min to find general knowledge about the data. Also, group them by year and month

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

-- Note: Small code that can be implemented in any part of the code. Using a more precise period (month)

WHERE
  (EXTRACT(MONTH FROM started_at) = 11 AND EXTRACT(YEAR FROM started_at) = 2023)
  OR
  (EXTRACT(MONTH FROM ended_at) = 11 AND EXTRACT(YEAR FROM ended_at) = 2023)

-- Or

EXTRACT(YEAR FROM started_at) AS year,
EXTRACT(MONTH FROM started_at) AS month

-- Number of rides per day of the week, and the mode of this

SELECT
  day_of_week,
  COUNT(day_of_week) AS number_of_rides
FROM
  `eminent-card-429217-h8.trip_data.all_trip_clean_v2`
GROUP BY
  day_of_week
ORDER BY
  number_of_rides DESC

LIMIT 1

-- Average Ride Length (by months and member/casual)

SELECT  
  year,
  month,
  FORMAT(
    '%t',
    TIME(TIMESTAMP_SECONDS(MOD(trip_data.avg_ride_length, 86400)))
    ) AS  avg_ride_length_t,
  member_casual,
  trip_data.avg_ride_length
FROM (
  SELECT
  EXTRACT(YEAR FROM started_at) AS year,
  EXTRACT(MONTH FROM started_at) AS month,
  member_casual,
  CAST(ROUND(AVG(ride_length_sec)) AS INT64) as avg_ride_length
  FROM `eminent-card-429217-h8.trip_data.all_trip_clean_v2`
  GROUP BY member_casual, year, month
) as trip_data

GROUP BY
  trip_data.member_casual,
  year,month,
  trip_data.avg_ride_length

ORDER BY
  year, month

-- Average Ride Length (by day of the week & casual/member)
  SELECT 
  trip_data.member_casual,
  trip_data.day_of_week,
  avg_ride_length,
  FORMAT(
    '%d Days, %t',
    DIV(trip_data.avg_ride_length, 86400),
    TIME(TIMESTAMP_SECONDS(MOD(trip_data.avg_ride_length, 86400)))
    ) AS  avg_ride_length_t,
FROM (
  SELECT
  member_casual,
  day_of_week, 
  CAST(ROUND(AVG(ride_length_sec)) AS INT64) as avg_ride_length
  FROM `eminent-card-429217-h8.trip_data.all_trip_clean_v2`
  GROUP BY 
  member_casual, 
  day_of_week
) as trip_data
  
GROUP BY 
  trip_data.member_casual, 
  trip_data.day_of_week,
  trip_data.avg_ride_length
ORDER BY 
  trip_data.member_casual, 
  trip_data.day_of_week;

-- Average Length (just by day of the week)
SELECT 
  trip_data.day_of_week,
  avg_ride_length,
  FORMAT(
    '%d Days, %t',
    DIV(trip_data.avg_ride_length, 86400),
    TIME(TIMESTAMP_SECONDS(MOD(trip_data.avg_ride_length, 86400)))
    ) AS  avg_ride_length_t,
FROM (
  SELECT
  day_of_week, 
  CAST(ROUND(AVG(ride_length_sec)) AS INT64) as avg_ride_length
  FROM `eminent-card-429217-h8.trip_data.all_trip_clean_v2`
  GROUP BY  
  day_of_week
) as trip_data
  
GROUP BY 
  trip_data.day_of_week,
  trip_data.avg_ride_length
ORDER BY  
  trip_data.day_of_week;

-- Number of Rides (Members/Casual & Day of the week)
SELECT
  member_casual, 
  day_of_week, 
  COUNT(ride_id) AS ride_count
FROM 
  `eminent-card-429217-h8.trip_data.all_trip_clean_v2`
GROUP BY 
  member_casual, 
  day_of_week
ORDER BY 
  member_casual, 
  day_of_week;

-- Number of Rides Overall (Members/Casual)
SELECT
  member_casual,  
  COUNT(ride_id) AS ride_count
FROM
  `eminent-card-429217-h8.trip_data.all_trip_clean_v2`
GROUP BY
  member_casual
ORDER BY
  member_casual

