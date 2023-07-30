SELECT t.region, t.datasource AS latest_data_source
FROM trips.trips_data t
JOIN (
    SELECT region, COUNT(*) AS region_count
    FROM trips.trips_data
    GROUP BY region
    ORDER BY COUNT(*) DESC
    LIMIT 2
) t2 ON t.region = t2.region
JOIN (
    SELECT region, MAX(datetime) AS max_datetime
    FROM trips.trips_data
    GROUP BY region
) t3 ON t.region = t3.region AND t.datetime = t3.max_datetime;