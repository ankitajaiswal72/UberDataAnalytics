# Write the code in Redshift QueryEditorV2

# Joining Tables on FactTable
CREATE TABLE ubermpl.uberanalytics AS
SELECT 
    f.trip_id,
    d.START_DATE,
    d.start_hour,
    d.start_day,
    d.start_month,
    d.start_year,
    d.start_weekday,
    d.END_DATE,
    d.end_hour,
    d.end_day,
    d.end_month,
    d.end_year,
    d.end_weekday,
    c.CATEGORY,
    sl.START as start_location,
    stp.STOP as stop_location,
    m.MILES,
    p.PURPOSE
FROM 
    ubermpl.fact_table f
JOIN 
    ubermpl.datetime_dim d ON f.datetime_id = d.datetime_id
JOIN 
    ubermpl.category_dim c ON f.category_id = c.category_id
JOIN 
    ubermpl.start_location_dim sl ON f.start_location_id = sl.start_location_id
JOIN 
    ubermpl.stop_location_dim stp ON f.stop_location_id = stp.stop_location_id
JOIN 
    ubermpl.miles_dim m ON f.miles_id = m.miles_id
JOIN 
    ubermpl.purpose_dim p ON f.purpose_id = p.purpose_id;

SELECT
    *
FROM
    "uberank"."public"."uberdata";



# SQL OPERATIONS
  
/*Monthly Trip Count and Average Miles*/

create table ubermpl.avgcount as
SELECT 
    d.start_year,
    d.start_month,
    COUNT(*) as trip_count,
    AVG(m.MILES) as avg_miles
FROM 
    ubermpl.fact_table f
JOIN 
    ubermpl.datetime_dim d ON f.datetime_id = d.datetime_id
JOIN 
    ubermpl.miles_dim m ON f.miles_id = m.miles_id
GROUP BY 
    d.start_year, d.start_month
ORDER BY 
    d.start_year, d.start_month;

/Top 5 Most Common Trip Purposes/
SELECT 
    p.PURPOSE,
    COUNT(*) as trip_count
FROM 
    ubermpl.fact_table f
JOIN 
    ubermpl.purpose_dim p ON f.purpose_id = p.purpose_id
GROUP BY 
    p.PURPOSE
ORDER BY 
    trip_count DESC
LIMIT 5;


/*Most Popular Start Locations by Day of Week*/

create table ubermpl.popular as
SELECT 
    d.start_weekday,
    sl.START as start_location,
    COUNT(*) as trip_count
FROM 
    ubermpl.fact_table f
JOIN 
    ubermpl.datetime_dim d ON f.datetime_id = d.datetime_id
JOIN 
    ubermpl.start_location_dim sl ON f.start_location_id = sl.start_location_id
GROUP BY 
    d.start_weekday, sl.START
QUALIFY 
    ROW_NUMBER() OVER (PARTITION BY d.start_weekday ORDER BY COUNT(*) DESC) = 1
ORDER BY 
    d.start_weekday;


/*Trips Longer Than Average By Category*/

create table ubermpl.long as
WITH avg_miles_by_category AS (
    SELECT 
        c.CATEGORY,
        AVG(m.MILES) as avg_miles
    FROM 
        ubermpl.fact_table f
    JOIN 
        ubermpl.category_dim c ON f.category_id = c.category_id
    JOIN 
        ubermpl.miles_dim m ON f.miles_id = m.miles_id
    GROUP BY 
        c.CATEGORY
)
SELECT 
    f.trip_id,
    c.CATEGORY,
    m.MILES,
    amc.avg_miles as category_avg_miles
FROM 
    ubermpl.fact_table f
JOIN 
    ubermpl.category_dim c ON f.category_id = c.category_id
JOIN 
    ubermpl.miles_dim m ON f.miles_id = m.miles_id
JOIN 
    avg_miles_by_category amc ON c.CATEGORY = amc.CATEGORY
WHERE 
    m.MILES > amc.avg_miles
ORDER BY 
    c.CATEGORY, m.MILES DESC;
