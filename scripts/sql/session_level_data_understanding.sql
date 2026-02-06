-- ============================================================
-- ✈️ TravelTide - Customer Segmentation Project
-- Notebook 02 - Data Understanding and Preparation (SQL Exploration)
-- Author: Maria Petralia
-- Context: MasterSchool - Data Science Program
-- Date: Feb 2026
--
-- Execution note:
-- This notebook runs exploratory SQL on large raw tables via Spark SQL/JDBC.
-- The sessions table contains ~5.4M records; execution may take ~10 minutes.
-- ============================================================


-- ============================================================
-- 3.3 Quick Data Preview
-- ============================================================

-- Count the number of records in Users, Sessions, Flights and Hotels
SELECT
  (SELECT COUNT(user_id) FROM users)   AS users_count,
  (SELECT COUNT(session_id) FROM sessions) AS sessions_count,
  (SELECT COUNT(trip_id) FROM flights) AS flights_count,
  (SELECT COUNT(trip_id) FROM hotels)  AS hotels_count;

-- Display the first 10 rows of sessions
SELECT * FROM sessions LIMIT 10;

-- Display the first 10 rows of users
SELECT * FROM users LIMIT 10;

-- Display the first 10 rows of flights
SELECT * FROM flights LIMIT 10;

-- Display the first 10 rows of hotels
SELECT * FROM hotels LIMIT 10;


-- ============================================================
-- 4. Data Consistency Checks
-- ============================================================

-- 4.1 Data Types
DESCRIBE users;
DESCRIBE sessions;
DESCRIBE flights;
DESCRIBE hotels;


-- 4.2 Primary Keys and Duplicates
-- Check duplicated on primary key for sessions
SELECT session_id, COUNT(*) AS duplicates
FROM sessions
GROUP BY session_id
HAVING COUNT(*) > 1;

-- Check duplicated on primary key for users
SELECT user_id, COUNT(*) AS duplicates
FROM users
GROUP BY user_id
HAVING COUNT(*) > 1;

-- Check duplicated on primary key for flights
SELECT trip_id, COUNT(*) AS duplicates
FROM flights
GROUP BY trip_id
HAVING COUNT(*) > 1;

-- Check duplicated on primary key for hotels
SELECT trip_id, COUNT(*) AS duplicates
FROM hotels
GROUP BY trip_id
HAVING COUNT(*) > 1;


-- 4.3 Null Values and Basic Statistics
-- Sessions: check null values on sessions columns
SELECT
  COUNT(CASE WHEN session_id IS NULL THEN 1 END) AS session_id_nulls,
  COUNT(CASE WHEN trip_id IS NULL THEN 1 END) AS trip_id_nulls,
  COUNT(CASE WHEN session_start IS NULL THEN 1 END) AS session_start_nulls,
  COUNT(CASE WHEN session_end IS NULL THEN 1 END) AS session_end_nulls,
  COUNT(CASE WHEN page_clicks IS NULL THEN 1 END) AS page_clicks_nulls,
  COUNT(CASE WHEN flight_discount IS NULL THEN 1 END) AS flight_discount_nulls,
  COUNT(CASE WHEN flight_discount_amount IS NULL THEN 1 END) AS flight_discount_amount_nulls,
  COUNT(CASE WHEN flight_booked IS NULL THEN 1 END) AS flight_booked_nulls,
  COUNT(CASE WHEN hotel_discount IS NULL THEN 1 END) AS hotel_discount_nulls,
  COUNT(CASE WHEN hotel_discount_amount IS NULL THEN 1 END) AS hotel_discount_amount_nulls,
  COUNT(CASE WHEN hotel_booked IS NULL THEN 1 END) AS hotel_booked_nulls,
  COUNT(CASE WHEN cancellation IS NULL THEN 1 END) AS cancellation_nulls
FROM sessions;

-- Users: check null values on users columns
SELECT
  COUNT(CASE WHEN user_id IS NULL THEN 1 END) AS user_id_nulls,
  COUNT(CASE WHEN birthdate IS NULL THEN 1 END) AS birthdate_nulls,
  COUNT(CASE WHEN gender IS NULL THEN 1 END) AS gender_nulls,
  COUNT(CASE WHEN married IS NULL THEN 1 END) AS married_nulls,
  COUNT(CASE WHEN has_children IS NULL THEN 1 END) AS has_children_nulls,
  COUNT(CASE WHEN home_country IS NULL THEN 1 END) AS home_country_nulls,
  COUNT(CASE WHEN home_city IS NULL THEN 1 END) AS home_city_nulls,
  COUNT(CASE WHEN home_airport IS NULL THEN 1 END) AS home_airport_nulls,
  COUNT(CASE WHEN home_airport_lat IS NULL THEN 1 END) AS home_airport_lat_nulls,
  COUNT(CASE WHEN home_airport_lon IS NULL THEN 1 END) AS home_airport_lon_nulls,
  COUNT(CASE WHEN sign_up_date IS NULL THEN 1 END) AS sign_up_date_nulls
FROM users;

-- Flights: check null values on flights columns
SELECT
  COUNT(CASE WHEN trip_id IS NULL THEN 1 END) AS trip_id_nulls,
  COUNT(CASE WHEN origin_airport IS NULL THEN 1 END) AS origin_airport_nulls,
  COUNT(CASE WHEN destination IS NULL THEN 1 END) AS destination_nulls,
  COUNT(CASE WHEN destination_airport IS NULL THEN 1 END) AS destination_airport_nulls,
  COUNT(CASE WHEN seats IS NULL THEN 1 END) AS seats_nulls,
  COUNT(CASE WHEN return_flight_booked IS NULL THEN 1 END) AS return_flight_booked_nulls,
  COUNT(CASE WHEN departure_time IS NULL THEN 1 END) AS departure_time_nulls,
  COUNT(CASE WHEN return_time IS NULL THEN 1 END) AS return_time_nulls,
  COUNT(CASE WHEN checked_bags IS NULL THEN 1 END) AS checked_bags_nulls,
  COUNT(CASE WHEN trip_airline IS NULL THEN 1 END) AS trip_airline_nulls,
  COUNT(CASE WHEN destination_airport_lat IS NULL THEN 1 END) AS destination_airport_lat_nulls,
  COUNT(CASE WHEN destination_airport_lon IS NULL THEN 1 END) AS destination_airport_lon_nulls,
  COUNT(CASE WHEN base_fare_usd IS NULL THEN 1 END) AS base_fare_usd_nulls
FROM flights;

-- Hotels: check null values on hotel columns
SELECT
  COUNT(CASE WHEN trip_id IS NULL THEN 1 END) AS trip_id_nulls,
  COUNT(CASE WHEN hotel_name IS NULL THEN 1 END) AS hotel_name_nulls,
  COUNT(CASE WHEN nights IS NULL THEN 1 END) AS nights_nulls,
  COUNT(CASE WHEN rooms IS NULL THEN 1 END) AS rooms_nulls,
  COUNT(CASE WHEN check_in_time IS NULL THEN 1 END) AS check_in_time_nulls,
  COUNT(CASE WHEN check_out_time IS NULL THEN 1 END) AS check_out_time_nulls,
  COUNT(CASE WHEN hotel_per_room_usd IS NULL THEN 1 END) AS hotel_per_room_usd_nulls
FROM hotels;


-- ============================================================
-- 5. Exploratory Analysis (SQL)
-- ============================================================

-- 5.1 Sessions Table Exploration
-- Total sessions per user (upper tail)
SELECT user_id, COUNT(*) AS tot_sessions
FROM sessions
GROUP BY user_id
ORDER BY tot_sessions DESC
LIMIT 20;

-- Cancellation distribution in sessions
SELECT cancellation, COUNT(*) AS num_sessions
FROM sessions
GROUP BY cancellation;

-- Distinguishing Booking, Browsing, and Cancellation Sessions
-- Booking sessions: cancellation=false AND (flight_booked OR hotel_booked)
-- Browsing: cancellation=false AND no booking flags
-- Cancellation sessions: cancellation=true
SELECT
  SUM(CASE WHEN (cancellation = false) AND (flight_booked=true OR hotel_booked=true) THEN 1 ELSE 0 END) AS booking_session,
  SUM(CASE WHEN (cancellation = false) AND flight_booked=false AND hotel_booked=false THEN 1 ELSE 0 END) AS browsing_sessions,
  SUM(CASE WHEN (cancellation = true) THEN 1 ELSE 0 END) AS cancellation_sessions
FROM sessions;

-- Trip vs Browsing-Only Sessions (trip_id NULL vs NOT NULL)
SELECT
  CASE WHEN trip_id IS NULL THEN 'no_trip' ELSE 'with_trip' END AS trip_flag,
  COUNT(*) AS num_sessions
FROM sessions
GROUP BY trip_flag;

-- Booking–Cancellation Relationship
-- Test whether cancellation sessions are linked to prior booking sessions via trip_id.
WITH cancelled_trips AS (
  SELECT DISTINCT trip_id
  FROM sessions
  WHERE cancellation = true
    AND trip_id IS NOT NULL
)
SELECT
  COUNT(DISTINCT s.trip_id) AS booking_and_cancelled_trips,
  SUM(CASE WHEN s.cancellation = false THEN 1 ELSE 0 END) AS booking_sessions_count,
  SUM(CASE WHEN s.cancellation = true THEN 1 ELSE 0 END) AS cancellation_sessions_count
FROM sessions s
JOIN cancelled_trips ct
  ON s.trip_id = ct.trip_id;

-- Flight/Hotel booking flags in Booking Sessions (cancellation=false)
SELECT
  flight_booked,
  hotel_booked,
  COUNT(*) AS num_sessions
FROM sessions
WHERE cancellation = false
GROUP BY flight_booked, hotel_booked
ORDER BY flight_booked DESC, hotel_booked DESC;

-- Check for cancellation sessions with incomplete bookings
-- NOTE: In your notebook, this returned 0 rows and you concluded that
-- cancellation sessions have both flight_booked and hotel_booked = TRUE.
SELECT *
FROM sessions
WHERE cancellation = true
  AND (hotel_booked = false OR flight_booked = false);

-- Session Details for Trip Shared across Multiple Sessions
WITH trips_multiple_sessions AS (
  SELECT trip_id
  FROM sessions
  WHERE trip_id IS NOT NULL
  GROUP BY trip_id
  HAVING COUNT(*) > 1
)
SELECT s.*
FROM sessions s
JOIN trips_multiple_sessions tms
  ON s.trip_id = tms.trip_id
ORDER BY s.trip_id, s.session_start
LIMIT 10;

-- Session Duration Distribution (bucketed)
SELECT
  CASE
    WHEN session_length_minutes BETWEEN 0 AND 5 THEN '0-5 min'
    WHEN session_length_minutes BETWEEN 6 AND 10 THEN '6-10 min'
    WHEN session_length_minutes BETWEEN 11 AND 30 THEN '11-30 min'
    WHEN session_length_minutes BETWEEN 31 AND 60 THEN '31-60 min'
    ELSE '>60 min'
  END AS session_length_bucket,
  COUNT(*) AS num_sessions
FROM (
  SELECT
    CAST((CAST(session_end AS TIMESTAMP) - CAST(session_start AS TIMESTAMP)) AS BIGINT) / 60 AS session_length_minutes
  FROM sessions
) AS sub
GROUP BY session_length_bucket
ORDER BY
  CASE
    WHEN session_length_bucket = '0-5 min' THEN 1
    WHEN session_length_bucket = '6-10 min' THEN 2
    WHEN session_length_bucket = '11-30 min' THEN 3
    WHEN session_length_bucket = '31-60 min' THEN 4
    ELSE 5
  END;

-- Count page clicks in session
SELECT page_clicks, COUNT(*) AS count
FROM sessions
GROUP BY page_clicks
ORDER BY page_clicks;

-- Discount Offered Flags (cancellation=false)
SELECT
  flight_discount,
  hotel_discount,
  COUNT(*) AS num_sessions
FROM sessions
WHERE cancellation = false
GROUP BY flight_discount, hotel_discount
ORDER BY flight_discount DESC, hotel_discount DESC;

-- Sessions + booking/cancellation rates by gender
SELECT
  u.gender,
  COUNT(*) AS total_sessions,
  SUM(
    CASE
      WHEN s.cancellation = FALSE
       AND (s.flight_booked = TRUE OR s.hotel_booked = TRUE)
      THEN 1 ELSE 0
    END
  ) AS booking_sessions,
  SUM(
    CASE WHEN s.cancellation = TRUE THEN 1 ELSE 0 END
  ) AS cancellation_sessions,
  -- booking rate: bookings / total sessions
  ROUND(
    SUM(
      CASE
        WHEN s.cancellation = FALSE
         AND (s.flight_booked = TRUE OR s.hotel_booked = TRUE)
        THEN 1 ELSE 0
      END
    ) * 1.0 / COUNT(*),
    3
  ) AS booking_rate,
  -- cancellation rate: cancellations / booking sessions
  ROUND(
    SUM(
      CASE WHEN s.cancellation = TRUE THEN 1 ELSE 0 END
    ) * 1.0 /
    NULLIF(
      SUM(
        CASE
          WHEN s.cancellation = FALSE
           AND (s.flight_booked = TRUE OR s.hotel_booked = TRUE)
          THEN 1 ELSE 0
        END
      ),
      0
    ),
    3
  ) AS cancellation_rate
FROM sessions s
JOIN users u
  ON s.user_id = u.user_id
GROUP BY u.gender
ORDER BY total_sessions DESC;

-- First/last session date and users by gender
SELECT
  gender,
  MIN(session_start) AS first_session_date,
  MAX(session_start) AS last_session_date,
  COUNT(DISTINCT s.user_id) AS n_users
FROM sessions s
JOIN users u ON s.user_id = u.user_id
GROUP BY gender
ORDER BY gender;


-- 5.2 Users Table Exploration
-- Distribution by gender, marital status, and children
SELECT gender, married, has_children, COUNT(user_id) AS tot_users
FROM users
GROUP BY gender, married, has_children;

-- Age at travel distribution (users + sessions + flights)
SELECT
  YEAR(u.birthdate) AS birthyear,
  FLOOR(datediff(f.departure_time, u.birthdate) / 365) AS age_at_travel,
  COUNT(DISTINCT u.user_id) AS tot_users
FROM users u
JOIN sessions s ON s.user_id = u.user_id
JOIN flights f ON f.trip_id = s.trip_id
GROUP BY
  YEAR(u.birthdate),
  FLOOR(datediff(f.departure_time, u.birthdate) / 365)
ORDER BY birthyear DESC;

-- Users by home_country
SELECT home_country, COUNT(user_id) AS tot_users
FROM users
GROUP BY home_country;


-- 5.3 Flights and Hotels Exploration
-- Most used airlines in the last 6 months
SELECT trip_airline, COUNT(trip_id) AS tot_used
FROM flights
WHERE departure_time > (SELECT MAX(departure_time) - INTERVAL '6 months' FROM flights)
GROUP BY trip_airline
ORDER BY tot_used DESC
LIMIT 30;

-- Average seats by airline
SELECT trip_airline, AVG(seats) AS average_seats
FROM flights
GROUP BY trip_airline
ORDER BY average_seats DESC;

-- Hotel name distribution
SELECT hotel_name, COUNT(*) AS tot_hotel_names
FROM hotels
GROUP BY hotel_name
ORDER BY tot_hotel_names DESC;

-- Annual Flight Counts Based on Departure Year
SELECT
  EXTRACT(YEAR FROM f.departure_time) AS year,
  COUNT(*) AS num_flights
FROM flights f
WHERE f.departure_time IS NOT NULL
GROUP BY 1
ORDER BY 1;

-- Nights Distribution
SELECT
  nights,
  COUNT(*) AS num_sessions
FROM hotels
GROUP BY nights
ORDER BY nights;

-- Annual Hotel Counts Based on Check-in Year
SELECT
  EXTRACT(YEAR FROM h.check_in_time) AS year,
  COUNT(*) AS num_stays
FROM hotels h
WHERE h.check_in_time IS NOT NULL
GROUP BY 1
ORDER BY 1;


-- ============================================================
-- 5.4 Cohort Sensitivity Analysis (User-Level)
-- ============================================================

-- 5.4.1 Baseline cohort: sessions > 7 after 2023-01-04
WITH filtered_sessions AS (
  SELECT
    user_id,
    trip_id,
    cancellation,
    flight_booked,
    hotel_booked
  FROM sessions
  WHERE session_start > '2023-01-04'
),
eligible_users AS (
  SELECT user_id
  FROM filtered_sessions
  GROUP BY user_id
  HAVING COUNT(*) > 7
),
cohort_sessions AS (
  SELECT fs.*
  FROM filtered_sessions fs
  JOIN eligible_users eu ON eu.user_id = fs.user_id
),
sessions_by_user AS (
  SELECT
    user_id,
    COUNT(*) AS total_sessions,
    COUNT(*) FILTER (WHERE trip_id IS NULL) AS browsing_sessions,
    COUNT(*) FILTER (
      WHERE trip_id IS NOT NULL AND cancellation = FALSE
      AND (flight_booked = TRUE OR hotel_booked = TRUE)
    ) AS booking_sessions,
    COUNT(*) FILTER (WHERE cancellation = TRUE) AS cancellation_sessions
  FROM cohort_sessions
  GROUP BY user_id
),
users_with_gender AS (
  SELECT
    sbu.*,
    u.gender
  FROM sessions_by_user sbu
  JOIN users u ON u.user_id = sbu.user_id
)
SELECT
  gender,
  COUNT(*) AS n_users,
  SUM(browsing_sessions) AS n_browsings,
  SUM(booking_sessions) AS n_bookings,
  SUM(cancellation_sessions) AS n_cancels,
  SUM(total_sessions) AS n_sessions,
  ROUND(SUM(booking_sessions)::numeric / NULLIF(SUM(total_sessions), 0), 3) AS booking_rate,
  ROUND(SUM(cancellation_sessions)::numeric / NULLIF(SUM(booking_sessions), 0), 3) AS cancel_rate
FROM users_with_gender
GROUP BY gender
ORDER BY n_users DESC;


-- 5.4.2 Lower threshold cohort: sessions > 5 after 2023-01-04
WITH filtered_sessions AS (
  SELECT
    user_id,
    trip_id,
    cancellation,
    flight_booked,
    hotel_booked
  FROM sessions
  WHERE session_start > '2023-01-04'
),
eligible_users AS (
  SELECT user_id
  FROM filtered_sessions
  GROUP BY user_id
  HAVING COUNT(*) > 5
),
cohort_sessions AS (
  SELECT fs.*
  FROM filtered_sessions fs
  JOIN eligible_users eu ON eu.user_id = fs.user_id
),
sessions_by_user AS (
  SELECT
    user_id,
    COUNT(*) AS total_sessions,
    COUNT(*) FILTER (WHERE trip_id IS NULL) AS browsing_sessions,
    COUNT(*) FILTER (
      WHERE trip_id IS NOT NULL AND cancellation = FALSE
      AND (flight_booked = TRUE OR hotel_booked = TRUE)
    ) AS booking_sessions,
    COUNT(*) FILTER (WHERE cancellation = TRUE) AS cancellation_sessions
  FROM cohort_sessions
  GROUP BY user_id
),
users_with_gender AS (
  SELECT
    sbu.*,
    u.gender
  FROM sessions_by_user sbu
  JOIN users u ON u.user_id = sbu.user_id
)
SELECT
  gender,
  COUNT(*) AS n_users,
  SUM(browsing_sessions) AS n_browsings,
  SUM(booking_sessions) AS n_bookings,
  SUM(cancellation_sessions) AS n_cancels,
  SUM(total_sessions) AS n_sessions,
  ROUND(SUM(booking_sessions)::numeric / NULLIF(SUM(total_sessions), 0), 3) AS booking_rate,
  ROUND(SUM(cancellation_sessions)::numeric / NULLIF(SUM(booking_sessions), 0), 3) AS cancel_rate
FROM users_with_gender
GROUP BY gender
ORDER BY n_users DESC;


-- Cohort definition: sessions > 3 after 2023-01-04
WITH filtered_sessions AS (
  SELECT
    user_id,
    trip_id,
    cancellation,
    flight_booked,
    hotel_booked
  FROM sessions
  WHERE session_start > '2023-01-04'
),
eligible_users AS (
  SELECT user_id
  FROM filtered_sessions
  GROUP BY user_id
  HAVING COUNT(*) > 3
),
cohort_sessions AS (
  SELECT fs.*
  FROM filtered_sessions fs
  JOIN eligible_users eu ON eu.user_id = fs.user_id
),
sessions_by_user AS (
  SELECT
    user_id,
    COUNT(*) AS total_sessions,
    COUNT(*) FILTER (WHERE trip_id IS NULL) AS browsing_sessions,
    COUNT(*) FILTER (
      WHERE trip_id IS NOT NULL AND cancellation = FALSE
      AND (flight_booked = TRUE OR hotel_booked = TRUE)
    ) AS booking_sessions,
    COUNT(*) FILTER (WHERE cancellation = TRUE) AS cancellation_sessions
  FROM cohort_sessions
  GROUP BY user_id
),
users_with_gender AS (
  SELECT
    sbu.*,
    u.gender
  FROM sessions_by_user sbu
  JOIN users u ON u.user_id = sbu.user_id
)
SELECT
  gender,
  COUNT(*) AS n_users,
  SUM(browsing_sessions) AS n_browsings,
  SUM(booking_sessions) AS n_bookings,
  SUM(cancellation_sessions) AS n_cancels,
  SUM(total_sessions) AS n_sessions,
  ROUND(SUM(booking_sessions)::numeric / NULLIF(SUM(total_sessions), 0), 3) AS booking_rate,
  ROUND(SUM(cancellation_sessions)::numeric / NULLIF(SUM(booking_sessions), 0), 3) AS cancel_rate
FROM users_with_gender
GROUP BY gender
ORDER BY n_users DESC;


-- ============================================================
-- 7. Final Session-Level Extraction Query (used in Spark SQL)
-- ============================================================
-- Purpose:
-- Build the consolidated session-level dataset used in downstream notebooks.
-- Applies:
--  - recency filter (sessions after 2023-01-04),
--  - minimum activity threshold (>7 sessions per user),
--  - temporal consistency rule: reintroduce pre-cutoff booking sessions
--    for trips cancelled after cutoff (complete lifecycle).
-- ============================================================

WITH
  -- 1. Sessions after cutoff date
  filtered_sessions AS (
    SELECT *
    FROM sessions
    WHERE session_start > '2023-01-04'
  ),

  -- 2. Users with sufficient activity
  filtered_users AS (
    SELECT
      user_id,
      COUNT(*) AS session_count
    FROM filtered_sessions
    GROUP BY user_id
    HAVING COUNT(*) > 7
  ),

  -- 3. Session-level table for filtered users (enriched with users + products)
  session_level AS (
    SELECT
      -- SESSION FIELDS
      s.session_id,
      s.user_id,
      s.trip_id,
      s.session_start,
      s.session_end,
      s.page_clicks,
      s.flight_discount,
      s.flight_discount_amount,
      s.flight_booked,
      s.hotel_discount,
      s.hotel_discount_amount,
      s.hotel_booked,
      s.cancellation,

      -- USER FIELDS
      u.birthdate,
      u.gender,
      u.married,
      u.has_children,
      u.home_country,
      u.home_city,
      u.home_airport,
      u.home_airport_lat,
      u.home_airport_lon,
      u.sign_up_date,

      -- FLIGHT FIELDS
      f.origin_airport,
      f.destination,
      f.destination_airport,
      f.seats,
      f.return_flight_booked,
      f.departure_time,
      f.return_time,
      f.checked_bags,
      f.trip_airline,
      f.destination_airport_lat,
      f.destination_airport_lon,
      f.base_fare_usd,

      -- HOTEL FIELDS
      h.hotel_name,
      h.nights,
      h.rooms,
      h.check_in_time,
      h.check_out_time,
      h.hotel_per_room_usd

    FROM filtered_sessions s
    INNER JOIN users u ON s.user_id = u.user_id
    LEFT JOIN flights f ON s.trip_id = f.trip_id
    LEFT JOIN hotels h ON s.trip_id = h.trip_id
    WHERE s.user_id IN (SELECT user_id FROM filtered_users)
  ),

  -- 4. Cancellation sessions inside the window
  cancellations AS (
    SELECT *
    FROM session_level
    WHERE cancellation = TRUE
  ),

  -- 5. Booking sessions BEFORE cutoff for trips cancelled AFTER cutoff
  sessions_trip_booked_before AS (
    SELECT *
    FROM sessions
    WHERE trip_id IN (SELECT trip_id FROM cancellations)
      AND session_start <= '2023-01-04'
  ),

  -- 6. Build enriched rows for those earlier booking sessions
  session_level_before AS (
    SELECT
      -- SESSION FIELDS
      s.session_id,
      s.user_id,
      s.trip_id,
      s.session_start,
      s.session_end,
      s.page_clicks,
      s.flight_discount,
      s.flight_discount_amount,
      s.flight_booked,
      s.hotel_discount,
      s.hotel_discount_amount,
      s.hotel_booked,
      s.cancellation,

      -- USER FIELDS
      u.birthdate,
      u.gender,
      u.married,
      u.has_children,
      u.home_country,
      u.home_city,
      u.home_airport,
      u.home_airport_lat,
      u.home_airport_lon,
      u.sign_up_date,

      -- FLIGHT FIELDS
      f.origin_airport,
      f.destination,
      f.destination_airport,
      f.seats,
      f.return_flight_booked,
      f.departure_time,
      f.return_time,
      f.checked_bags,
      f.trip_airline,
      f.destination_airport_lat,
      f.destination_airport_lon,
      f.base_fare_usd,

      -- HOTEL FIELDS
      h.hotel_name,
      h.nights,
      h.rooms,
      h.check_in_time,
      h.check_out_time,
      h.hotel_per_room_usd

    FROM sessions_trip_booked_before s
    INNER JOIN users u ON s.user_id = u.user_id
    LEFT JOIN flights f ON s.trip_id = f.trip_id
    LEFT JOIN hotels h ON s.trip_id = h.trip_id
    WHERE s.user_id IN (SELECT user_id FROM filtered_users)
  ),

  -- 7. Combine both parts
  final_tab AS (
    SELECT * FROM session_level
    UNION ALL
    SELECT * FROM session_level_before
  )

SELECT *
FROM final_tab;
