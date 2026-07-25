{{
    config(
        materialized='table',
        partition_by={
            "field": "pickup_datetime",
            "data_type": "timestamp",
            "granularity": "day"
        },
        cluster_by=["pickup_location_id", "payment_type"]
    )
}}

/*
    Fact table: Core trip-level fact table with enriched metrics.
    Partitioned by pickup_datetime (DAY) and clustered by
    pickup_location_id and payment_type for efficient dashboard queries.
*/

with trips as (
    select * from {{ ref('stg_yellow_taxi') }}
)

select
    -- Dimensions
    vendor_id,
    pickup_location_id,
    dropoff_location_id,
    payment_type_id,
    payment_type,
    rate_code_id,
    rate_code,
    passenger_count,

    -- Time dimensions (pre-computed for dashboard performance)
    pickup_datetime,
    dropoff_datetime,
    extract(YEAR from pickup_datetime)      as pickup_year,
    extract(MONTH from pickup_datetime)     as pickup_month,
    extract(DAY from pickup_datetime)       as pickup_day,
    extract(HOUR from pickup_datetime)      as pickup_hour,
    extract(DAYOFWEEK from pickup_datetime) as pickup_day_of_week,
    format_timestamp('%A', pickup_datetime) as pickup_day_name,

    -- Measures
    trip_distance_miles,
    trip_duration_minutes,
    fare_amount,
    tip_amount,
    tolls_amount,
    congestion_surcharge,
    total_amount,

    -- Derived metrics
    case
        when trip_distance_miles > 0
        then round(fare_amount / trip_distance_miles, 2)
        else 0
    end                                     as fare_per_mile,

    case
        when fare_amount > 0
        then round(tip_amount / fare_amount * 100, 2)
        else 0
    end                                     as tip_percentage,

    is_store_and_forward

from trips