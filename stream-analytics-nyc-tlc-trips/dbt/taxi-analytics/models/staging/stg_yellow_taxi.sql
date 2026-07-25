{{
    config(
        materialized='view'
    )
}}

/*
    Staging model: Clean and standardize raw yellow taxi data.
    - Filter out invalid trips (zero distance, negative fares)
    - Cast data types
    - Add human-readable payment type labels
*/

with raw_trips as (
    select * from {{ source('raw', 'yellow_taxi_raw') }}
),

cleaned as (
    select
        -- IDs
        VendorID                                          as vendor_id,
        cast(PULocationID as INT64)                       as pickup_location_id,
        cast(DOLocationID as INT64)                       as dropoff_location_id,

        -- Timestamps
        tpep_pickup_datetime                              as pickup_datetime,
        tpep_dropoff_datetime                             as dropoff_datetime,

        -- Trip info
        cast(passenger_count as INT64)                    as passenger_count,
        round(trip_distance, 2)                           as trip_distance_miles,

        -- Duration in minutes
        round(
            timestamp_diff(tpep_dropoff_datetime, tpep_pickup_datetime, SECOND) / 60.0,
            2
        )                                                 as trip_duration_minutes,

        -- Fare breakdown
        round(fare_amount, 2)                             as fare_amount,
        round(tip_amount, 2)                              as tip_amount,
        round(tolls_amount, 2)                            as tolls_amount,
        round(total_amount, 2)                            as total_amount,
        round(congestion_surcharge, 2)                    as congestion_surcharge,

        -- Payment type label
        cast(payment_type as INT64)                       as payment_type_id,
        case cast(payment_type as INT64)
            when 1 then 'Credit Card'
            when 2 then 'Cash'
            when 3 then 'No Charge'
            when 4 then 'Dispute'
            when 5 then 'Unknown'
            when 6 then 'Voided'
            else 'Other'
        end                                               as payment_type,

        -- Rate code
        cast(RatecodeID as INT64)                         as rate_code_id,
        case cast(RatecodeID as INT64)
            when 1 then 'Standard'
            when 2 then 'JFK'
            when 3 then 'Newark'
            when 4 then 'Nassau/Westchester'
            when 5 then 'Negotiated'
            when 6 then 'Group Ride'
            else 'Unknown'
        end                                               as rate_code,

        -- Store and forward
        case store_and_fwd_flag
            when 'Y' then true
            when 'N' then false
            else null
        end                                               as is_store_and_forward

    from raw_trips
    where
        -- Filter invalid trips
        trip_distance > {{ var('min_trip_distance') }}
        and trip_distance < {{ var('max_trip_distance') }}
        and fare_amount >= {{ var('min_fare') }}
        and fare_amount <= {{ var('max_fare') }}
        and tpep_pickup_datetime is not null
        and tpep_dropoff_datetime is not null
        -- Ensure pickup is before dropoff
        and tpep_pickup_datetime < tpep_dropoff_datetime
        -- Filter out unreasonably long trips (> 12 hours)
        and timestamp_diff(tpep_dropoff_datetime, tpep_pickup_datetime, HOUR) < 12
        -- Only keep 2024 data (filter out dirty records with wrong years)
        and tpep_pickup_datetime >= '2026-01-01'
        and tpep_pickup_datetime < '2026-02-01'
)

select * from cleaned