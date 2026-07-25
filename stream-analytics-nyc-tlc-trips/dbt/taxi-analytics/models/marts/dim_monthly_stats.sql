{{
    config(
        materialized='table'
    )
}}

/*
    Data mart: Monthly aggregated statistics.
    Pre-aggregated for dashboard performance — feeds the monthly trends tile.
*/

with trips as (
    select * from {{ ref('fct_trips') }}
)

select
    pickup_year,
    pickup_month,
    format_date('%Y-%m', date(pickup_datetime))   as year_month,

    -- Volume metrics
    count(*)                                        as total_trips,
    sum(passenger_count)                            as total_passengers,

    -- Distance & duration
    round(avg(trip_distance_miles), 2)              as avg_trip_distance,
    round(avg(trip_duration_minutes), 2)            as avg_trip_duration,

    -- Revenue metrics
    round(sum(total_amount), 2)                     as total_revenue,
    round(avg(total_amount), 2)                     as avg_total_amount,
    round(avg(fare_amount), 2)                      as avg_fare,
    round(avg(tip_amount), 2)                       as avg_tip,
    round(avg(tip_percentage), 2)                   as avg_tip_percentage,

    -- Payment type breakdown
    countif(payment_type = 'Credit Card')           as credit_card_trips,
    countif(payment_type = 'Cash')                  as cash_trips,

from trips
group by 1, 2, 3
order by 1, 2