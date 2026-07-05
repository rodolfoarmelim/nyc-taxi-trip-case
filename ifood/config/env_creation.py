from pyspark.sql import SparkSession

# Creating Spark Session
spark = SparkSession.builder.getOrCreate()

# Creating Catalog Structures
print("Initiating Catalog Creation")
## Catalog: nyc_taxi
spark.sql("CREATE CATALOG IF NOT EXISTS nyc_taxi COMMENT 'Catalog resposible storing all the data related to yellow and green taxi trips Data Product'")
print("Catalog nyc_taxi successfully created!")

## Initiating Landing Zone Structure Creation
print("Initiating Landing Zone Structure Creation")

## Schema: Landing
spark.sql("CREATE SCHEMA IF NOT EXISTS nyc_taxi.landing COMMENT 'Schema to absorb the landing zone volume'")
print("Schema landing successfully created!")

### Volume: Landing Zone
spark.sql("CREATE VOLUME IF NOT EXISTS nyc_taxi.landing.landing_zone COMMENT 'Volume to store all files related to nyc trip data'")
print("Volume landing_zone successfully created!")

### Schema: Bronze
## Initiating Bronze Schema Creation
print("Initiating Bronze Schema Creation")

spark.sql("CREATE SCHEMA IF NOT EXISTS nyc_taxi.bronze COMMENT 'Bronze layer on the medallion architecture for the nyc taxi data'")
print("Schema bronze successfully created!")

### Table: tb_bronze_yellow_taxi_trips
print("Initiating Bronze table tb_bronze_yellow_taxi_trips creation")
query_yellow = """
CREATE TABLE IF NOT EXISTS nyc_taxi.bronze.tb_bronze_yellow_taxi_trips (
    VendorID INT COMMENT 'A code indicating the TPEP provider that provided the record.
\n 1 = Creative Mobile Technologies, LLC
\n 2 = Curb Mobility, LLC
\n 6 = Myle Technologies Inc
\n 7 = Helix
',
    tpep_pickup_datetime TIMESTAMP COMMENT 'The date and time when the meter was engaged.',
    tpep_dropoff_datetime TIMESTAMP COMMENT 'The date and time when the meter was disengaged. ',
    passenger_count INT COMMENT 'The number of passengers in the vehicle. ',
    trip_distance DOUBLE COMMENT 'The elapsed trip distance in miles reported by the taximeter',
    RatecodeID STRING COMMENT 'The final rate code in effect at the end of the trip.
\n 1 = Standard rate
\n 2 = JFK
\n 3 = Newark
\n 4 = Nassau or Westchester
\n 5 = Negotiated fare
\n 6 = Group ride
\n 99 = Null/unknown
',
    store_and_fwd_flag STRING COMMENT 'This flag indicates whether the trip record was held in vehicle memory before
sending to the vendor, aka “store and forward,” because the vehicle did not
have a connection to the server.
\n Y = store and forward trip
\n N = not a store and forward trip',
    PULocationID INT COMMENT 'TLC Taxi Zone in which the taximeter was engaged.',
    DOLocationID INT COMMENT 'TLC Taxi Zone in which the taximeter was disengaged.',
    payment_type INT COMMENT 'A numeric code signifying how the passenger paid for the trip.
\n 0 = Flex Fare trip
\n 1 = Credit card
\n 2 = Cash
\n 3 = No charge
\n 4 = Dispute
\n 5 = Unknown
\n 6 = Voided trip',
    fare_amount DOUBLE COMMENT 'The time-and-distance fare calculated by the meter. For additional
information on the following columns, see
https://www.nyc.gov/site/tlc/passengers/taxi-fare.page',
    extra DOUBLE COMMENT 'Miscellaneous extras and surcharges.',
    mta_tax DOUBLE COMMENT 'Tax that is automatically triggered based on the metered rate in use.',
    tip_amount DOUBLE COMMENT 'Tip amount – This field is automatically populated for credit card tips. Cash
tips are not included.',
    tolls_amount DOUBLE COMMENT 'Total amount of all tolls paid in trip.',
    improvement_surcharge DOUBLE COMMENT 'Improvement surcharge assessed trips at the flag drop. The improvement
surcharge began being levied in 2015.',
    total_amount DOUBLE COMMENT 'The total amount charged to passengers. Does not include cash tips',
    congestion_surcharge DOUBLE COMMENT 'Total amount collected in trip for NYS congestion surcharge',
    airport_fee DOUBLE COMMENT 'For pick up only at LaGuardia and John F. Kennedy Airports.',
    dt_ingestion TIMESTAMP COMMENT 'Datetime of the data ingestion',
    year_month_file STRING COMMENT 'Year and month on YYYY-MM format of the file the data as collected. Used to partition the data'
)
USING DELTA
COMMENT 'Table that store raw data of yellow taxi trips from files stored in landing.landing_zone volume. The archives have standard names of yellow_tripdata_YYYY-MM.parquet, using the year-month reference of the file to create a year_month_file column to partion the data.'
PARTITIONED BY (year_month_file);
"""

spark.sql(query_yellow)
print("Table tb_bronze_yellow_taxi_trips successfully created.\n")

### Table: tb_bronze_green_taxi_trips
print("Initiating Bronze table tb_bronze_green_taxi_trips creation")
query_green = """
CREATE TABLE IF NOT EXISTS nyc_taxi.bronze.tb_bronze_green_taxi_trips (
    VendorID INT COMMENT 'A code indicating the LPEP provider that provided the record.
\n 1 = Creative Mobile Technologies, LLC
\n 2 = Curb Mobility, LLC
\n 6 = Myle Technologies Inc
',
    lpep_pickup_datetime TIMESTAMP COMMENT 'The date and time when the meter was engaged.',
    lpep_dropoff_datetime TIMESTAMP COMMENT 'The date and time when the meter was disengaged. ',
    store_and_fwd_flag STRING COMMENT 'This flag indicates whether the trip record was held in vehicle memory before
sending to the vendor, aka “store and forward,” because the vehicle did not
have a connection to the server.',
    RatecodeID STRING COMMENT 'The final rate code in effect at the end of the trip.
\n 1 = Standard rate
\n 2 = JFK
\n 3 = Newark
\n 4 = Nassau or Westchester
\n 5 = Negotiated fare
\n 6 = Group ride
\n 99 = Null/unknown
',
    PULocationID INT COMMENT 'TLC Taxi Zone in which the taximeter was engaged.',
    DOLocationID INT COMMENT 'TLC Taxi Zone in which the taximeter was disengaged.',
    passenger_count INT COMMENT 'The number of passengers in the vehicle. ',
    trip_distance DOUBLE COMMENT 'The elapsed trip distance in miles reported by the taximeter',
    fare_amount DOUBLE COMMENT 'The time-and-distance fare calculated by the meter. For additional
information on the following columns, see
https://www.nyc.gov/site/tlc/passengers/taxi-fare.page',
    extra DOUBLE COMMENT 'Miscellaneous extras and surcharges.',
    mta_tax DOUBLE COMMENT 'Tax that is automatically triggered based on the metered rate in use.',
    tip_amount DOUBLE COMMENT 'Tip amount – This field is automatically populated for credit card tips. Cash
tips are not included.',
    tolls_amount DOUBLE COMMENT 'Total amount of all tolls paid in trip.',
    ehail_fee DOUBLE COMMENT 'ehail fee',
    improvement_surcharge DOUBLE COMMENT 'Improvement surcharge assessed trips at the flag drop. The improvement
surcharge began being levied in 2015.',
    total_amount DOUBLE COMMENT 'The total amount charged to passengers. Does not include cash tips',
    payment_type INT COMMENT 'A numeric code signifying how the passenger paid for the trip.',
    trip_type INT COMMENT 'A code indicating whether the trip was a street-hail or a dispatch that is
automatically assigned based on the metered rate in use but can be altered by the driver.
\n 1 = Street-hail
\n 2 = Dispatch
',
    congestion_surcharge DOUBLE COMMENT 'Total amount collected in trip for NYS congestion surcharge',
    dt_ingestion TIMESTAMP COMMENT 'Datetime of the data ingestion',
    year_month_file STRING COMMENT 'Year and month on YYYY-MM format of the file the data as collected. Used to partition the data'
)
USING DELTA
COMMENT 'Table that store raw data of green taxi trips from files stored in landing.landing_zone volume. The archives have standard names of green_tripdata_YYYY-MM.parquet, using the year-month reference of the file to create a year_month_file column to partion the data'
PARTITIONED BY (year_month_file);
"""

spark.sql(query_green)
print("Table tb_bronze_green_taxi_trips successfully created.\n")

## Initiating Silver Schema Creation
print("Initiating Silver Schema Creation")

spark.sql("CREATE SCHEMA IF NOT EXISTS nyc_taxi.silver COMMENT 'Silver layer on the medallion architecture for the nyc taxi data'")
print("Schema silver successfully created!")

### Table: tb_silver_taxi_trips
print("Initiating Silver table tb_silver_taxi_trips creation")
query_silver = """
CREATE TABLE IF NOT EXISTS nyc_taxi.silver.tb_silver_taxi_trips (
    vendor_id INT COMMENT 'A code indicating the LPEP provider that provided the record.
\n 1 = Creative Mobile Technologies, LLC
\n 2 = Curb Mobility, LLC
\n 6 = Myle Technologies Inc',
    passenger_count INT COMMENT 'The number of passengers in the vehicle.',
    total_amount DOUBLE COMMENT 'The total amount charged to passengers. Does not include cash tips',
    pickup_datetime TIMESTAMP COMMENT 'The date and time when the meter was engaged.',
    dropoff_datetime TIMESTAMP COMMENT 'The date and time when the meter was disengaged.',
    taxi_color STRING COMMENT 'Identifier for the taxi color: yellow or green.',
    trip_distance DOUBLE COMMENT 'The elapsed trip distance in miles reported by the taximeter',
    pickup_time_hour INT COMMENT 'The hour component extracted from the pickup_datetime.',
    pickup_time_year_month STRING COMMENT 'The year and month component extracted from the pickup datetime.',
    dropoff_time_hour INT COMMENT 'The hour component extracted from the dropoff_datetime.',
    dropoff_time_year_month STRING COMMENT 'The year and month component extracted from the dropoff datetime.',
    is_passenger_count_recorded BOOLEAN COMMENT 'Flag indicating if the passenger_count is greater than zero and not null.',
    dt_ingestion TIMESTAMP COMMENT 'Datetime of the data ingestion into the silver layer.',
    year_month_file STRING COMMENT 'Year and month on YYYY-MM format of the file the data as collected. Used to partition the data.'
)
USING DELTA
COMMENT 'Refined silver table containing unified yellow and green taxi trip data. Records cleaned of statistical outliers and adjusted to business rules.'
PARTITIONED BY (year_month_file);
"""

spark.sql(query_silver)
print("Table tb_silver_taxi_trips successfully created.\n")

## Initiating Gold Schema Creation
print("Initiating Gold Schema Creation")

spark.sql("CREATE SCHEMA IF NOT EXISTS nyc_taxi.gold COMMENT 'Gold layer on the medallion architecture for the nyc taxi data'")
print("Schema gold successfully created!")

### Table: tb_gold_taxi_trips_full_data
print("Initiating Gold table tb_gold_taxi_trips_full_data creation")
query_gold_taxi_trips_full_data = """
CREATE TABLE IF NOT EXISTS nyc_taxi.gold.tb_gold_taxi_trips_full_data (
    vendor_id INT COMMENT 'A code indicating the LPEP provider that provided the record.
\n 1 = Creative Mobile Technologies, LLC
\n 2 = Curb Mobility, LLC
\n 6 = Myle Technologies Inc',
    passenger_count INT COMMENT 'The number of passengers in the vehicle.',
    total_amount DOUBLE COMMENT 'The total amount charged to passengers. Does not include cash tips',
    pickup_datetime TIMESTAMP COMMENT 'The date and time when the meter was engaged.',
    dropoff_datetime TIMESTAMP COMMENT 'The date and time when the meter was disengaged.',
    taxi_color STRING COMMENT 'Identifier for the taxi color: yellow or green.',
    pickup_time_hour INT COMMENT 'The hour component extracted from the pickup_datetime.',
    is_passenger_count_recorded BOOLEAN COMMENT 'Flag indicating if the passenger_count is greater than zero and not null.',
    dt_ingestion TIMESTAMP COMMENT 'Datetime of the data ingestion into the gold layer.',
    pickup_time_year_month STRING COMMENT 'The year and month component extracted from the pickup datetime.'
)
USING DELTA
COMMENT 'Refined gold table containing unified yellow and green taxi trip data. Records cleaned of statistical outliers and adjusted to business rules and only with columns required by business reasons and spcific analysis requirementes.'
PARTITIONED BY (pickup_time_year_month);
"""
spark.sql(query_gold_taxi_trips_full_data)
print("Table tb_gold_taxi_trips_full_data successfully created.\n")

### Table: tb_gold_taxi_trips_analysis_per_hour_per_month
print("Initiating Gold table tb_gold_taxi_trips_analysis_per_hour_per_month creation")
query_gold_taxi_trips_analysis_per_hour_per_month = """
CREATE TABLE IF NOT EXISTS nyc_taxi.gold.tb_gold_taxi_trips_analysis_per_hour_per_month (
    taxi_color STRING COMMENT 'Identifier for the taxi color: yellow or green.',
    pickup_time_hour INT COMMENT 'The hour component extracted from the pickup_datetime.',
    total_trips INT COMMENT 'Count of all trips taken in the universe of analysis.',
    total_amount DOUBLE COMMENT 'Sum of all the amount charged of all trips taken in the universe of analysis.',
    avg_amount DOUBLE COMMENT 'Average amount charged per trip in the universe of analysis.',
    total_passengers INT COMMENT 'Full amount of passengers carried in all the trips in the universe of analysis.',
    avg_passengers_count_with_recorded_data DOUBLE COMMENT 'Average number of passengers carried per trip in the universe of analysis considering only the trips with passager count recorded (null or different of zero).',
    avg_passengers_count_all_data DOUBLE COMMENT 'Average number of passengers carried per trip in the universe of analysis considering all trip excluding null values (trips without any passenger_count will not cloud the data)',
    dt_ingestion TIMESTAMP COMMENT 'Datetime of the data ingestion into the gold layer.',
    pickup_time_year_month STRING COMMENT 'The year and month component extracted from the pickup datetime.'
)
USING DELTA
COMMENT 'Refined gold table containing unified yellow and green taxi trip data for grouped analysis. Stores by the combination of taxi color, the hour of the pickup_time and the year and month of the pickup_time insightful data about the amount charged (total and average) and number of passengers carried (total, average per trip considering only trips with recorded passenger count and average per trip considering all trips).'
PARTITIONED BY (pickup_time_year_month);
"""
spark.sql(query_gold_taxi_trips_analysis_per_hour_per_month)
print("Table tb_gold_taxi_trips_analysis_per_hour_per_month successfully created.\n")

### Table: tb_gold_taxi_trips_analysis_per_hour_per_month
print("Initiating Gold table tb_gold_taxi_trips_analysis_per_month creation")
query_gold_taxi_trips_analysis_per_month = """
CREATE TABLE IF NOT EXISTS nyc_taxi.gold.tb_gold_taxi_trips_analysis_per_month (
    taxi_color STRING COMMENT 'Identifier for the taxi color: yellow or green.',
    total_trips INT COMMENT 'Count of all trips taken in the universe of analysis.',
    total_amount DOUBLE COMMENT 'Sum of all the amount charged of all trips taken in the universe of analysis.',
    avg_amount DOUBLE COMMENT 'Average amount charged per trip in the universe of analysis.',
    total_passengers INT COMMENT 'Full amount of passengers carried in all the trips in the universe of analysis.',
    avg_passengers_count_with_recorded_data DOUBLE COMMENT 'Average number of passengers carried per trip in the universe of analysis considering only the trips with passager count recorded (null or different of zero).',
    avg_passengers_count_all_data DOUBLE COMMENT 'Average number of passengers carried per trip in the universe of analysis considering all trip excluding null values (trips without any passenger_count will not cloud the data)',
    dt_ingestion TIMESTAMP COMMENT 'Datetime of the data ingestion into the gold layer.',
    pickup_time_year_month STRING COMMENT 'The year and month component extracted from the pickup datetime.'
)
USING DELTA
COMMENT 'Refined gold table containing unified yellow and green taxi trip data for grouped analysis. Stores by the combination of taxi color and the year and month of the pickup_time insightful data about the amount charged (total and average) and number of passengers carried (total, average per trip considering only trips with recorded passenger count and average per trip considering all trips).'
PARTITIONED BY (pickup_time_year_month);
"""
spark.sql(query_gold_taxi_trips_analysis_per_month)
print("Table tb_gold_taxi_trips_analysis_per_month successfully created.\n")




