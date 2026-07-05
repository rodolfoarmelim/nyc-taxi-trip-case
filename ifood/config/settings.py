# settings.py
import os
import sys
from pyspark.sql import SparkSession

# --- 1. VARIÁVEIS FIXAS (Podem ser importadas diretamente) ---
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
TAXI_COLOR_LIST = ["yellow", "green"]
LANDING_ZONE_VOLUME_PATH = "/Volumes/nyc_taxi/landing/landing_zone/"

## BRONZE TABLE INFORMATION
TB_BRONZE_YELLOW_TAXI_TRIPS_SCHEMA = {
        "VendorID": "int",
        "tpep_pickup_datetime": "timestamp",
        "tpep_dropoff_datetime": "timestamp",
        "passenger_count": "int",
        "trip_distance": "double",
        "RatecodeID": "string",
        "store_and_fwd_flag": "string",
        "PULocationID": "int",
        "DOLocationID": "int",
        "payment_type": "int",
        "fare_amount": "double",
        "extra": "double",
        "mta_tax": "double",
        "tip_amount": "double",
        "tolls_amount": "double",
        "improvement_surcharge": "double",
        "total_amount": "double",
        "congestion_surcharge": "double",
        "airport_fee": "double",
        "dt_ingestion": "timestamp",
        "year_month_file": "string"
    }

TB_BRONZE_GREEN_TAXI_TRIPS_SCHEMA = {
        "VendorID": "int",
        "lpep_pickup_datetime": "timestamp",
        "lpep_dropoff_datetime": "timestamp",
        "store_and_fwd_flag": "string",
        "RatecodeID": "string",
        "PULocationID": "int",
        "DOLocationID": "int",
        "passenger_count": "int",
        "trip_distance": "double",
        "fare_amount": "double",
        "extra": "double",
        "mta_tax": "double",
        "tip_amount": "double",
        "tolls_amount": "double",
        "ehail_fee": "double",
        "improvement_surcharge": "double",
        "total_amount": "double",
        "payment_type": "int",
        "trip_type": "int",
        "congestion_surcharge": "double",
        "dt_ingestion": "timestamp",
        "year_month_file": "string"
    }

BRONZE_TABLE_METADATA = {
        "yellow": {"table": "nyc_taxi.bronze.tb_bronze_yellow_taxi_trips", "schema": TB_BRONZE_YELLOW_TAXI_TRIPS_SCHEMA},
        "green": {"table": "nyc_taxi.bronze.tb_bronze_green_taxi_trips", "schema": TB_BRONZE_GREEN_TAXI_TRIPS_SCHEMA}
    }

### Silver Table Information
TB_SILVER_TAXI_TRIPS_SCHEMA = {
        "vendor_id": "int",
        "passenger_count": "int",
        "total_amount": "double",
        "pickup_datetime": "timestamp",
        "dropoff_datetime": "timestamp",
        "trip_distance": "double",
        "taxi_color": "string",
        "pickup_time_hour": "int",
        "pickup_time_year_month": "string",
        "dropoff_time_hour": "int",
        "dropoff_time_year_month": "string",
        "is_passenger_count_recorded": "boolean",
        "dt_ingestion": "timestamp",
        "year_month_file": "string"
}

SILVER_TABLE_RULES_BY_COLOR = {
        "yellow" : {
            "vendor_id_col_to_rename": "VendorID",
            "pickup_col_to_rename": "tpep_pickup_datetime", 
            "dropoff_col_to_rename": "tpep_dropoff_datetime",
            "bronze_origin_colums": [
                "VendorID",
                "tpep_pickup_datetime",
                "tpep_dropoff_datetime",
                "total_amount",
                "passenger_count",
                "trip_distance",
                "year_month_file",
            ],
            "logical_filters": [
                "total_amount > 0",
                "duracao_minutos_floor > 0",
                "trip_distance > 0",
                "duracao_dias < 2",
                "desvio_absoluto_meses = 0",
                "(passenger_count <= 6 OR passenger_count IS NULL)"
                ]
            },
        "green" :  {
            "vendor_id_col_to_rename": "VendorID",
            "pickup_col_to_rename": "lpep_pickup_datetime", 
            "dropoff_col_to_rename": "lpep_dropoff_datetime",
            "bronze_origin_colums": [
                "VendorID",
                "lpep_pickup_datetime",
                "lpep_dropoff_datetime",
                "total_amount",
                "passenger_count",
                "trip_distance",
                "year_month_file",
            ],
             "logical_filters": [
                "total_amount > 0",
                "duracao_minutos_floor > 0",
                "trip_distance > 0",
                "duracao_dias < 1",
                "desvio_absoluto_meses = 0",
                "(passenger_count <= 6 OR passenger_count IS NULL)"
                ]
            }
    }
SILVER_TABLE_METADATA = {
        "table": "nyc_taxi.silver.tb_silver_taxi_trips", 
        "schema": TB_SILVER_TAXI_TRIPS_SCHEMA,
    }

### Gold Tables Information
TB_GOLD_TAXI_TRIPS_FULL_DATA_SCHEMA = {
        "vendor_id": "int",
        "passenger_count": "int",
        "total_amount": "double",
        "pickup_datetime": "timestamp",
        "dropoff_datetime": "timestamp",
        "taxi_color": "string",
        "pickup_time_hour": "int",
        "is_passenger_count_recorded": "boolean",
        "dt_ingestion": "timestamp",
        "pickup_time_year_month": "string"
}

TB_GOLD_TAXI_TRIPS_ANALYSIS_PER_HOUR_PER_MONTH_FULL = {
        "pickup_time_hour": "int",
        "total_trips": "int",
        "total_amount_sum": "double",
        "avg_amount": "double",
        "total_passengers": "int",
        "avg_passengers_count_with_recorded_data": "double",
        "avg_passengers_count_all_data": "double",
        "dt_ingestion": "timestamp",
        "pickup_time_year_month": "string"
}

TB_GOLD_TAXI_TRIPS_ANALYSIS_PER_MONTH_PER_COLOR = {
        "taxi_color": "string",
        "total_trips": "int",
        "total_amount_sum": "double",
        "avg_amount": "double",
        "total_passengers": "int",
        "avg_passengers_count_with_recorded_data": "double",
        "avg_passengers_count_all_data": "double",
        "dt_ingestion": "timestamp",
        "pickup_time_year_month": "string"
}

GOLD_TABLE_METADATA = {
        "full_analysis": 
            {
                "table": "nyc_taxi.gold.tb_gold_taxi_trips_full_data", 
                "schema": TB_GOLD_TAXI_TRIPS_FULL_DATA_SCHEMA
             },
        "analysis_per_hour_per_month_full": 
            {
                "table": "nyc_taxi.gold.tb_gold_taxi_trips_analysis_per_hour_per_month", 
                "schema": TB_GOLD_TAXI_TRIPS_ANALYSIS_PER_HOUR_PER_MONTH_FULL
            },
        "analysis_per_month_per_color": 
            {
                "table": "nyc_taxi.gold.tb_gold_taxi_trips_analysis_per_month_per_color", 
                "schema": TB_GOLD_TAXI_TRIPS_ANALYSIS_PER_MONTH_PER_COLOR
            },
    }
