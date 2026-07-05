import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from config.settings import GOLD_TABLE_METADATA, SILVER_TABLE_METADATA
from config.functions import save_as_table_delta

def create_table_gold_full_data(df):
    """
    Função que realiza as transformações e cria a tabela "nyc_taxi.gold.tb_gold_taxi_trips_full_data
    """
    print(f"  -> [DATA PREP] Tratando e preparando os dados da tabela: {GOLD_TABLE_METADATA['full_analysis']['table']}")

    df_gold_full_data = df.withColumn('dt_ingestion', F.current_timestamp())

    select_dinamico_expr = [
        F.col(col_name).cast(col_type) 
        for col_name, col_type in GOLD_TABLE_METADATA["full_analysis"]["schema"].items()
    ]
    
    df_final_gold =  df_gold_full_data.select(*select_dinamico_expr)

    partition_list_pickup_year_month = [linha["pickup_time_year_month"] for linha in df_final_gold.select("pickup_time_year_month").distinct().orderBy("pickup_time_year_month").collect()]

    print(f"  -> [ESCRITA] Salvando partição/ões {partition_list_pickup_year_month} na tabela Gold: {GOLD_TABLE_METADATA['full_analysis']['table']}")

    formatted_partitions = ", ".join([f"'{p}'" for p in partition_list_pickup_year_month])

    save_as_table_delta(df_final_gold, "overwrite", GOLD_TABLE_METADATA["full_analysis"]["table"], "pickup_time_year_month", formatted_partitions)


def create_table_gold_analysis_per_hour_per_month_full(df):
    """
    Função que realiza as transformações e agrupamentos para criar a tabela nyc_taxi.gold.tb_gold_taxi_trips_analysis_per_hour_per_month_full_full
    """
    print(f"  -> [DATA PREP] Tratando e preparando os dados da tabela: {GOLD_TABLE_METADATA['analysis_per_hour_per_month_full']['table']}")

    df_gold_groupping = df.groupBy("taxi_color", "pickup_time_year_month", "pickup_time_hour")\
        .agg( \
            F.count("*").alias("total_trips"),
            F.round(F.sum("total_amount"),2).alias("total_amount_sum"),
            F.round(F.avg("total_amount"),2).alias("avg_amount"),
            F.sum("passenger_count").alias("total_passengers"),
            F.round(F.avg(F.when(F.col("is_passenger_count_recorded") == True, F.col("passenger_count")).otherwise(None)),2).alias('avg_passengers_count_with_recorded_data'),
            F.round(F.avg("passenger_count"),2).alias("avg_passengers_count_all_data")
            ) \
            .withColumn("dt_ingestion", F.current_timestamp())

    select_dinamico_expr = [
        F.col(col_name).cast(col_type) 
        for col_name, col_type in GOLD_TABLE_METADATA["analysis_per_hour_per_month_full"]["schema"].items()
    ]
    
    df_final_gold_per_hour_per_month =  df_gold_groupping.select(*select_dinamico_expr)

    partition_list_pickup_year_month = [linha["pickup_time_year_month"] for linha in df_gold_groupping.select("pickup_time_year_month").distinct().orderBy("pickup_time_year_month").collect()]

    print(f"  -> [ESCRITA] Salvando partição/ões {partition_list_pickup_year_month} na tabela Gold: {GOLD_TABLE_METADATA['analysis_per_hour_per_month_full']['table']}")

    formatted_partitions = ", ".join([f"'{p}'" for p in partition_list_pickup_year_month])

    save_as_table_delta(df_final_gold_per_hour_per_month, "overwrite", GOLD_TABLE_METADATA['analysis_per_hour_per_month_full']['table'], "pickup_time_year_month", formatted_partitions)

def create_table_gold_analysis_per_month_per_color(df):
    """
    Função que realiza as transformações e agrupamentos para criar a tabela nyc_taxi.gold.tb_gold_taxi_trips_analysis_per_month_per_color
    """
    print(f"  -> [DATA PREP] Tratando e preparando os dados da tabela: {GOLD_TABLE_METADATA['analysis_per_month_per_color']['table']}")

    df_gold_groupping = df.groupBy("taxi_color", "pickup_time_year_month")\
        .agg( \
            F.count("*").alias("total_trips"),
            F.round(F.sum("total_amount"),2).alias("total_amount_sum"),
            F.round(F.avg("total_amount"),2).alias("avg_amount"),
            F.sum("passenger_count").alias("total_passengers"),
            F.round(F.avg(F.when(F.col("is_passenger_count_recorded") == True, F.col("passenger_count")).otherwise(None)),2).alias('avg_passengers_count_with_recorded_data'),
            F.round(F.avg("passenger_count"),2).alias("avg_passengers_count_all_data")
            ) \
            .withColumn("dt_ingestion", F.current_timestamp())

    select_dinamico_expr = [
        F.col(col_name).cast(col_type) 
        for col_name, col_type in GOLD_TABLE_METADATA["analysis_per_month_per_color"]["schema"].items()
    ]
    
    df_final_gold_per_month =  df_gold_groupping.select(*select_dinamico_expr)

    partition_list_pickup_year_month = [linha["pickup_time_year_month"] for linha in df_gold_groupping.select("pickup_time_year_month").distinct().orderBy("pickup_time_year_month").collect()]

    print(f"  -> [ESCRITA] Salvando partição/ões {partition_list_pickup_year_month} na tabela Gold: {GOLD_TABLE_METADATA['analysis_per_month_per_color']['table']}")

    formatted_partitions = ", ".join([f"'{p}'" for p in partition_list_pickup_year_month])

    save_as_table_delta(df_final_gold_per_month, "overwrite", GOLD_TABLE_METADATA['analysis_per_month_per_color']['table'], "pickup_time_year_month", formatted_partitions)

# ==============================================================================
# ORQUESTRAÇÃO: SILVER TO GOLD
# ==============================================================================
def process_silver_to_gold(particoes):
    """
    Função principal que orquestra a leitura da Silver, cria as tabelas solicitadas por negócio para análise e grava no schema Gold
    """
    print("\n=============================================")
    print("--- [STEP 4] INICIANDO: SILVER TO GOLD  ---")
    print("=============================================")
    
    spark = SparkSession.builder.getOrCreate()
    # Lendo o df_inicial referente a cor que deve ser aplicada
    df_initial = (
        spark.table(SILVER_TABLE_METADATA["table"])
        .filter(F.col("year_month_file").isin(particoes))
    )

    # Realizando as chamadas das funções na sequência para processar os dados
    create_table_gold_full_data(df_initial)
    create_table_gold_analysis_per_hour_per_month_full(df_initial)
    create_table_gold_analysis_per_month_per_color(df_initial)


    print("\n=============================================")
    print(f"--- [STEP 4] SUCESSO: SILVER TO GOLD FINALIZADO ---")
    print("=============================================\n")