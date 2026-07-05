import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from config.settings import TAXI_COLOR_LIST, SILVER_TABLE_RULES_BY_COLOR, BRONZE_TABLE_METADATA, SILVER_TABLE_METADATA
from config.functions import save_as_table_delta

# ==============================================================================
# FUNÇÃO CORE: TRANSFORMAÇÃO E LIMPEZA
# ==============================================================================
def apply_silver_transformations(spark, table_source, color, particoes):
    """
    Função genérica que aplica os filtros lógicos, remove outliers estatísticos/negócio
    e formata o DataFrame para o Schema unificado da camada Silver.
    """
    # Lendo o df_inicial referente a cor que deve ser aplicada
    df_initial = (
        spark.table(table_source)
        .filter(F.col("year_month_file").isin(particoes))
    )

    # Selecionando somente colunas necessárias para próximos passos
    df_initial = df_initial.select(SILVER_TABLE_RULES_BY_COLOR[color]["bronze_origin_colums"])
    
    # Pegando colunas necessárias para tratamento 1 e rename: pickup e dropoff + vendor_id
    vendor_id_col = SILVER_TABLE_RULES_BY_COLOR[color]["vendor_id_col_to_rename"]
    pickup_col = SILVER_TABLE_RULES_BY_COLOR[color]["pickup_col_to_rename"]
    dropoff_col = SILVER_TABLE_RULES_BY_COLOR[color]["dropoff_col_to_rename"]

    # 1. Criação de Colunas Auxiliares Temporais
    df_initial = (df_initial
        .withColumn("duracao_segundos", F.unix_timestamp(dropoff_col) - F.unix_timestamp(pickup_col))
        .withColumn("duracao_minutos_floor", F.floor(F.col("duracao_segundos") / 60))
        .withColumn("duracao_horas", F.col("duracao_segundos") / 3600)
        .withColumn("duracao_dias", F.col("duracao_segundos") / 86400)
        
        .withColumn("ano_mes_dropoff", F.date_format(dropoff_col, "yyyy-MM"))
        .withColumn("data_ref_arquivo", F.to_date(F.col("year_month_file"), "yyyy-MM"))
        .withColumn("data_ref_dropoff", F.to_date(F.col("ano_mes_dropoff"), "yyyy-MM"))
        .withColumn(
            "desvio_absoluto_meses", 
            F.abs(F.round(F.months_between(F.col("data_ref_dropoff"), F.col("data_ref_arquivo")), 0)).cast("int")
        )
    )

    # Pegando filtros necessários para lógica de filtros
    logical_filters_rules = SILVER_TABLE_RULES_BY_COLOR[color]["logical_filters"]
    filter_string = " AND ".join(logical_filters_rules)

    # 2. Filtros Lógicos Iniciais (Parametrizados)
    df_clean = df_initial.filter(filter_string)

    # 3. Variáveis Derivadas
    df_metricas = (df_clean
        .withColumn("tarifa_por_milha", F.col("total_amount") / F.col("trip_distance"))
        .withColumn(
            "velocidade_mph", 
            F.when(F.col("duracao_horas") > 0, F.col("trip_distance") / F.col("duracao_horas")).otherwise(0)
        )
    )

    # 4. Cálculo Dinâmico de Percentis (Calculado isoladamente para a cor atual)
    limites = df_metricas.approxQuantile(["tarifa_por_milha", "velocidade_mph"], [0.01, 0.99], 0.001)
    p1_tarifa, p99_tarifa = limites[0][0], limites[0][1]
    p1_vel = limites[1][0]

    # 5. Aplicação das Regras de Negócio (Expurgo de Outliers)
    df_filtered = df_metricas.filter(
        (F.col("tarifa_por_milha") >= (p1_tarifa / 2)) &
        (F.col("tarifa_por_milha") <= (p99_tarifa * 2)) &
        (F.col("velocidade_mph") >= p1_vel) &
        (F.col("velocidade_mph") <= 80.0) # Teto da regra de NY
    )

    # 6. Aplicação das Regras de Negócio e Renomeação
    df_silver_transformado = (df_filtered
        .withColumnRenamed(vendor_id_col, "vendor_id")
        .withColumnRenamed(pickup_col, "pickup_datetime")
        .withColumnRenamed(dropoff_col, "dropoff_datetime")
        .withColumn("taxi_color", F.lit(color))
        .withColumn("pickup_time_hour", F.hour(F.col("pickup_datetime")))
        .withColumn("dropoff_time_hour", F.hour(F.col("dropoff_datetime")))
        .withColumn(
            "is_passenger_count_recorded", 
            F.when((F.col("passenger_count").isNotNull()) & (F.col("passenger_count") > 0), F.lit(True))
             .otherwise(F.lit(False))
        )
        .withColumn("dt_ingestion", F.current_timestamp())
        .withColumn("pickup_time_year_month",  F.date_format(F.col("pickup_datetime"), "yyyy-MM"))
        .withColumn("dropoff_time_year_month", F.date_format(F.col("dropoff_datetime"), "yyyy-MM"))
        )

    # 7. Projeção Dinâmica e Tipagem Forte (Schema Enforcement)
    # Lê o dicionário, seleciona a coluna e já aplica o cast garantindo o contrato de dados
    select_dinamico_expr = [
        F.col(col_name).cast(col_type) 
        for col_name, col_type in SILVER_TABLE_METADATA["schema"].items()
    ]
    
    # Aplica a lista de expressões no DataFrame final
    df_silver_schema = df_silver_transformado.select(*select_dinamico_expr)

    return df_silver_schema

# ==============================================================================
# ORQUESTRAÇÃO: BRONZE TO SILVER
# ==============================================================================
def process_bronze_to_silver(particoes, cor_override=None):
    """
    Função principal que orquestra a leitura da Bronze, aplica o tratamento 
    por cor, faz o Union e grava na Silver por partição (Mês/Ano).
    """
    print("\n=============================================")
    print("--- [STEP 3] INICIANDO: BRONZE TO SILVER  ---")
    print("=============================================")
    
    spark = SparkSession.builder.getOrCreate()
    if cor_override and cor_override not in TAXI_COLOR_LIST:
        raise ValueError(f"Cor de reprocessamento inválida: {cor_override}. Escolha entre {TAXI_COLOR_LIST}")

    cores_para_processar = [cor_override] if cor_override else TAXI_COLOR_LIST
    dfs_to_union = []

    for color in cores_para_processar:
        table_source = BRONZE_TABLE_METADATA[color]["table"]
        print(f"\n[PROCESSANDO] Cor: {color}")
        query_process = f"df_silver_{color} = apply_silver_transformations(spark, table_source, color, particoes)"
        print(f"[CRIANDO] Cor: {color}, Particoes: {particoes}, tabela: df_silver_{color}")
        exec(query_process)
        dfs_to_union.append(eval(f"df_silver_{color}"))        
            
    # Append dos dataframes das duas cores
    df_final_silver = dfs_to_union[0].unionByName(dfs_to_union[1])

    # 4. Gravação Otimizada na Tabela Silver (Overwrite + replaceWhere)
    print(f"  -> [ESCRITA] Salvando partição/ões {particoes} na tabela Silver: {SILVER_TABLE_METADATA["table"]}")

    formatted_partitions = ", ".join([f"'{p}'" for p in particoes])

    save_as_table_delta(df_final_silver, "overwrite", SILVER_TABLE_METADATA["table"], "year_month_file", formatted_partitions)

    print("\n=============================================")
    print(f"--- [STEP 3] SUCESSO: BRONZE TO SILVER FINALIZADO ---")
    print("=============================================\n")