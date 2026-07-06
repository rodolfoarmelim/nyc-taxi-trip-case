import sys
from pyspark.sql.functions import col, current_timestamp, lit
from config.settings import TAXI_COLOR_LIST, LANDING_ZONE_VOLUME_PATH, BRONZE_TABLE_METADATA
from config.functions import save_as_table_delta

def process_single_file(spark, color: str, year_month: str, expected_schema: dict, target_table: str):
    """
    Função modularizada para processar um único arquivo específico.
    Aplica o cast explícito coluna por coluna com base no schema esperado.
    """
   
    nome_arquivo = f"{color}_tripdata_{year_month}.parquet" 
    caminho_volume = f"{LANDING_ZONE_VOLUME_PATH}/{nome_arquivo}" if not LANDING_ZONE_VOLUME_PATH.endswith('/') else f"{LANDING_ZONE_VOLUME_PATH}{nome_arquivo}"

    print(f"\n[LEITURA] Lendo arquivo: {nome_arquivo}")
    
    # 1. Leitura do Volume
    df_landing = spark.read.parquet(caminho_volume)
    
    # 2. Seleção e Cast Dinâmico de Colunas (Garantia Absoluta do Schema)
    # Montamos uma lista de expressões 'col(nome).cast(tipo)' para passar no select
    schema_cols_to_calculate = ["dt_ingestion", "year_month_file"]
    
    select_expr = [
        col(col_name).cast(col_type) 
        for col_name, col_type in expected_schema.items() if col_name not in schema_cols_to_calculate
    ]
    
    df_casted = df_landing.select(*select_expr)

    # 3. Adiciona as colunas de Ingestão e Partição
    df_bronze = df_casted \
        .withColumn("dt_ingestion", current_timestamp()) \
        .withColumn("year_month_file", lit(year_month))

    # 4. Append na tabela Delta particionada
    print(f"[ESCRITA] Registrando a partição {year_month} na tabela: {target_table}")
    year_month_string = f"'{year_month}'"
    save_as_table_delta(df_bronze, "overwrite", target_table, "year_month_file", year_month_string)
    
    print(f"[SUCESSO] {nome_arquivo} processado, tipado e salvo na Bronze.")


def process_landing_to_bronze(spark, particoes: list):
    """
    Função principal do step. Aceita o parâmetro opcional 'cor_override'
    para permitir reprocessamentos específicos de uma única cor de táxi.
    """
    print("\n=============================================")
    print("--- [STEP 2] INICIANDO: LANDING TO BRONZE ---")
    print("=============================================")

    # Atribui valor as cores de processamento
    cores_para_processar = TAXI_COLOR_LIST
    print(f"[CONFIG] Cores selecionadas para este run: {cores_para_processar}")

    # Mapeamento estrito do nome da coluna e do TIPO idêntico à DDL da tabela
    layer_mapping = BRONZE_TABLE_METADATA
    
    arquivos_processados = 0

    for color in cores_para_processar:
        for year_month in particoes:
            target_table = layer_mapping[color]["table"]
            expected_schema = layer_mapping[color]["schema"] 

            try:
                # Passamos o dicionário de schema em vez de uma lista simples de colunas
                process_single_file(spark, color, year_month, expected_schema, target_table)
                arquivos_processados += 1
            except Exception as e:
                nome_arquivo = f"{color}_tripdata_{year_month}.parquet"
                print(f"[ERRO] Falha crítica ao processar {nome_arquivo}: {str(e)}")
                raise e

    print("\n=============================================")
    print(f"--- [STEP 2] SUCESSO: LANDING TO BRONZE FINALIZADO ---")
    print(f"Total de arquivos processados com sucesso: {arquivos_processados}")
    print("=============================================\n")