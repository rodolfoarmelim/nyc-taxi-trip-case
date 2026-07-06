import os
import sys
from pyspark.sql import SparkSession
import time
import argparse
from datetime import datetime
from config.settings import DEFAULT_ANO_MES

def save_as_table_delta(df, saving_mode: str, target_table, partition_column: str, partition_value: str):
    """
    Função para salvar tabelas particionadas no formato delta utilizando replaceWhere
    """
    df.write \
        .format("delta") \
        .mode(saving_mode) \
        .option("replaceWhere", f"{partition_column} in ({partition_value})") \
        .saveAsTable(target_table)


# --- 2. PARTE DINÂMICA (Precisa de uma função para tempo de execução) ---
def get_runtime_parameters(ano_mes_override=None):
    """
    Captura os parâmetros dinâmicos de forma agnóstica:
    Funciona tanto no Databricks (Widgets) quanto em Python puro (Variáveis de Ambiente).
    """

    # Se houver override direto via código
    if ano_mes_override:
        ano_mes_raw = ano_mes_override

    else:
        try:
            # Tenta ler via Databricks (Widgets)
            spark = SparkSession.builder.getOrCreate()
            # Tratamento seguro para pegar o dbutils
            try:
                from pyspark.dbutils import DBUtils
                dbutils = DBUtils(spark)
            except ImportError:
                dbutils = spark.conf.get("spark.databricks.service.server.clusterId", None) # Fallback bobo só pra gerar erro se não estiver no Databricks
                dbutils = spark._jvm.com.databricks.service.SparkServiceConnection.exec.dbutils
                
            ano_mes_raw = dbutils.widgets.get("PIPELINE_ANO_MES")
            if not ano_mes_raw:
                ano_mes_raw = DEFAULT_ANO_MES
                
        except Exception:
            # Se falhar (está rodando local ou no Job via CLI), recorre às Variáveis de Ambiente ou Default
            ano_mes_raw = os.getenv("PIPELINE_ANO_MES", DEFAULT_ANO_MES)

    # Tratamento e geração da lista final
    year_month_list = [ano_mes.strip() for ano_mes in str(ano_mes_raw).split(",")]

    return year_month_list

def time_log_metrics(start):
    """
    Captura a diferença entre o inicio de uma tarefa e a finalização da outra
    """
    end = time.time()
    seconds_total_duration = int(end - start)
    
    minutes = seconds_total_duration // 60
    seconds = seconds_total_duration % 60
    
    formatted_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    duration_text = f"{minutes} minutos e {seconds} segundos"
    
    return formatted_date, duration_text

def parse_arguments():
    """
    Captura argumentos iniciais para executar steps a partir de um ponto OU somente um step específico
    """
    parser = argparse.ArgumentParser(description="Orquestrador do Pipeline NYC Taxi")
    
    # Argumento para rodar apenas UM step
    parser.add_argument(
        '--step', 
        type=str, 
        choices=['landing', 'bronze', 'silver', 'gold'], 
        help="Executa SOMENTE o step especificado."
    )
    
    # Argumento para iniciar A PARTIR de um step
    parser.add_argument(
        '--start-from', 
        type=str, 
        choices=['landing', 'bronze', 'silver', 'gold'],
        default='landing', # Se não passar nada, começa do início
        help="Inicia a execução a partir deste step até o final."
    )

    parser.add_argument(
        '--ano-mes', 
        type=str, 
        help="Substitui as partições a serem processadas. Ex: '2023-01,2023-02'"
    )
    
    args, unknown = parser.parse_known_args()
    return args