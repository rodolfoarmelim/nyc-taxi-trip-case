import os
import sys
from pyspark.sql import SparkSession
import time
from datetime import datetime

def save_as_table_delta(df, saving_mode, target_table, partition_column, partition_value):
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

    DEFAULT_ANO_MES = "2023-01,2023-02,2023-03,2023-04,2023-05"

    # Se houver override direto via código
    if ano_mes_override:
        ano_mes_raw = ano_mes_override

    else:
        try:
            # 1. Tenta ler via Databricks (Widgets)
            spark = SparkSession.builder.getOrCreate()
            dbutils = spark.getOrCreate()._jvm.com.databricks.service.SparkServiceConnection.exec.dbutils
            ano_mes_raw = dbutils.widgets.get("PIPELINE_ANO_MES") or DEFAULT_ANO_MES
        except Exception:
            # 2. Se falhar, verifica se os parâmetros vieram via Job Parameters (--key value) na CLI
            # O Databricks passa os parâmetros do Job como argumentos no formato ['--PIPELINE_ANOS', '2023']
            args = sys.argv
            ano_mes_raw = None
            
            if "--PIPELINE_ANO_MES" in args:
                idx = args.index("--PIPELINE_ANO_MES")
                anos_raw = args[idx + 1]
                
            # 3. Se não achou na CLI, recorre às Variáveis de Ambiente ou ao Default definitivo
            if not ano_mes_raw:
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