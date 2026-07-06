# src/main.py
# Inicio: importações de pacotes e ferramentas necessárias
import sys
import os
import time
from datetime import datetime
# Fim: importações de pacotes e ferramentas necessárias

# 2. Importa os módulos necessários
# from pyspark.sql import SparkSession
from databricks.connect import DatabricksSession
from config.functions import get_runtime_parameters, time_log_metrics, parse_arguments
from src.ingestion_to_landing import download_landing_data
from src.landing_to_bronze import process_landing_to_bronze
from src.bronze_to_silver import process_bronze_to_silver
from src.silver_to_gold import process_silver_to_gold
from config.settings import STEPS_SEQUENCE

# Inicio: Função de execução final de todos os steps do pipeline
def run():
    print("=== INICIANDO PIPELINE NYC TAXI ===")

    # Inicio: Captura os parâmetros necessários para a execução
    spark = DatabricksSession.builder.serverless().getOrCreate()
    args = parse_arguments()
    particoes = get_runtime_parameters(ano_mes_override=args.ano_mes)
    print(f"Partições detectadas para processamento: {particoes}")
    # Fim: Captura os parâmetros necessários para a execução
    # Marca o início de todo o pipeline
    start_pipeline = time.time()

    # Define a ordem lógica dos steps
    steps = STEPS_SEQUENCE
    
    # Se o usuário pediu apenas um step, a lista a executar será apenas ele
    if args.step:
        steps_to_run = [args.step]
    else:
        # Se pediu para começar de um ponto, cortamos a lista a partir daquele ponto
        start_index = steps.index(args.start_from)
        steps_to_run = steps[start_index:]

    # --- EXECUÇÃO CONDICIONAL DOS STEPS ---

    if 'landing' in steps_to_run:
        print("\n--- INICIANDO FASE: LANDING ---")
        start_step = time.time()
        download_landing_data(particoes)
        end_step, duration_step = time_log_metrics(start_step)
        print(f"=== FINALIZADO STEP_LANDING EM: {end_step} | DURAÇÃO: {duration_step} ===")

    if 'bronze' in steps_to_run:
        print("\n--- INICIANDO FASE: BRONZE ---")
        start_step = time.time()
        process_landing_to_bronze(spark, particoes)
        end_step, duration_step = time_log_metrics(start_step)
        print(f"=== FINALIZADO LANDING TO BRONZE EM: {end_step} | DURAÇÃO: {duration_step} ===")

    if 'silver' in steps_to_run:
        print("\n--- INICIANDO FASE: SILVER ---")
        start_step = time.time()
        process_bronze_to_silver(spark, particoes)
        end_step, duration_step = time_log_metrics(start_step)
        print(f"=== FINALIZADO BRONZE TO SILVER EM: {end_step} | DURAÇÃO: {duration_step} ===")

    if 'gold' in steps_to_run:
        print("\n--- INICIANDO FASE: GOLD ---")
        start_step = time.time()
        process_silver_to_gold(spark, particoes)
        end_step, duration_step = time_log_metrics(start_step)
        print(f"=== FINALIZADO SILVER TO GOLD EM: {end_step} | DURAÇÃO: {duration_step} ===")

    end_pipeline, duration_pipeline = time_log_metrics(start_pipeline)
    print(f"\n=== PIPELINE EXECUTADO COM SUCESSO. FINALIZADO EM: {end_pipeline} | DURAÇÃO: {duration_pipeline} ===")

if __name__ == "__main__":
    run()