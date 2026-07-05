# src/main.py
# Inicio: importações de pacotes e ferramentas necessárias
import sys
import os
import time
from datetime import datetime
# Fim: importações de pacotes e ferramentas necessárias

# Inicio: Garantir que o código encontre as pastas de config necessárias
# folder_path = os.path.abspath('..')
# if folder_path not in sys.path:
#     sys.path.append(folder_path)
# Fim: Garantir que o código encontre as pastas de config necessárias

# 2. Importa os módulos necessários
from config.functions import get_runtime_parameters, time_log_metrics
from src.ingestion_to_landing import download_landing_data
from src.landing_to_bronze import process_landing_to_bronze
from src.bronze_to_silver import process_bronze_to_silver
from src.silver_to_gold import process_silver_to_gold

# Inicio: Função de execução final de todos os steps do pipeline
def run():
    print("=== INICIANDO PIPELINE NYC TAXI ===")
    
    # Inicio: Captura os parâmetros necessários para a execução
    particoes = get_runtime_parameters()
    print(f"Partições detectadas para processamento: {particoes}")
    # Fim: Captura os parâmetros necessários para a execução
    # Marca o início de todo o pipeline
    start_pipeline = time.time()

    # Inicio: Fase de Ingestão para landing zone
    download_landing_data(particoes)
    end_step_1, duration_step_1 = time_log_metrics(start_pipeline)
    print(f"=== FINALIZADO STEP_LANDING EM: {end_step_1} | DURAÇÃO: {duration_step_1} ===")
    
    # # Passo 2: Fase Bronze (Leitura dos arquivos brutos e carga no Delta)
    start_step_2 = time.time()
    process_landing_to_bronze(particoes)
    end_step_2, duration_step_2 = time_log_metrics(start_step_2)
    print(f"=== FINALIZADO LANDING TO BRONZE EM: {end_step_2} | DURAÇÃO: {duration_step_2} ===")

    # # Passo 3: Fase Silver (Limpeza dos dados e tratamento de schema)
    start_step_3 = time.time()
    process_bronze_to_silver(particoes)
    end_step_3, duration_step_3 = time_log_metrics(start_step_3)
    print(f"=== FINALIZADO BRONZE TO SILVER EM: {end_step_3} | DURAÇÃO: {duration_step_3} ===")

    # # Passo 4: Fase Gold (Agrupamento e camada analítica de dados)
    start_step_4 = time.time()
    process_silver_to_gold(particoes)
    end_step_4, duration_step_4 = time_log_metrics(start_step_4)
    print(f"=== FINALIZADO SILVER TO GOLD EM: {end_step_4} | DURAÇÃO: {duration_step_4} ===")

    end_pipeline, duration_pipeline = time_log_metrics(start_pipeline)
    print(f"=== PIPELINE EXECUTADO COM SUCESSO. FINALIZADO EM: {end_pipeline} | DURAÇÃO: {duration_pipeline} ===")
# Fim: Função de execução final de todos os steps do pipeline

# Chamada da função final
if __name__ == "__main__":
    run()