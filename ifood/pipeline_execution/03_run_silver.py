import sys
import os

diretorio_atual = os.path.dirname(os.path.abspath(__file__))
raiz_projeto = os.path.abspath(os.path.join(diretorio_atual, '..'))

if raiz_projeto not in sys.path:
    sys.path.append(raiz_projeto)

from config.functions import get_runtime_parameters
from src.bronze_to_silver import process_bronze_to_silver

if __name__ == "__main__":
    print("Iniciando Job: Bronze to Silver...")
    particoes = get_runtime_parameters()
    process_bronze_to_silver(particoes)