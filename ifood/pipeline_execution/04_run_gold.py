import sys
import os

diretorio_atual = os.path.dirname(os.path.abspath(__file__))
raiz_projeto = os.path.abspath(os.path.join(diretorio_atual, '..'))

if raiz_projeto not in sys.path:
    sys.path.append(raiz_projeto)

from config.functions import get_runtime_parameters
from src.silver_to_gold import process_silver_to_gold

if __name__ == "__main__":
    print("Iniciando Job: Silver to Gold...")
    particoes = get_runtime_parameters()
    process_silver_to_gold(particoes)