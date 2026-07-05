import sys
import os

folder_path = os.path.abspath('..')
if folder_path not in sys.path:
    sys.path.append(folder_path)

from config.functions import get_runtime_parameters
from src.silver_to_gold import process_silver_to_gold

if __name__ == "__main__":
    print("Iniciando Job: Silver to Gold...")
    particoes = get_runtime_parameters()
    process_silver_to_gold(particoes)