import sys
import os

folder_path = os.path.abspath('..')
if folder_path not in sys.path:
    sys.path.append(folder_path)

from config.functions import get_runtime_parameters
from src.bronze_to_silver import process_bronze_to_silver

if __name__ == "__main__":
    print("Iniciando Job: Bronze to Silver...")
    particoes = get_runtime_parameters()
    process_bronze_to_silver(particoes)